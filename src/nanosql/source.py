"""`TableSource` del motor vectorizado alimentado por snapshots MVCC.

Cierra el circuito de integración del plano de lectura: el ejecutor pide
batches columnares de una tabla, y esta fuente los construye a partir de las
filas visibles para la transacción activa según `mvcc-transaction-manager`
(incluidas las escrituras propias aún sin confirmar, que el scan del gestor
ya mezcla desde el write buffer local).

El payload de cada versión de fila es JSON UTF-8 `{columna: valor}`. Se
eligió JSON sobre un binario propio porque el valor viaja dentro de páginas/
bloques ya checksumeados por el motor de storage (la integridad no depende
del payload) y porque hace las bases de datos inspeccionables con un editor
hexadecimal — deuda de rendimiento asumida y documentada en el README.
"""

from __future__ import annotations

import json
from collections.abc import Iterator

import numpy as np
from mvcc_transaction_manager import MVCCTransactionManager, RowId, TransactionId
from vectorized_execution_engine.batch import (
    DEFAULT_BATCH_SIZE,
    RowBatch,
    columns_from_python,
)
from vectorized_execution_engine.models import ColumnType, Schema

from .catalog import Catalog, ColumnValue, TableSchema
from .errors import InternalError
from .rowstore import split_row_id, table_row_prefix, table_row_prefix_end

# Columna sintética con el rowid interno de cada fila, expuesta SOLO cuando
# el engine planifica un UPDATE/DELETE (nunca en un SELECT de usuario): es
# como el plan identifica qué filas re-escribir o borrar. El nombre con
# doble guión bajo no es un identificador SQL válido para el usuario, así
# que no puede colisionar con columnas reales.
ROWID_COLUMN = "__rowid__"


def encode_row_payload(row: dict[str, ColumnValue]) -> bytes:
    return json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode_row_payload(payload: bytes) -> dict[str, ColumnValue]:
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InternalError(f"payload de fila irreconocible en el storage: {payload!r}") from exc
    if not isinstance(decoded, dict):
        raise InternalError(f"payload de fila no es un objeto JSON: {payload!r}")
    return decoded


class MvccTableSource:
    """Implementación de `vectorized_execution_engine.batch.TableSource`.

    Cada `scan` lee las filas visibles del snapshot de `txn_id` y las
    agrupa en `RowBatch`es columnares del tamaño por defecto del ejecutor.
    `mvcc.scan` recorre todas las filas conocidas por el gestor (es la única
    forma de que la transacción vea también sus propias escrituras sin
    confirmar), y aquí se filtra por el prefijo de la tabla pedida — coste
    lineal en el total de filas de la base de datos por tabla escaneada,
    asumido para v1 y documentado.
    """

    def __init__(
        self,
        manager: MVCCTransactionManager,
        txn_id: TransactionId,
        catalog: Catalog,
        *,
        include_rowid: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._manager = manager
        self._txn_id = txn_id
        self._catalog = catalog
        self._include_rowid = include_rowid
        self._batch_size = batch_size

    def _table_schema(self, table_name: str) -> TableSchema:
        return self._catalog.table(table_name)

    def schema(self, table_name: str) -> Schema:
        table = self._table_schema(table_name)
        names: list[str] = []
        types: dict[str, ColumnType] = {}
        if self._include_rowid:
            names.append(ROWID_COLUMN)
            types[ROWID_COLUMN] = ColumnType.INTEGER
        for column in table.columns:
            names.append(column.name)
            types[column.name] = column.column_type
        return Schema(columns=tuple(names), types=types)

    def scan(self, table_name: str) -> Iterator[RowBatch]:
        table = self._table_schema(table_name)
        schema = self.schema(table_name)
        prefix = table_row_prefix(table_name)

        visible = self._manager.scan_range(
            self._txn_id, RowId(prefix), RowId(table_row_prefix_end(table_name))
        )

        for start in range(0, len(visible), self._batch_size):
            chunk = visible[start : start + self._batch_size]
            decoded = [decode_row_payload(payload) for _, payload in chunk]
            columns: dict[str, np.ndarray] = {}
            nulls: dict[str, np.ndarray] = {}
            if self._include_rowid:
                rowids: list[ColumnValue] = [split_row_id(row_id)[1] for row_id, _ in chunk]
                columns[ROWID_COLUMN], nulls[ROWID_COLUMN] = columns_from_python(
                    rowids, ColumnType.INTEGER
                )
            for column in table.columns:
                values = [row.get(column.name) for row in decoded]
                columns[column.name], nulls[column.name] = columns_from_python(
                    values, column.column_type
                )
            yield RowBatch(schema, columns, nulls)
