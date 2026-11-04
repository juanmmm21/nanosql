"""Catálogo de esquemas: tablas, columnas e índices declarados por DDL.

El tipo canónico de columna es directamente `ColumnType` de
`vectorized-execution-engine` — el motor que al final materializa cada valor
en un array de NumPy — en vez de un enum paralelo propio: así el catálogo no
puede declarar nunca un tipo que el ejecutor no sepa representar.

Persistencia: un único `catalog.json` versionado por base de datos, escrito
de forma atómica (temporal + fsync + rename, la misma disciplina que el
manifest de `lsm-tree-engine`). El catálogo cambia sólo con DDL — muchísimo
menos frecuente que el DML — así que un JSON legible vale más que un formato
binario propio; la integridad de los *datos* ya la protegen las páginas con
checksum del motor de storage activo.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from vectorized_execution_engine.models import ColumnType

from .errors import InternalError, OperationalError, ProgrammingError

CATALOG_FORMAT_VERSION = 1

# Mapeo del nombre de tipo SQL (DATA_TYPE_KEYWORDS del parser) al tipo
# columnar del ejecutor. DATE/TIME/TIMESTAMP son parseables pero el motor
# vectorizado no tiene representación para ellos — se rechazan en el DDL con
# NotSupportedError (ver engine.py), no aquí: este módulo sólo conoce los
# tipos que sí existen.
SQL_TYPE_TO_COLUMN_TYPE: dict[str, ColumnType] = {
    "INTEGER": ColumnType.INTEGER,
    "INT": ColumnType.INTEGER,
    "SMALLINT": ColumnType.INTEGER,
    "BIGINT": ColumnType.INTEGER,
    "FLOAT": ColumnType.FLOAT,
    "REAL": ColumnType.FLOAT,
    "DOUBLE": ColumnType.FLOAT,
    "DECIMAL": ColumnType.FLOAT,
    "NUMERIC": ColumnType.FLOAT,
    "VARCHAR": ColumnType.STRING,
    "CHAR": ColumnType.STRING,
    "TEXT": ColumnType.STRING,
    "BOOLEAN": ColumnType.BOOLEAN,
    "BOOL": ColumnType.BOOLEAN,
}

# Valor de columna tal y como viaja por el driver (y se serializa en JSON).
ColumnValue = int | float | str | bool | None


@dataclass(frozen=True, slots=True)
class ColumnSchema:
    name: str
    column_type: ColumnType
    not_null: bool = False
    primary_key: bool = False
    unique: bool = False
    default: ColumnValue = None
    has_default: bool = False


@dataclass(frozen=True, slots=True)
class IndexSchema:
    name: str
    table: str
    columns: tuple[str, ...]
    unique: bool = False


@dataclass(frozen=True, slots=True)
class TableSchema:
    name: str
    columns: tuple[ColumnSchema, ...]

    def column(self, name: str) -> ColumnSchema:
        for col in self.columns:
            if col.name == name:
                return col
        raise ProgrammingError(
            f"la columna '{name}' no existe en la tabla '{self.name}'", sqlstate="42703"
        )

    def column_names(self) -> tuple[str, ...]:
        return tuple(col.name for col in self.columns)

    def has_column(self, name: str) -> bool:
        return any(col.name == name for col in self.columns)


@dataclass(slots=True)
class Catalog:
    """Estado mutable del esquema de la base de datos. La sincronización la
    aporta el lock de ejecución del engine (ver `engine.py`): este objeto
    nunca se comparte fuera de él."""

    tables: dict[str, TableSchema] = field(default_factory=dict)
    indexes: dict[str, IndexSchema] = field(default_factory=dict)

    def table(self, name: str) -> TableSchema:
        try:
            return self.tables[name]
        except KeyError as exc:
            raise ProgrammingError(f"la tabla '{name}' no existe", sqlstate="42P01") from exc

    def add_table(self, table: TableSchema) -> None:
        if table.name in self.tables:
            raise ProgrammingError(f"la tabla '{table.name}' ya existe", sqlstate="42P07")
        self.tables[table.name] = table

    def add_index(self, index: IndexSchema) -> None:
        if index.name in self.indexes:
            raise ProgrammingError(f"el índice '{index.name}' ya existe", sqlstate="42P07")
        table = self.table(index.table)
        for column in index.columns:
            if not table.has_column(column):
                raise ProgrammingError(
                    f"la columna '{column}' del índice '{index.name}' no existe "
                    f"en la tabla '{index.table}'"
                )
        self.indexes[index.name] = index

    def drop_table(self, name: str) -> None:
        """Retira la tabla y todos sus índices del esquema."""
        self.table(name)  # error tipado si no existe
        del self.tables[name]
        for index_name in [i.name for i in self.indexes.values() if i.table == name]:
            del self.indexes[index_name]

    def indexes_of(self, table_name: str) -> tuple[IndexSchema, ...]:
        return tuple(idx for idx in self.indexes.values() if idx.table == table_name)


def _column_to_json(col: ColumnSchema) -> dict[str, object]:
    return {
        "name": col.name,
        "type": col.column_type.name,
        "not_null": col.not_null,
        "primary_key": col.primary_key,
        "unique": col.unique,
        "default": col.default,
        "has_default": col.has_default,
    }


def _column_from_json(raw: dict[str, object]) -> ColumnSchema:
    try:
        type_name = raw["type"]
        if not isinstance(type_name, str):
            raise KeyError("type")
        return ColumnSchema(
            name=str(raw["name"]),
            column_type=ColumnType[type_name],
            not_null=bool(raw["not_null"]),
            primary_key=bool(raw["primary_key"]),
            unique=bool(raw["unique"]),
            default=raw.get("default"),  # type: ignore[arg-type]
            has_default=bool(raw.get("has_default", False)),
        )
    except KeyError as exc:
        raise InternalError(f"columna malformada en catalog.json: {raw!r}") from exc


def save_catalog(path: Path, catalog: Catalog, *, backend_name: str) -> None:
    """Escritura atómica: el catálogo antiguo sigue íntegro hasta el rename."""
    payload = {
        "version": CATALOG_FORMAT_VERSION,
        "backend": backend_name,
        "tables": [
            {"name": t.name, "columns": [_column_to_json(c) for c in t.columns]}
            for t in catalog.tables.values()
        ],
        "indexes": [
            {"name": i.name, "table": i.table, "columns": list(i.columns), "unique": i.unique}
            for i in catalog.indexes.values()
        ],
    }
    tmp_path = path.with_suffix(".json.tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except OSError as exc:
        raise OperationalError(f"no se pudo persistir el catálogo en {path}: {exc}") from exc


def load_catalog(path: Path) -> tuple[Catalog, str]:
    """Carga `catalog.json` y devuelve `(catálogo, nombre_de_backend)`."""
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except OSError as exc:
        raise OperationalError(f"no se pudo leer el catálogo {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise InternalError(f"catalog.json corrupto en {path}: {exc}") from exc

    if not isinstance(payload, dict) or payload.get("version") != CATALOG_FORMAT_VERSION:
        raise InternalError(
            f"versión de catálogo no soportada en {path}: "
            f"{payload.get('version') if isinstance(payload, dict) else payload!r}"
        )

    catalog = Catalog()
    try:
        for raw_table in payload["tables"]:
            columns = tuple(_column_from_json(raw) for raw in raw_table["columns"])
            catalog.tables[str(raw_table["name"])] = TableSchema(
                name=str(raw_table["name"]), columns=columns
            )
        for raw_index in payload["indexes"]:
            catalog.indexes[str(raw_index["name"])] = IndexSchema(
                name=str(raw_index["name"]),
                table=str(raw_index["table"]),
                columns=tuple(str(c) for c in raw_index["columns"]),
                unique=bool(raw_index["unique"]),
            )
        backend_name = str(payload["backend"])
    except (KeyError, TypeError) as exc:
        raise InternalError(f"estructura inesperada en catalog.json de {path}") from exc
    return catalog, backend_name
