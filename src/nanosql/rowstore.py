"""`RowStore` de `mvcc-transaction-manager` implementado sobre el backend
clave-valor del motor de storage activo.

Es el punto donde el control de concurrencia MVCC y el storage real se
tocan: `MVCCTransactionManager` sólo conoce el `Protocol RowStore`, y esta
implementación materializa cada cadena de versiones como claves contiguas
del backend ordenado.

Formato de clave (orden lexicográfico = orden lógico):

    <row_id utf-8> 0x00 <commit_seq big-endian 8 bytes>

donde `row_id` es `"<tabla>\\x1f<rowid decimal de 20 dígitos>"`. El cero
final del separador nunca aparece en un `row_id` válido (el DDL rechaza
nombres de tabla con caracteres de control), así que el prefijo de una fila
jamás colisiona con el de otra; el rowid con ceros a la izquierda hace que
el orden de cadena de `row_id` coincida con el orden numérico de inserción
dentro de cada tabla — la propiedad de la que depende `scan` del MVCC para
ser determinista.

Formato de valor: `[versión formato 1B][flags 1B][created_by 8B][payload]`,
con el bit 0 de flags marcando tombstone (la versión representa un DELETE y
no lleva payload). La integridad frente a corrupción la aportan las páginas
y bloques con checksum del motor de storage subyacente; esta cabecera sólo
versiona el formato lógico para poder evolucionarlo.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field

from mvcc_transaction_manager import CommitSeq, RowId, RowVersion, TransactionId

from .durability import WalDurability
from .errors import InternalError
from .storage import KeyValueBackend

ROW_ID_SEPARATOR = "\x1f"
KEY_SEPARATOR = b"\x00"
ROWID_DIGITS = 20
VERSION_FORMAT = 1
_FLAG_TOMBSTONE = 0b0000_0001
_HEADER = struct.Struct(">BBQ")
_SEQ = struct.Struct(">Q")


def make_row_id(table: str, rowid: int) -> RowId:
    return RowId(f"{table}{ROW_ID_SEPARATOR}{rowid:0{ROWID_DIGITS}d}")


def split_row_id(row_id: RowId) -> tuple[str, int]:
    table, _, raw = str(row_id).rpartition(ROW_ID_SEPARATOR)
    if not table:
        raise InternalError(f"row_id sin separador de tabla: {row_id!r}")
    return table, int(raw)


def table_row_prefix(table: str) -> str:
    return f"{table}{ROW_ID_SEPARATOR}"


def table_row_prefix_end(table: str) -> str:
    """Cota superior exclusiva de los row_ids de `table` (el separador
    0x1f incrementado): `[prefijo, fin)` contiene exactamente sus filas."""
    return f"{table}{chr(ord(ROW_ID_SEPARATOR) + 1)}"


def _encode_key(row_id: RowId, commit_seq: CommitSeq) -> bytes:
    return str(row_id).encode("utf-8") + KEY_SEPARATOR + _SEQ.pack(int(commit_seq))


def _decode_key(key: bytes) -> tuple[RowId, CommitSeq]:
    # El separador está siempre en posición fija desde el final: buscarlo
    # con rfind sería incorrecto porque los 8 bytes big-endian del seq
    # pueden contener 0x00 legítimamente.
    separator_at = len(key) - _SEQ.size - 1
    if separator_at < 1 or key[separator_at : separator_at + 1] != KEY_SEPARATOR:
        raise InternalError(f"clave de versión malformada en el storage: {key!r}")
    row_id = RowId(key[:separator_at].decode("utf-8"))
    (seq,) = _SEQ.unpack(key[separator_at + 1 :])
    return row_id, CommitSeq(seq)


def _prefix_end(prefix: bytes) -> bytes:
    """Cota superior exclusiva exacta para un scan por prefijo: el prefijo
    con su último byte incrementado. Vale tanto para el prefijo de fila
    (termina en 0x00) como para el de tabla (termina en 0x1f): en ambos
    casos el byte nunca es 0xff."""
    return prefix[:-1] + bytes([prefix[-1] + 1])


def _encode_version(version: RowVersion) -> bytes:
    flags = _FLAG_TOMBSTONE if version.value is None else 0
    payload = b"" if version.value is None else version.value
    return _HEADER.pack(VERSION_FORMAT, flags, int(version.created_by)) + payload


def _decode_version(row_id: RowId, commit_seq: CommitSeq, data: bytes) -> RowVersion:
    if len(data) < _HEADER.size:
        raise InternalError(f"versión de fila truncada para {row_id!r}")
    fmt, flags, created_by = _HEADER.unpack_from(data)
    if fmt != VERSION_FORMAT:
        raise InternalError(f"formato de versión de fila desconocido ({fmt}) para {row_id!r}")
    value = None if flags & _FLAG_TOMBSTONE else data[_HEADER.size :]
    return RowVersion(
        row_id=row_id,
        value=value,
        created_by=TransactionId(created_by),
        commit_seq=commit_seq,
    )


@dataclass(frozen=True, slots=True)
class TableBootstrap:
    """Resumen por tabla calculado en la pasada única de apertura."""

    max_rowid: int
    live_rows: int


@dataclass(frozen=True, slots=True)
class BootstrapSummary:
    max_commit_seq: int
    tables: dict[str, TableBootstrap] = field(default_factory=dict)


class DurableRowStore:
    """Implementación de `RowStore` sobre `KeyValueBackend` + WAL opcional.

    Sincronización: todos los métodos del protocolo los invoca
    `MVCCTransactionManager` bajo su propio lock interno, y el engine
    serializa además la ejecución de sentencias — este objeto no añade
    locking propio.

    Registro en WAL: `append_version`/`prune_versions` sólo mutan el
    backend después de registrar la operación en la transacción WAL activa
    (`begin_logged_batch`), preservando el invariante write-ahead. Con
    `durability=None` (bases de datos `:memory:`) se escribe directo.
    """

    def __init__(self, backend: KeyValueBackend, durability: WalDurability | None) -> None:
        self._backend = backend
        self._durability = durability
        self._wal_txn: int | None = None

    # -- delimitación de la transacción WAL activa --------------------------

    def begin_logged_batch(self, wal_txn: int) -> None:
        if self._wal_txn is not None:
            raise InternalError("ya hay una transacción WAL activa en el row store")
        self._wal_txn = wal_txn

    def end_logged_batch(self) -> None:
        self._wal_txn = None

    # -- protocolo RowStore --------------------------------------------------

    def append_version(self, version: RowVersion) -> None:
        key = _encode_key(version.row_id, version.commit_seq)
        value = _encode_version(version)
        if self._durability is not None and self._wal_txn is not None:
            self._durability.log_put(self._wal_txn, key, value)
        self._backend.put(key, value)

    def versions_of(self, row_id: RowId) -> Sequence[RowVersion]:
        prefix = str(row_id).encode("utf-8") + KEY_SEPARATOR
        versions: list[RowVersion] = []
        for key, data in self._backend.scan(prefix, _prefix_end(prefix)):
            found_row_id, seq = _decode_key(key)
            versions.append(_decode_version(found_row_id, seq, data))
        return versions

    def all_versions(self) -> Iterator[RowVersion]:
        """Implementa el protocolo opcional `BulkRowStore` del gestor MVCC:
        una única pasada ordenada sobre el backend en vez de un scan por
        prefijo por cada fila — el camino que convierte la materialización
        de un snapshot completo en UN range scan del B+Tree/LSM."""
        for key, data in self._backend.scan():
            row_id, seq = _decode_key(key)
            yield _decode_version(row_id, seq, data)

    def versions_in_range(self, start: RowId, end: RowId) -> Iterator[RowVersion]:
        """Implementa `RangeRowStore` del gestor MVCC: las claves son
        `row_id + 0x00 + seq`, así que el rango de row_ids [start, end) es
        exactamente el rango de claves [start, end) del backend."""
        for key, data in self._backend.scan(str(start).encode("utf-8"), str(end).encode("utf-8")):
            row_id, seq = _decode_key(key)
            yield _decode_version(row_id, seq, data)

    def row_ids(self) -> Iterator[RowId]:
        last: RowId | None = None
        for key, _ in self._backend.scan():
            row_id, _seq = _decode_key(key)
            if row_id != last:
                last = row_id
                yield row_id

    def prune_versions(self, row_id: RowId, keep: Sequence[RowVersion]) -> int:
        keep_seqs = {int(v.commit_seq) for v in keep}
        prefix = str(row_id).encode("utf-8") + KEY_SEPARATOR
        # Se materializa la lista antes de borrar: mutar el árbol mientras
        # se recorre su iterador de scan invalidaría el recorrido.
        doomed = [
            (key, data)
            for key, data in self._backend.scan(prefix, _prefix_end(prefix))
            if _decode_key(key)[1] not in keep_seqs
        ]
        for key, data in doomed:
            if self._durability is not None and self._wal_txn is not None:
                self._durability.log_delete(self._wal_txn, key, data)
            self._backend.delete(key)
        return len(doomed)

    # -- utilidades para el engine (fuera del protocolo) ---------------------

    def bootstrap_summary(self) -> BootstrapSummary:
        """Una única pasada ordenada sobre todo el storage al abrir: máximo
        `commit_seq` global (para restaurar el horizonte MVCC), y por tabla
        el rowid máximo asignado y el número de filas vivas (cuya versión
        más reciente no es tombstone; las claves de una fila están ordenadas
        por seq, así que la última vista es la más reciente)."""
        max_seq = 0
        tables: dict[str, TableBootstrap] = {}
        current_row: RowId | None = None
        current_alive = False
        for key, data in self._backend.scan():
            row_id, seq = _decode_key(key)
            max_seq = max(max_seq, int(seq))
            version = _decode_version(row_id, seq, data)
            if row_id != current_row:
                self._flush_bootstrap_row(tables, current_row, current_alive)
                current_row = row_id
            current_alive = version.value is not None
        self._flush_bootstrap_row(tables, current_row, current_alive)
        return BootstrapSummary(max_commit_seq=max_seq, tables=tables)

    @staticmethod
    def _flush_bootstrap_row(
        tables: dict[str, TableBootstrap], row_id: RowId | None, alive: bool
    ) -> None:
        if row_id is None:
            return
        table, rowid = split_row_id(row_id)
        previous = tables.get(table, TableBootstrap(max_rowid=0, live_rows=0))
        tables[table] = TableBootstrap(
            max_rowid=max(previous.max_rowid, rowid),
            live_rows=previous.live_rows + (1 if alive else 0),
        )

    def latest_live_payloads(self, table: str) -> Iterator[bytes]:
        """Payload de la versión confirmada más reciente de cada fila viva
        de `table` (las filas cuyo último estado es un tombstone se
        omiten). Misma vista que `live_row_count`: la entrada de `ANALYZE`."""
        prefix = table_row_prefix(table).encode("utf-8")
        current_row: RowId | None = None
        current_value: bytes | None = None
        for key, data in self._backend.scan(prefix, _prefix_end(prefix)):
            row_id, seq = _decode_key(key)
            if row_id != current_row and current_value is not None:
                yield current_value
            current_row = row_id
            current_value = _decode_version(row_id, seq, data).value
        if current_value is not None:
            yield current_value

    def live_row_count(self, table: str) -> int:
        """Filas de `table` cuya versión más reciente confirmada no es un
        tombstone. Es la estadística de cardinalidad que se entrega al
        optimizador: aproximada respecto a la visibilidad de un snapshot
        concreto, exacta respecto al último commit — suficiente para
        estimar costes."""
        prefix = table_row_prefix(table).encode("utf-8")
        count = 0
        current_row: RowId | None = None
        current_alive = False
        for key, data in self._backend.scan(prefix, _prefix_end(prefix)):
            row_id, seq = _decode_key(key)
            if row_id != current_row:
                count += 1 if current_alive else 0
                current_row = row_id
            current_alive = _decode_version(row_id, seq, data).value is not None
        count += 1 if current_alive else 0
        return count
