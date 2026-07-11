"""Durabilidad vía `write-ahead-log-recovery`: cada commit MVCC se registra
en el WAL del ecosistema antes de tocar el storage.

Invariante protegido (el más importante de `../CLAUDE.md` del ecosistema):
ninguna mutación del backend clave-valor se considera durable hasta que su
registro está en el WAL con `fsync`. `WriteAheadLog.append_*` hace fsync por
registro, así que el orden `log_put → backend.put` garantiza que incluso una
página desalojada del buffer pool del B+Tree en mitad de una transacción
tiene ya su registro en disco. Si el proceso muere antes del registro
COMMIT, la recuperación deshace las escrituras parciales (undo); si muere
después, las re-aplica (redo) aunque el storage nunca llegase a flushear.

El backend LSM tiene además su propio WAL interno por operación: con ese
backend cada commit paga dos fsync (uno por log). Es redundante pero nunca
incorrecto, y mantiene un único camino de recuperación para ambos motores.
"""

from __future__ import annotations

from pathlib import Path

from write_ahead_log_recovery import (
    RecoveryReport,
    WalError,
    WalIOError,
    WriteAheadLog,
    recover,
)

from .errors import OperationalError
from .storage import KeyValueBackend

# Nombre lógico de tabla usado en cada registro del WAL. La clave del par ya
# codifica tabla + fila + versión (ver rowstore.py), así que un único espacio
# de nombres es suficiente y la recuperación no necesita entender el esquema.
KV_TABLE_NAME = "kv"


class KvRecoveryApplier:
    """`StorageApplier` de la fase redo/undo: re-aplica registros del WAL
    directamente sobre el backend clave-valor. Las tres operaciones son
    idempotentes (put sobrescribe, delete tolera clave ausente), condición
    necesaria porque el redo repite historia ya aplicada."""

    def __init__(self, backend: KeyValueBackend) -> None:
        self._backend = backend

    def apply_insert(self, table: str, key: bytes, value: bytes) -> None:
        self._backend.put(key, value)

    def apply_update(self, table: str, key: bytes, value: bytes) -> None:
        self._backend.put(key, value)

    def apply_delete(self, table: str, key: bytes) -> None:
        self._backend.delete(key)


def recover_storage(wal_path: Path, backend: KeyValueBackend) -> RecoveryReport | None:
    """Recuperación al abrir la base de datos: redo de todo lo confirmado,
    undo de lo que quedó a medias. Devuelve el informe, o `None` si no había
    WAL previo (base de datos nueva o cierre limpio con log truncado)."""
    if not wal_path.exists():
        return None
    try:
        report = recover(wal_path, KvRecoveryApplier(backend))
    except WalError as exc:
        raise OperationalError(f"la recuperación del WAL {wal_path} falló: {exc}") from exc
    # Lo recuperado debe quedar en disco antes de truncar el log que lo
    # respalda — si no, un segundo crash perdería lo redo-eado.
    backend.flush()
    return report


class WalDurability:
    """Sesión de WAL abierta de una base de datos en disco.

    El engine envuelve cada commit MVCC (y cada pasada de GC de versiones)
    en una transacción de este log: `begin → log_put/log_delete por cada
    mutación → commit`. El id de transacción del WAL es independiente del
    id MVCC — sólo delimita qué registros deshacer si el proceso muere
    antes del COMMIT del log.
    """

    def __init__(self, path: Path) -> None:
        try:
            self._wal = WriteAheadLog(path)
        except WalError as exc:
            raise OperationalError(f"no se pudo abrir el WAL {path}: {exc}") from exc

    def begin(self) -> int:
        try:
            return self._wal.begin_transaction()
        except WalIOError as exc:
            raise OperationalError(f"fallo de E/S iniciando transacción WAL: {exc}") from exc

    def log_put(self, wal_txn: int, key: bytes, value: bytes) -> None:
        try:
            self._wal.append_insert(wal_txn, KV_TABLE_NAME, key, value)
        except WalIOError as exc:
            raise OperationalError(f"fallo de E/S registrando escritura en WAL: {exc}") from exc

    def log_delete(self, wal_txn: int, key: bytes, old_value: bytes) -> None:
        try:
            self._wal.append_delete(wal_txn, KV_TABLE_NAME, key, old_value)
        except WalIOError as exc:
            raise OperationalError(f"fallo de E/S registrando borrado en WAL: {exc}") from exc

    def commit(self, wal_txn: int) -> None:
        try:
            self._wal.commit(wal_txn)
        except WalIOError as exc:
            raise OperationalError(f"fallo de E/S confirmando transacción WAL: {exc}") from exc

    def abort(self, wal_txn: int) -> None:
        try:
            self._wal.abort(wal_txn)
        except WalIOError as exc:
            raise OperationalError(f"fallo de E/S abortando transacción WAL: {exc}") from exc

    def checkpoint(self, backend: KeyValueBackend) -> None:
        """Flush del storage + checkpoint + truncado del log ya cubierto.

        El orden importa: primero el storage persiste todo lo que el log
        respalda, sólo entonces es seguro truncar. Se invoca con el lock de
        ejecución del engine en mano, así que nunca hay transacciones WAL
        activas en este punto.
        """
        backend.flush()
        try:
            info = self._wal.checkpoint(active_transaction_ids=[])
            self._wal.truncate_before(info.safe_truncation_lsn)
        except WalIOError as exc:
            raise OperationalError(f"fallo de E/S en checkpoint del WAL: {exc}") from exc

    def close(self) -> None:
        try:
            self._wal.close()
        except WalIOError as exc:
            raise OperationalError(f"fallo de E/S cerrando el WAL: {exc}") from exc
