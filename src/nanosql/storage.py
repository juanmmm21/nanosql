"""Adaptadores de storage: los dos motores del ecosistema detrás de una
única interfaz clave-valor ordenada.

`bplus-tree-storage-engine` y `lsm-tree-engine` exponen APIs casi idénticas
(put/get/delete/scan sobre `bytes`) pero con diferencias reales: el B+Tree
lanza `KeyNotFoundError` al borrar una clave ausente y separa buffer pool de
dispositivo de páginas; el LSM borra con tombstones idempotentes y gestiona
su propio directorio. Este módulo normaliza ambas detrás del `Protocol`
`KeyValueBackend` — que es lo único que el resto de `nanosql` conoce — y
traduce sus excepciones tipadas a la jerarquía del driver.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Protocol, runtime_checkable

from bplus_tree_storage_engine import (
    BPlusTree,
    BplusTreeError,
    BufferPool,
    BufferPoolConfig,
    FilePageDevice,
    InMemoryPageDevice,
    KeyNotFoundError,
    LruReplacementPolicy,
    PageCorruptionError,
    StorageIOError,
)
from lsm_tree_engine import LsmError, LsmTree
from lsm_tree_engine import StorageIOError as LsmStorageIOError

from .errors import InternalError, OperationalError

# Capacidad por defecto del buffer pool del backend B+Tree, en páginas de 4KiB
# (la constante de página la fija el propio motor). 256 páginas = 1MiB de
# caché: suficiente para que los benchmarks no midan sólo E/S y lo bastante
# pequeño para que la política LRU trabaje de verdad en datasets medianos.
DEFAULT_BUFFER_POOL_PAGES = 256


@runtime_checkable
class KeyValueBackend(Protocol):
    """Almacén clave-valor ordenado por clave (orden lexicográfico de bytes).

    Es la frontera entre `nanosql` y el motor de storage activo: el row
    store MVCC (`rowstore.py`) y la recuperación WAL (`durability.py`) sólo
    hablan esta interfaz, así que cambiar de motor es elegir otro adaptador
    al conectar, nunca tocar la lógica de integración.
    """

    def put(self, key: bytes, value: bytes) -> None:
        """Inserta o sobrescribe `key`."""
        ...

    def get(self, key: bytes) -> bytes | None:
        """Valor de `key`, o `None` si no existe."""
        ...

    def delete(self, key: bytes) -> None:
        """Elimina `key` si existe. Idempotente: borrar una clave ausente
        no es un error (la recuperación WAL puede re-aplicar borrados)."""
        ...

    def scan(
        self, start: bytes | None = None, end: bytes | None = None
    ) -> Iterator[tuple[bytes, bytes]]:
        """Entradas en orden ascendente de clave, acotadas a `[start, end)`."""
        ...

    def flush(self) -> None:
        """Persiste a disco todo lo pendiente en memoria."""
        ...

    def close(self) -> None:
        """Libera los recursos del motor (idempotente)."""
        ...


class BPlusTreeBackend:
    """`KeyValueBackend` sobre `bplus-tree-storage-engine`.

    Con `path=None` usa un dispositivo de páginas en memoria (modo
    `:memory:` del driver, sin durabilidad); con un path real, un archivo de
    páginas en disco detrás del buffer pool LRU acotado del propio motor.
    """

    def __init__(
        self,
        path: Path | None,
        *,
        buffer_pool_pages: int = DEFAULT_BUFFER_POOL_PAGES,
    ) -> None:
        try:
            device = InMemoryPageDevice() if path is None else FilePageDevice(path)
            # En disco el archivo debe pasar de un checkpoint completo al
            # siguiente: el WAL de nanosql es lógico (re-aplica puts) y no
            # puede rehacer sobre un árbol que un kill -9 dejó a medio split.
            # crash_consistent = no-steal + flush atómico con diario.
            pool = BufferPool(
                device,
                BufferPoolConfig(capacity=buffer_pool_pages, crash_consistent=path is not None),
                LruReplacementPolicy(),
            )
            self._tree = BPlusTree(pool)
        except StorageIOError as exc:
            raise OperationalError(f"no se pudo abrir el storage B+Tree: {exc}") from exc
        except BplusTreeError as exc:
            raise InternalError(f"storage B+Tree inconsistente al abrir: {exc}") from exc
        self._closed = False

    def put(self, key: bytes, value: bytes) -> None:
        try:
            self._tree.insert(key, value)
        except StorageIOError as exc:
            raise OperationalError(f"fallo de E/S escribiendo en el B+Tree: {exc}") from exc
        except BplusTreeError as exc:
            raise InternalError(f"error del motor B+Tree en put: {exc}") from exc

    def get(self, key: bytes) -> bytes | None:
        try:
            return self._tree.search(key)
        except StorageIOError as exc:
            raise OperationalError(f"fallo de E/S leyendo del B+Tree: {exc}") from exc
        except (PageCorruptionError, BplusTreeError) as exc:
            raise InternalError(f"error del motor B+Tree en get: {exc}") from exc

    def delete(self, key: bytes) -> None:
        try:
            self._tree.delete(key)
        except KeyNotFoundError:
            # Idempotencia exigida por el Protocol: la fase redo/undo de la
            # recuperación WAL puede repetir borrados ya aplicados.
            return
        except StorageIOError as exc:
            raise OperationalError(f"fallo de E/S borrando del B+Tree: {exc}") from exc
        except BplusTreeError as exc:
            raise InternalError(f"error del motor B+Tree en delete: {exc}") from exc

    def scan(
        self, start: bytes | None = None, end: bytes | None = None
    ) -> Iterator[tuple[bytes, bytes]]:
        try:
            yield from self._tree.scan(start, end)
        except StorageIOError as exc:
            raise OperationalError(f"fallo de E/S recorriendo el B+Tree: {exc}") from exc
        except (PageCorruptionError, BplusTreeError) as exc:
            raise InternalError(f"error del motor B+Tree en scan: {exc}") from exc

    def flush(self) -> None:
        try:
            self._tree.flush()
        except StorageIOError as exc:
            raise OperationalError(f"fallo de E/S en flush del B+Tree: {exc}") from exc

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._tree.close()
        except StorageIOError as exc:
            raise OperationalError(f"fallo de E/S cerrando el B+Tree: {exc}") from exc


class LsmTreeBackend:
    """`KeyValueBackend` sobre `lsm-tree-engine`.

    El motor LSM gestiona su propio directorio (memtable + WAL interno +
    SSTables por niveles + manifest) y sus borrados son tombstones
    idempotentes, así que este adaptador es casi transparente: sólo traduce
    excepciones. Nota de durabilidad: cada `put`/`delete` ya pasa por el WAL
    interno del LSM con fsync; el WAL del ecosistema (`durability.py`) se
    mantiene igualmente por encima para que ambos backends compartan un
    único camino de recuperación — redundante, nunca incorrecto, y el coste
    (dos fsync por commit) queda documentado en el benchmark del README.
    """

    def __init__(self, directory: Path) -> None:
        try:
            self._tree = LsmTree.open(directory)
        except LsmStorageIOError as exc:
            raise OperationalError(f"no se pudo abrir el storage LSM: {exc}") from exc
        except LsmError as exc:
            raise InternalError(f"storage LSM inconsistente al abrir: {exc}") from exc
        self._closed = False

    def put(self, key: bytes, value: bytes) -> None:
        try:
            self._tree.put(key, value)
        except LsmStorageIOError as exc:
            raise OperationalError(f"fallo de E/S escribiendo en el LSM: {exc}") from exc
        except LsmError as exc:
            raise InternalError(f"error del motor LSM en put: {exc}") from exc

    def get(self, key: bytes) -> bytes | None:
        try:
            return self._tree.get(key)
        except LsmStorageIOError as exc:
            raise OperationalError(f"fallo de E/S leyendo del LSM: {exc}") from exc
        except LsmError as exc:
            raise InternalError(f"error del motor LSM en get: {exc}") from exc

    def delete(self, key: bytes) -> None:
        # El delete del LSM escribe un tombstone: borrar una clave ausente
        # es válido por diseño (la idempotencia que exige el Protocol).
        try:
            self._tree.delete(key)
        except LsmStorageIOError as exc:
            raise OperationalError(f"fallo de E/S borrando del LSM: {exc}") from exc
        except LsmError as exc:
            raise InternalError(f"error del motor LSM en delete: {exc}") from exc

    def scan(
        self, start: bytes | None = None, end: bytes | None = None
    ) -> Iterator[tuple[bytes, bytes]]:
        try:
            yield from self._tree.scan(start, end)
        except LsmStorageIOError as exc:
            raise OperationalError(f"fallo de E/S recorriendo el LSM: {exc}") from exc
        except LsmError as exc:
            raise InternalError(f"error del motor LSM en scan: {exc}") from exc

    def flush(self) -> None:
        try:
            self._tree.flush()
        except LsmStorageIOError as exc:
            raise OperationalError(f"fallo de E/S en flush del LSM: {exc}") from exc
        except LsmError as exc:
            raise InternalError(f"error del motor LSM en flush: {exc}") from exc

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._tree.close()
        except LsmStorageIOError as exc:
            raise OperationalError(f"fallo de E/S cerrando el LSM: {exc}") from exc
