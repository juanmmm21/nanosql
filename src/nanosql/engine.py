"""Motor de base de datos: el ensamblaje real de los subproyectos.

`DatabaseEngine` es el objeto (uno por base de datos abierta, compartido por
todas las conexiones del proceso) que orquesta el ciclo de vida completo de
una sentencia:

    SQL → sql-query-parser → (traducción) → cost-based-query-optimizer
        → (traducción) → vectorized-execution-engine → MvccTableSource
        → mvcc-transaction-manager → DurableRowStore
        → write-ahead-log-recovery + backend de storage (B+Tree o LSM)

con `lock-manager-deadlock-detector` fencing el DDL: toda sentencia adquiere
locks SHARED de tabla bajo su transacción (liberados en commit/rollback,
2PL estricto) y el DDL exige EXCLUSIVE — así un CREATE INDEX nunca cambia el
esquema bajo los pies de una transacción en vuelo. Los conflictos de
escritura entre transacciones NO usan locks: los resuelve MVCC con
first-committer-wins, que es exactamente el reparto de responsabilidades
descrito en el README.

Sincronización: un `RLock` de engine serializa la ejecución de sentencias y
los commits (el buffer pool del B+Tree no es thread-safe). Los locks de
tabla se adquieren SIEMPRE antes de tomar ese `RLock`: esperar un lock de
tabla con el `RLock` en mano bloquearía el commit que debe liberarlo.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field, replace
from pathlib import Path

import cost_based_query_optimizer.errors as opt_errors
import cost_based_query_optimizer.models as om
import numpy as np
import sql_query_parser.models as pm
import vectorized_execution_engine.errors as vec_errors
from cost_based_query_optimizer.pipeline import OptimizationResult, optimize
from cost_based_query_optimizer.pipeline import explain as optimizer_explain
from lock_manager_deadlock_detector import (
    DeadlockAbortError,
    LockManager,
    LockMode,
    LockTimeoutError,
)
from lock_manager_deadlock_detector import (
    TransactionId as LockOwnerId,
)
from mvcc_transaction_manager import (
    IsolationLevel,
    MVCCTransactionManager,
    SerializationConflictError,
    TransactionId,
    TransactionNotActiveError,
    TransactionStatus,
    WriteConflictError,
)
from vectorized_execution_engine.batch import RowBatch
from vectorized_execution_engine.pipeline import collect

from .catalog import (
    SQL_TYPE_TO_COLUMN_TYPE,
    Catalog,
    ColumnSchema,
    ColumnValue,
    IndexSchema,
    TableSchema,
    load_catalog,
    save_catalog,
)
from .durability import WalDurability, recover_storage
from .errors import (
    DataError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TransactionConflictError,
)
from .rowstore import ROW_ID_SEPARATOR, DurableRowStore, make_row_id, split_row_id
from .source import ROWID_COLUMN, MvccTableSource, decode_row_payload, encode_row_payload
from .storage import BPlusTreeBackend, KeyValueBackend, LsmTreeBackend
from .subqueries import contains_subquery, expand_statement, statement_tables
from .translate import (
    expression_to_optimizer,
    physical_plan_to_executor,
    select_to_optimizer,
)

MEMORY_PATH = ":memory:"
CATALOG_FILENAME = "catalog.json"
WAL_FILENAME = "wal.log"
BACKEND_BTREE = "btree"
BACKEND_LSM = "lsm"

# Cadencia de mantenimiento, en commits de escritura. El GC de versiones
# MVCC evita que las cadenas crezcan sin límite (y que la comprobación
# SERIALIZABLE degrade — hallazgo del benchmark de mvcc-transaction-manager);
# el checkpoint acota el tamaño del WAL y el coste de la próxima recuperación.
DEFAULT_GC_INTERVAL = 64
DEFAULT_CHECKPOINT_INTERVAL = 256
DEFAULT_LOCK_TIMEOUT_SECONDS = 10.0

_ISOLATION_LEVELS: dict[str, IsolationLevel] = {
    "read_committed": IsolationLevel.READ_COMMITTED,
    "repeatable_read": IsolationLevel.REPEATABLE_READ,
    "serializable": IsolationLevel.SERIALIZABLE,
}

_AGGREGATE_NAMES = frozenset({"COUNT", "SUM", "AVG", "MIN", "MAX"})


def parse_isolation_level(name: str) -> IsolationLevel:
    try:
        return _ISOLATION_LEVELS[name.lower()]
    except KeyError as exc:
        raise ProgrammingError(
            f"nivel de aislamiento desconocido: {name!r} "
            f"(válidos: {', '.join(sorted(_ISOLATION_LEVELS))})"
        ) from exc


@dataclass(frozen=True, slots=True)
class StatementResult:
    """Resultado de una sentencia ya ejecutada, en tipos Python planos."""

    kind: str
    columns: tuple[str, ...] = ()
    column_types: tuple[str, ...] = ()
    rows: list[tuple[ColumnValue, ...]] = field(default_factory=list)
    rowcount: int = -1


def _validate_identifier(name: str, what: str) -> None:
    if not name:
        raise ProgrammingError(f"el nombre de {what} no puede estar vacío")
    if any(ord(ch) < 32 for ch in name) or ROW_ID_SEPARATOR in name:
        raise ProgrammingError(
            f"el nombre de {what} {name!r} contiene caracteres de control no permitidos"
        )
    if name.startswith("__") and name.endswith("__"):
        # Reservado para columnas sintéticas internas como __rowid__.
        raise ProgrammingError(f"el nombre de {what} {name!r} está reservado para uso interno")


def _numpy_cell_to_python(value: object) -> ColumnValue:
    if isinstance(value, np.generic):
        value = value.item()
    if value is None or isinstance(value, bool | int | float | str):
        return value
    raise InternalError(f"el ejecutor produjo un valor no representable: {value!r}")


def _batch_to_rows(
    batch: RowBatch, *, exclude: frozenset[str] = frozenset()
) -> list[tuple[ColumnValue, ...]]:
    """Materializa un `RowBatch` como tuplas Python. `exclude` recorta las
    columnas internas (claves de ORDER BY ocultas, `__rowid__`) que el plan
    necesitó pero el usuario no proyectó."""
    names = [name for name in batch.schema.columns if name not in exclude]
    if not names:
        return []
    columns: list[list[ColumnValue]] = []
    for name in names:
        values = batch.columns[name]
        nulls = batch.nulls[name]
        columns.append(
            [None if nulls[i] else _numpy_cell_to_python(values[i]) for i in range(len(values))]
        )
    return [tuple(row) for row in zip(*columns, strict=True)]


def _contains_aggregate(expr: pm.Expression) -> bool:
    if isinstance(expr, pm.FunctionCall):
        if expr.name.upper() in _AGGREGATE_NAMES:
            return True
        return any(_contains_aggregate(arg) for arg in expr.arguments)
    if isinstance(expr, pm.BinaryOp):
        return _contains_aggregate(expr.left) or _contains_aggregate(expr.right)
    if isinstance(expr, pm.UnaryOp):
        return _contains_aggregate(expr.operand)
    if isinstance(expr, pm.Between):
        return any(_contains_aggregate(e) for e in (expr.operand, expr.low, expr.high))
    if isinstance(expr, pm.InList):
        return _contains_aggregate(expr.operand) or any(_contains_aggregate(v) for v in expr.values)
    if isinstance(expr, pm.Like):
        return _contains_aggregate(expr.operand) or _contains_aggregate(expr.pattern)
    if isinstance(expr, pm.IsNull):
        return _contains_aggregate(expr.operand)
    return False


def evaluate_constant(expr: pm.Expression) -> ColumnValue:
    """Evalúa una expresión constante (valores de INSERT, DEFAULT de DDL).

    Sólo literales y aritmética/negación sobre ellos: cualquier referencia a
    columna aquí es un error del programador, no algo que evaluar. No
    re-implementa el evaluador vectorizado — ese opera sobre batches de
    filas; esto es plegado de constantes escalares del driver.
    """
    if isinstance(expr, pm.Literal):
        return expr.value
    if isinstance(expr, pm.UnaryOp):
        operand = evaluate_constant(expr.operand)
        if expr.operator in (pm.UnaryOperator.NEG, pm.UnaryOperator.POS):
            if operand is None:
                return None
            if isinstance(operand, bool) or not isinstance(operand, int | float):
                raise DataError(f"no se puede negar el valor no numérico {operand!r}")
            return -operand if expr.operator is pm.UnaryOperator.NEG else operand
        if operand is None:
            return None
        if not isinstance(operand, bool):
            raise DataError(f"NOT requiere un booleano, no {operand!r}")
        return not operand
    if isinstance(expr, pm.BinaryOp):
        left = evaluate_constant(expr.left)
        right = evaluate_constant(expr.right)
        if left is None or right is None:
            return None
        if (
            isinstance(left, bool)
            or isinstance(right, bool)
            or not isinstance(left, int | float)
            or not isinstance(right, int | float)
        ):
            raise DataError("la aritmética constante requiere operandos numéricos")
        op = expr.operator
        if op is pm.BinaryOperator.ADD:
            return left + right
        if op is pm.BinaryOperator.SUB:
            return left - right
        if op is pm.BinaryOperator.MUL:
            return left * right
        if op is pm.BinaryOperator.DIV:
            if right == 0:
                raise DataError("división por cero en expresión constante", sqlstate="22012")
            if isinstance(left, int) and isinstance(right, int):
                # Misma semántica que el ejecutor: división entera truncada.
                return _truncated_quotient(left, right)
            return left / right
        if op is pm.BinaryOperator.MOD:
            if right == 0:
                raise DataError("módulo por cero en expresión constante", sqlstate="22012")
            if isinstance(left, int) and isinstance(right, int):
                return left - right * _truncated_quotient(left, right)
            return math.fmod(left, right)
        raise ProgrammingError(
            f"el operador {op.name} no está permitido en una expresión constante"
        )
    raise ProgrammingError(
        "sólo se admiten expresiones constantes en esta posición "
        f"(encontrado {type(expr).__name__})"
    )


def _truncated_quotient(left: int, right: int) -> int:
    """División entera truncada hacia cero (SQL), no la `//` de Python."""
    quotient = abs(left) // abs(right)
    return -quotient if (left < 0) != (right < 0) else quotient


class DatabaseEngine:
    """Una base de datos abierta. Ver docstring del módulo."""

    def __init__(
        self,
        *,
        path: Path | None,
        backend_name: str,
        kv: KeyValueBackend,
        durability: WalDurability | None,
        catalog: Catalog,
        lock_timeout: float,
        gc_interval: int,
        checkpoint_interval: int,
    ) -> None:
        self._path = path
        self._backend_name = backend_name
        self._kv = kv
        self._durability = durability
        self._catalog = catalog
        self._row_store = DurableRowStore(kv, durability)
        self._locks: LockManager[str] = LockManager(default_timeout=lock_timeout)
        self._lock = threading.RLock()
        self._gc_interval = gc_interval
        self._checkpoint_interval = checkpoint_interval
        self._commits_since_gc = 0
        self._commits_since_checkpoint = 0
        self._next_rowid: dict[str, int] = {}
        self._live_counts: dict[str, int] = {}
        self._txn_written_rows: dict[TransactionId, dict[str, set[int]]] = {}
        self._closed = False

        summary = self._row_store.bootstrap_summary()
        for table, info in summary.tables.items():
            self._next_rowid[table] = info.max_rowid + 1
            self._live_counts[table] = info.live_rows
        # El contador de commits se siembra con el máximo commit_seq ya
        # materializado (API oficial del gestor desde que aceptó el seed por
        # constructor): sin él, nada persistido sería visible al reabrir.
        self._mvcc = MVCCTransactionManager(
            self._row_store, last_committed_seq=summary.max_commit_seq
        )

    # ------------------------------------------------------------------
    # Apertura
    # ------------------------------------------------------------------

    @classmethod
    def open(
        cls,
        database: str | Path,
        *,
        backend: str | None = None,
        lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
        gc_interval: int = DEFAULT_GC_INTERVAL,
        checkpoint_interval: int = DEFAULT_CHECKPOINT_INTERVAL,
    ) -> DatabaseEngine:
        if str(database) == MEMORY_PATH:
            backend_name = backend or BACKEND_BTREE
            if backend_name != BACKEND_BTREE:
                raise NotSupportedError(
                    "las bases de datos :memory: sólo soportan el backend 'btree' "
                    "(el motor LSM necesita un directorio para sus SSTables)"
                )
            return cls(
                path=None,
                backend_name=backend_name,
                kv=BPlusTreeBackend(None),
                durability=None,
                catalog=Catalog(),
                lock_timeout=lock_timeout,
                gc_interval=gc_interval,
                checkpoint_interval=checkpoint_interval,
            )

        directory = Path(database)
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise OperationalError(f"no se pudo crear el directorio {directory}: {exc}") from exc

        catalog_path = directory / CATALOG_FILENAME
        if catalog_path.exists():
            catalog, stored_backend = load_catalog(catalog_path)
            if backend is not None and backend != stored_backend:
                raise OperationalError(
                    f"la base de datos {directory} se creó con el backend "
                    f"'{stored_backend}' y no puede reabrirse como '{backend}'"
                )
            backend_name = stored_backend
        else:
            catalog = Catalog()
            backend_name = backend or BACKEND_BTREE

        kv = cls._open_backend(backend_name, directory)
        wal_path = directory / WAL_FILENAME
        recover_storage(wal_path, kv)
        durability = WalDurability(wal_path)
        # Tras la recuperación el log ya está reflejado en el storage: se
        # arranca con el WAL truncado para no re-recorrer historia antigua
        # en cada apertura.
        durability.checkpoint(kv)

        engine = cls(
            path=directory,
            backend_name=backend_name,
            kv=kv,
            durability=durability,
            catalog=catalog,
            lock_timeout=lock_timeout,
            gc_interval=gc_interval,
            checkpoint_interval=checkpoint_interval,
        )
        if not catalog_path.exists():
            save_catalog(catalog_path, catalog, backend_name=backend_name)
        return engine

    @staticmethod
    def _open_backend(backend_name: str, directory: Path) -> KeyValueBackend:
        if backend_name == BACKEND_BTREE:
            return BPlusTreeBackend(directory / "data.btree")
        if backend_name == BACKEND_LSM:
            return LsmTreeBackend(directory / "lsm")
        raise NotSupportedError(f"backend de storage desconocido: {backend_name!r}")

    @property
    def backend_name(self) -> str:
        return self._backend_name

    @property
    def catalog(self) -> Catalog:
        return self._catalog

    def catalog_snapshot(self) -> Catalog:
        """Copia consistente del catálogo, tomada bajo el lock del engine:
        segura de recorrer desde otro hilo mientras el DDL sigue avanzando
        (los esquemas son inmutables; sólo se copian los diccionarios)."""
        with self._lock:
            return Catalog(tables=dict(self._catalog.tables), indexes=dict(self._catalog.indexes))

    # ------------------------------------------------------------------
    # Transacciones
    # ------------------------------------------------------------------

    def begin_transaction(self, isolation: IsolationLevel) -> TransactionId:
        with self._lock:
            self._ensure_open()
            txn_id = self._mvcc.begin(isolation)
            self._txn_written_rows[txn_id] = {}
            return txn_id

    def commit(self, txn_id: TransactionId) -> None:
        # try/finally sobre TODO el commit: un commit fallido (conflicto,
        # violación de unicidad, transacción muerta) también debe liberar
        # los locks de tabla de la transacción — retenerlos bloquearía
        # cualquier DDL futuro sobre esas tablas para siempre.
        try:
            with self._lock:
                self._ensure_open()
                written = self._txn_written_rows.pop(txn_id, {})
                try:
                    self._validate_unique_at_commit(txn_id, written)
                except IntegrityError:
                    # La transacción no puede confirmarse tal cual; se
                    # aborta (mismo contrato que un conflicto de escritura:
                    # el llamador reintenta la transacción entera).
                    self._mvcc.abort(txn_id)
                    raise
                wal_txn: int | None = None
                if self._durability is not None and written:
                    wal_txn = self._durability.begin()
                    self._row_store.begin_logged_batch(wal_txn)
                try:
                    self._mvcc.commit(txn_id)
                except (WriteConflictError, SerializationConflictError) as exc:
                    # El gestor detecta el conflicto ANTES de materializar
                    # nada: la transacción WAL, si existe, se cierra vacía.
                    if wal_txn is not None:
                        self._durability_abort(wal_txn)
                    raise TransactionConflictError(str(exc)) from exc
                except TransactionNotActiveError as exc:
                    if wal_txn is not None:
                        self._durability_abort(wal_txn)
                    raise ProgrammingError(str(exc)) from exc
                finally:
                    self._row_store.end_logged_batch()
                if wal_txn is not None and self._durability is not None:
                    self._durability.commit(wal_txn)
                for table in written:
                    self._live_counts.pop(table, None)
                if written:
                    self._commits_since_gc += 1
                    self._commits_since_checkpoint += 1
                    self._maybe_run_maintenance()
        finally:
            self._locks.release_all(LockOwnerId(int(txn_id)))

    def _validate_unique_at_commit(
        self, txn_id: TransactionId, written: dict[str, set[int]]
    ) -> None:
        """Revalidación de unicidad contra lo ÚLTIMO confirmado, en commit.

        Las comprobaciones por sentencia validan contra el snapshot de la
        propia transacción, que no ve lo que otra transacción concurrente
        confirmó después: dos transacciones podían insertar el mismo valor
        "único" sin verse. Como el lock de ejecución del engine serializa
        los commits, revalidar aquí las filas escritas contra una vista
        READ_COMMITTED fresca (excluyendo las propias filas escritas, que
        esta transacción va a sobrescribir o borrar) cierra ese hueco por
        completo dentro del proceso.
        """
        for table_name, rowids in written.items():
            table = self._catalog.tables.get(table_name)
            if table is None:
                continue  # tabla con escrituras pero sin esquema: imposible salvo bug
            constraints = self._unique_constraints(table)
            if not constraints:
                continue
            mine: list[dict[str, ColumnValue]] = []
            for rowid in rowids:
                payload = self._mvcc.get(txn_id, make_row_id(table_name, rowid))
                if payload is not None:  # los borrados no pueden crear duplicados
                    mine.append(decode_row_payload(payload))
            if not mine:
                continue
            probe = self._mvcc.begin(IsolationLevel.READ_COMMITTED)
            try:
                committed = [
                    row
                    for rowid, row in self._visible_table_rows(probe, table_name)
                    if rowid not in rowids
                ]
            finally:
                self._mvcc.abort(probe)
            for constraint in constraints:
                existing: set[tuple[ColumnValue, ...]] = set()
                for row in committed:
                    committed_key = tuple(row.get(c) for c in constraint)
                    if None not in committed_key:
                        existing.add(committed_key)
                for row in mine:
                    key = tuple(row.get(c) for c in constraint)
                    if None in key:
                        continue
                    if key in existing:
                        raise IntegrityError(
                            f"commit rechazado: otra transacción confirmó antes el valor "
                            f"{key!r} para la restricción única "
                            f"({', '.join(constraint)}) de '{table_name}'",
                            sqlstate="23505",
                        )

    def transaction_active(self, txn_id: TransactionId) -> bool:
        try:
            return self._mvcc.status_of(txn_id) is TransactionStatus.ACTIVE
        except TransactionNotActiveError:
            return False

    def rollback(self, txn_id: TransactionId) -> None:
        with self._lock:
            self._ensure_open()
            self._txn_written_rows.pop(txn_id, None)
            try:
                self._mvcc.abort(txn_id)
            except TransactionNotActiveError:
                # La transacción pudo morir ya (víctima de un conflicto):
                # el rollback debe ser idempotente para el driver.
                pass
        self._locks.release_all(LockOwnerId(int(txn_id)))

    def _durability_abort(self, wal_txn: int) -> None:
        if self._durability is not None:
            self._durability.abort(wal_txn)

    def _maybe_run_maintenance(self) -> None:
        if self._commits_since_gc >= self._gc_interval:
            self._commits_since_gc = 0
            self._run_gc()
        if self._durability is not None and (
            self._commits_since_checkpoint >= self._checkpoint_interval
        ):
            self._commits_since_checkpoint = 0
            self._durability.checkpoint(self._kv)

    def _run_gc(self) -> None:
        """Poda de versiones muertas, registrada en el WAL como cualquier
        otra mutación del storage (los borrados también deben sobrevivir o
        deshacerse coherentemente ante un crash)."""
        wal_txn: int | None = None
        if self._durability is not None:
            wal_txn = self._durability.begin()
            self._row_store.begin_logged_batch(wal_txn)
        try:
            self._mvcc.gc()
        finally:
            self._row_store.end_logged_batch()
        if wal_txn is not None and self._durability is not None:
            self._durability.commit(wal_txn)

    # ------------------------------------------------------------------
    # Ejecución de sentencias
    # ------------------------------------------------------------------

    def execute_statement(self, txn_id: TransactionId, stmt: pm.Statement) -> StatementResult:
        if not isinstance(
            stmt,
            pm.SelectStatement | pm.InsertStatement | pm.UpdateStatement | pm.DeleteStatement,
        ):
            raise InternalError(f"sentencia no enrutable: {type(stmt).__name__}")
        # Los locks SHARED cubren también las tablas de las subconsultas (a
        # cualquier profundidad): la expansión las va a LEER bajo esta misma
        # transacción, y deben quedar tan protegidas frente al DDL como las
        # tablas de la sentencia externa.
        self._acquire_table_locks(txn_id, statement_tables(stmt), LockMode.SHARED)
        with self._lock:
            self._ensure_open()
            # Materialización de subconsultas ANTES de traducir/planificar,
            # con el mismo txn (mismo snapshot MVCC) que la sentencia externa.
            stmt = expand_statement(
                stmt, self._catalog, lambda select: self._execute_select(txn_id, select)
            )
            if isinstance(stmt, pm.SelectStatement):
                return self._execute_select(txn_id, stmt)
            if isinstance(stmt, pm.InsertStatement):
                return self._execute_insert(txn_id, stmt)
            if isinstance(stmt, pm.UpdateStatement):
                return self._execute_update(txn_id, stmt)
            if isinstance(stmt, pm.DeleteStatement):
                return self._execute_delete(txn_id, stmt)
            # Imposible por construcción: la expansión conserva el tipo de
            # la sentencia; mypy no puede saberlo, un humano sí.
            raise InternalError(
                f"la expansión cambió el tipo de la sentencia: {type(stmt).__name__}"
            )

    def execute_ddl(self, stmt: pm.Statement) -> StatementResult:
        """DDL bajo lock EXCLUSIVE de la tabla afectada. El llamador (la
        conexión) ya confirmó su transacción abierta: el DDL de nanosql
        autocommitea, como en MySQL, y se documenta así."""
        ddl_owner = self.begin_transaction(IsolationLevel.READ_COMMITTED)
        try:
            if isinstance(stmt, pm.CreateTableStatement):
                self._acquire_table_locks(ddl_owner, [stmt.table], LockMode.EXCLUSIVE)
                with self._lock:
                    self._ensure_open()
                    return self._execute_create_table(stmt)
            if isinstance(stmt, pm.CreateIndexStatement):
                self._acquire_table_locks(ddl_owner, [stmt.table], LockMode.EXCLUSIVE)
                with self._lock:
                    self._ensure_open()
                    return self._execute_create_index(ddl_owner, stmt)
            if isinstance(stmt, pm.DropTableStatement):
                self._acquire_table_locks(ddl_owner, [stmt.table], LockMode.EXCLUSIVE)
                with self._lock:
                    self._ensure_open()
                    return self._execute_drop_table(stmt)
            raise InternalError(f"DDL no enrutable: {type(stmt).__name__}")
        finally:
            self.rollback(ddl_owner)

    def _acquire_table_locks(
        self, txn_id: TransactionId, tables: list[str], mode: LockMode
    ) -> None:
        owner = LockOwnerId(int(txn_id))
        # Orden total (alfabético) de adquisición: previene deadlocks entre
        # sentencias multi-tabla incluso sin depender del detector.
        for table in sorted(set(tables)):
            try:
                self._locks.acquire(owner, table, mode)
            except DeadlockAbortError as exc:
                self.rollback(txn_id)
                raise TransactionConflictError(
                    f"transacción abortada como víctima de deadlock sobre '{table}'",
                    sqlstate="40P01",
                ) from exc
            except LockTimeoutError as exc:
                raise OperationalError(
                    f"timeout esperando el lock de la tabla '{table}' "
                    "(¿DDL concurrente con una transacción larga?)",
                    sqlstate="55P03",
                ) from exc

    # -- SELECT ---------------------------------------------------------

    def _execute_select(self, txn_id: TransactionId, stmt: pm.SelectStatement) -> StatementResult:
        tables = [stmt.from_table.name] + [join.table.name for join in stmt.joins]
        for table in tables:
            self._catalog.table(table)  # existencia; error tipado si falta
        translated = select_to_optimizer(stmt, self._star_columns(tables))
        batch = self._run_select_plan(txn_id, translated.statement, tables, include_rowid=False)
        hidden = frozenset(translated.hidden_columns)
        rows = _batch_to_rows(batch, exclude=hidden)
        visible = [name for name in batch.schema.columns if name not in hidden]
        types = tuple(batch.schema.type_of(name).name for name in visible)
        return StatementResult(
            kind="select",
            columns=tuple(translated.output_names.get(name, name) for name in visible),
            column_types=types,
            rows=rows,
            rowcount=len(rows),
        )

    def _star_columns(self, tables: list[str]) -> dict[str, tuple[str, ...]]:
        return {name: self._catalog.table(name).column_names() for name in tables}

    def _optimize_select(
        self, opt_select: om.SelectStatement, tables: list[str], *, include_rowid: bool
    ) -> OptimizationResult:
        stats_catalog = self._optimizer_catalog(tables, include_rowid=include_rowid)
        try:
            return optimize(opt_select, stats_catalog)
        except opt_errors.UnresolvedColumnError as exc:
            raise ProgrammingError(f"columna no resuelta: {exc}", sqlstate="42703") from exc
        except opt_errors.UnsupportedQueryError as exc:
            raise NotSupportedError(f"consulta no soportada por el optimizador: {exc}") from exc
        except opt_errors.MissingStatisticsError as exc:
            raise ProgrammingError(
                f"tabla desconocida para el optimizador: {exc}", sqlstate="42P01"
            ) from exc
        except opt_errors.OptimizerError as exc:
            raise InternalError(f"el optimizador falló: {exc}") from exc

    def explain_select(self, stmt: pm.SelectStatement) -> StatementResult:
        """`EXPLAIN <select>`: el plan físico elegido por el optimizador con
        cardinalidades y costes estimados, renderizado por el `explain` del
        propio optimizador. Planifica contra las estadísticas actuales sin
        abrir transacción ni tomar locks de tabla: no lee datos.
        """
        if contains_subquery(stmt):
            raise NotSupportedError(
                "EXPLAIN no soporta consultas con subconsultas: la estrategia de "
                "materialización tendría que ejecutarlas para poder planificar"
            )
        with self._lock:
            self._ensure_open()
            tables = [stmt.from_table.name] + [join.table.name for join in stmt.joins]
            for table in tables:
                self._catalog.table(table)
            translated = select_to_optimizer(stmt, self._star_columns(tables))
            result = self._optimize_select(translated.statement, tables, include_rowid=False)
        text = optimizer_explain(result.physical_plan)
        rows: list[tuple[ColumnValue, ...]] = [(line,) for line in text.splitlines()]
        return StatementResult(
            kind="explain",
            columns=("query plan",),
            column_types=("STRING",),
            rows=rows,
            rowcount=len(rows),
        )

    def _run_select_plan(
        self,
        txn_id: TransactionId,
        opt_select: om.SelectStatement,
        tables: list[str],
        *,
        include_rowid: bool,
    ) -> RowBatch:
        result = self._optimize_select(opt_select, tables, include_rowid=include_rowid)
        plan = physical_plan_to_executor(result.physical_plan)
        source = MvccTableSource(self._mvcc, txn_id, self._catalog, include_rowid=include_rowid)
        try:
            return collect(plan, source)
        except vec_errors.DivisionByZeroError as exc:
            raise DataError(str(exc), sqlstate="22012") from exc
        except vec_errors.UnresolvedColumnError as exc:
            raise ProgrammingError(
                f"columna no resuelta en ejecución: {exc}", sqlstate="42703"
            ) from exc
        except (
            vec_errors.UnsupportedExpressionError,
            vec_errors.UnsupportedOperatorError,
            vec_errors.UnsupportedJoinConditionError,
        ) as exc:
            raise NotSupportedError(f"característica no soportada por el ejecutor: {exc}") from exc
        except vec_errors.MemoryLimitExceededError as exc:
            raise OperationalError(f"presupuesto de memoria del ejecutor agotado: {exc}") from exc
        except vec_errors.SchemaConflictError as exc:
            raise ProgrammingError(str(exc)) from exc
        except vec_errors.ExecutionEngineError as exc:
            raise InternalError(f"el ejecutor falló: {exc}") from exc

    def _optimizer_catalog(self, tables: list[str], *, include_rowid: bool) -> om.Catalog:
        stats: dict[str, om.TableStatistics] = {}
        for table_name in set(tables):
            table = self._catalog.table(table_name)
            row_count = self._live_count(table_name)
            # Sin recolector de estadísticas por columna en v1: se asume
            # distinct_count = filas (columna casi única). Es la suposición
            # documentada más conservadora para joins; los histogramas del
            # optimizador quedan disponibles para cuando exista un ANALYZE.
            columns = {
                column.name: om.ColumnStatistics(distinct_count=max(1, row_count))
                for column in table.columns
            }
            if include_rowid:
                columns[ROWID_COLUMN] = om.ColumnStatistics(distinct_count=max(1, row_count))
            indexes = tuple(
                om.IndexStatistics(name=idx.name, columns=idx.columns, unique=idx.unique)
                for idx in self._catalog.indexes_of(table_name)
            )
            stats[table_name] = om.TableStatistics(
                row_count=row_count, columns=columns, indexes=indexes
            )
        return om.Catalog(tables=stats)

    def _live_count(self, table: str) -> int:
        cached = self._live_counts.get(table)
        if cached is None:
            cached = self._row_store.live_row_count(table)
            self._live_counts[table] = cached
        return cached

    # -- INSERT ---------------------------------------------------------

    def _execute_insert(self, txn_id: TransactionId, stmt: pm.InsertStatement) -> StatementResult:
        table = self._catalog.table(stmt.table)
        if stmt.columns:
            for name in stmt.columns:
                table.column(name)  # error tipado si no existe
            if len(set(stmt.columns)) != len(stmt.columns):
                raise ProgrammingError("columna repetida en la lista de columnas del INSERT")
            target_columns = stmt.columns
        else:
            target_columns = table.column_names()

        provided_rows: list[dict[str, ColumnValue]] = []
        if stmt.source is not None:
            # INSERT ... SELECT: la consulta origen corre por el pipeline
            # completo bajo la MISMA transacción (mismo snapshot MVCC); sus
            # filas se insertan con las mismas reglas que un VALUES.
            selected = self._execute_select(txn_id, stmt.source)
            if len(selected.columns) != len(target_columns):
                raise ProgrammingError(
                    f"INSERT espera {len(target_columns)} columnas y el SELECT devuelve "
                    f"{len(selected.columns)}",
                    sqlstate="42601",
                )
            provided_rows = [dict(zip(target_columns, row, strict=True)) for row in selected.rows]
        for row_exprs in stmt.rows:
            if len(row_exprs) != len(target_columns):
                raise ProgrammingError(
                    f"INSERT espera {len(target_columns)} valores por fila, "
                    f"recibidos {len(row_exprs)}"
                )
            provided_rows.append(
                {
                    name: evaluate_constant(expr)
                    for name, expr in zip(target_columns, row_exprs, strict=True)
                }
            )

        new_rows: list[dict[str, ColumnValue]] = []
        for provided in provided_rows:
            row: dict[str, ColumnValue] = {}
            for column in table.columns:
                if column.name in provided:
                    value = provided[column.name]
                elif column.has_default:
                    value = column.default
                else:
                    value = None
                row[column.name] = self._coerce_value(table.name, column, value)
            new_rows.append(row)

        self._check_unique_constraints(txn_id, table, new_rows, exclude_rowids=set())

        for row in new_rows:
            rowid = self._allocate_rowid(table.name)
            self._mvcc_put(txn_id, table.name, rowid, row)
            self._record_write(txn_id, table.name, rowid)
        return StatementResult(kind="insert", rowcount=len(new_rows))

    def _record_write(self, txn_id: TransactionId, table: str, rowid: int) -> None:
        per_table = self._txn_written_rows.setdefault(txn_id, {})
        per_table.setdefault(table, set()).add(rowid)

    def _allocate_rowid(self, table: str) -> int:
        rowid = self._next_rowid.get(table, 1)
        self._next_rowid[table] = rowid + 1
        return rowid

    def _mvcc_put(
        self, txn_id: TransactionId, table: str, rowid: int, row: dict[str, ColumnValue]
    ) -> None:
        try:
            self._mvcc.put(txn_id, make_row_id(table, rowid), encode_row_payload(row))
        except TransactionNotActiveError as exc:
            raise ProgrammingError(str(exc)) from exc

    def _coerce_value(
        self, table_name: str, column: ColumnSchema, value: ColumnValue
    ) -> ColumnValue:
        from vectorized_execution_engine.models import ColumnType

        if value is None:
            if column.not_null or column.primary_key:
                raise IntegrityError(
                    f"la columna '{column.name}' de '{table_name}' no admite NULL",
                    sqlstate="23502",
                )
            return None
        # bool antes que int: en Python bool es subclase de int y colaría
        # como entero en una columna INTEGER.
        if isinstance(value, bool):
            if column.column_type is not ColumnType.BOOLEAN:
                raise DataError(
                    f"valor booleano para la columna {column.column_type.name} "
                    f"'{column.name}' de '{table_name}'",
                    sqlstate="42804",
                )
            return value
        if isinstance(value, int):
            if column.column_type is ColumnType.INTEGER:
                return value
            if column.column_type is ColumnType.FLOAT:
                return float(value)
            raise DataError(
                f"valor entero para la columna {column.column_type.name} "
                f"'{column.name}' de '{table_name}'",
                sqlstate="42804",
            )
        if isinstance(value, float):
            if column.column_type is ColumnType.FLOAT:
                return value
            raise DataError(
                f"valor de coma flotante para la columna {column.column_type.name} "
                f"'{column.name}' de '{table_name}' (no hay truncado implícito)",
                sqlstate="42804",
            )
        if isinstance(value, str):
            if column.column_type is ColumnType.STRING:
                return value
            raise DataError(
                f"valor de texto para la columna {column.column_type.name} "
                f"'{column.name}' de '{table_name}'",
                sqlstate="42804",
            )
        raise DataError(f"tipo de valor no soportado: {type(value).__name__}", sqlstate="42804")

    # -- restricciones de unicidad ---------------------------------------

    def _unique_constraints(self, table: TableSchema) -> list[tuple[str, ...]]:
        constraints: list[tuple[str, ...]] = []
        for column in table.columns:
            if column.primary_key or column.unique:
                constraints.append((column.name,))
        for index in self._catalog.indexes_of(table.name):
            if index.unique:
                constraints.append(index.columns)
        # Dedupe conservando orden (una PK con índice único encima no debe
        # comprobarse dos veces).
        seen: set[tuple[str, ...]] = set()
        unique: list[tuple[str, ...]] = []
        for constraint in constraints:
            if constraint not in seen:
                seen.add(constraint)
                unique.append(constraint)
        return unique

    def _visible_table_rows(
        self, txn_id: TransactionId, table: str
    ) -> list[tuple[int, dict[str, ColumnValue]]]:
        prefix = f"{table}{ROW_ID_SEPARATOR}"
        try:
            visible = self._mvcc.scan(txn_id)
        except TransactionNotActiveError as exc:
            raise ProgrammingError(str(exc)) from exc
        return [
            (split_row_id(row_id)[1], decode_row_payload(payload))
            for row_id, payload in visible
            if str(row_id).startswith(prefix)
        ]

    def _check_unique_constraints(
        self,
        txn_id: TransactionId,
        table: TableSchema,
        candidate_rows: list[dict[str, ColumnValue]],
        *,
        exclude_rowids: set[int],
    ) -> None:
        """Unicidad evaluada contra el snapshot de la propia transacción.

        Limitación honesta (documentada en el README): dos transacciones
        concurrentes pueden insertar el mismo valor "único" sin verse — el
        first-committer-wins de MVCC protege filas, no valores de columna.
        Cerrar ese hueco requiere validación de unicidad en el commit del
        gestor MVCC (mejora propuesta al subproyecto, no parcheada aquí).
        """
        constraints = self._unique_constraints(table)
        if not constraints:
            return
        existing = self._visible_table_rows(txn_id, table.name)
        for constraint in constraints:
            seen: set[tuple[ColumnValue, ...]] = set()
            for rowid, row in existing:
                if rowid in exclude_rowids:
                    continue
                key = tuple(row.get(col) for col in constraint)
                if None not in key:
                    seen.add(key)
            for row in candidate_rows:
                key = tuple(row.get(col) for col in constraint)
                if None in key:
                    continue  # los NULL nunca chocan entre sí, como en SQL
                if key in seen:
                    raise IntegrityError(
                        f"valor duplicado {key!r} para la restricción única "
                        f"({', '.join(constraint)}) de '{table.name}'",
                        sqlstate="23505",
                    )
                seen.add(key)

    # -- UPDATE / DELETE --------------------------------------------------

    def _matching_rows(
        self,
        txn_id: TransactionId,
        table: str,
        where: pm.Expression | None,
        extra_items: list[om.SelectItem],
    ) -> RowBatch:
        """Ejecuta el pipeline completo (optimizador + ejecutor vectorizado)
        para identificar las filas afectadas por un UPDATE/DELETE: un SELECT
        sintético de la columna interna `__rowid__` (más las expresiones de
        asignación, en el caso del UPDATE) con el mismo WHERE."""
        if where is not None and _contains_aggregate(where):
            raise ProgrammingError("las funciones agregadas no están permitidas en WHERE")
        items = [
            om.SelectItem(expression=om.ColumnRef(name=ROWID_COLUMN), alias=ROWID_COLUMN)
        ] + extra_items
        opt_select = om.SelectStatement(
            columns=tuple(items),
            from_table=om.TableRef(name=table),
            where=expression_to_optimizer(where) if where is not None else None,
        )
        return self._run_select_plan(txn_id, opt_select, [table], include_rowid=True)

    def _execute_update(self, txn_id: TransactionId, stmt: pm.UpdateStatement) -> StatementResult:
        table = self._catalog.table(stmt.table)
        if not stmt.assignments:
            raise ProgrammingError("UPDATE sin asignaciones")
        assigned_names: list[str] = []
        for assignment in stmt.assignments:
            table.column(assignment.column)
            if assignment.column in assigned_names:
                raise ProgrammingError(
                    f"la columna '{assignment.column}' aparece dos veces en el SET",
                    sqlstate="42601",
                )
            if _contains_aggregate(assignment.value):
                raise ProgrammingError("las funciones agregadas no están permitidas en SET")
            assigned_names.append(assignment.column)

        extra_items = [
            om.SelectItem(
                expression=expression_to_optimizer(assignment.value),
                alias=f"__assign_{i}__",
            )
            for i, assignment in enumerate(stmt.assignments)
        ]
        batch = self._matching_rows(txn_id, stmt.table, stmt.where, extra_items)
        matched = _batch_to_rows(batch)
        if not matched:
            return StatementResult(kind="update", rowcount=0)

        updated_rows: list[tuple[int, dict[str, ColumnValue]]] = []
        for row_values in matched:
            rowid = row_values[0]
            if not isinstance(rowid, int):
                raise InternalError(f"__rowid__ no entero en el plan de UPDATE: {rowid!r}")
            current_payload = self._mvcc.get(txn_id, make_row_id(stmt.table, rowid))
            if current_payload is None:
                # Imposible salvo bug: la fila acaba de ser visible en el
                # mismo snapshot que ahora la lee.
                raise InternalError(f"fila {rowid} de '{stmt.table}' desapareció del snapshot")
            row = decode_row_payload(current_payload)
            for offset, assignment in enumerate(stmt.assignments):
                column = table.column(assignment.column)
                row[column.name] = self._coerce_value(stmt.table, column, row_values[1 + offset])
            updated_rows.append((rowid, row))

        constrained = {
            name for constraint in self._unique_constraints(table) for name in constraint
        }
        if constrained.intersection(assigned_names):
            self._check_unique_constraints(
                txn_id,
                table,
                [row for _, row in updated_rows],
                exclude_rowids={rowid for rowid, _ in updated_rows},
            )

        for rowid, row in updated_rows:
            self._mvcc_put(txn_id, stmt.table, rowid, row)
            self._record_write(txn_id, stmt.table, rowid)
        return StatementResult(kind="update", rowcount=len(updated_rows))

    def _execute_delete(self, txn_id: TransactionId, stmt: pm.DeleteStatement) -> StatementResult:
        self._catalog.table(stmt.table)
        batch = self._matching_rows(txn_id, stmt.table, stmt.where, extra_items=[])
        matched = _batch_to_rows(batch)
        for (rowid,) in matched:
            if not isinstance(rowid, int):
                raise InternalError(f"__rowid__ no entero en el plan de DELETE: {rowid!r}")
            try:
                self._mvcc.delete(txn_id, make_row_id(stmt.table, rowid))
            except TransactionNotActiveError as exc:
                raise ProgrammingError(str(exc)) from exc
            self._record_write(txn_id, stmt.table, rowid)
        return StatementResult(kind="delete", rowcount=len(matched))

    # -- DDL --------------------------------------------------------------

    def _execute_create_table(self, stmt: pm.CreateTableStatement) -> StatementResult:
        _validate_identifier(stmt.table, "tabla")
        if stmt.table in self._catalog.tables:
            if stmt.if_not_exists:
                return StatementResult(kind="create_table", rowcount=0)
            raise ProgrammingError(f"la tabla '{stmt.table}' ya existe", sqlstate="42P07")

        columns: list[ColumnSchema] = []
        seen_names: set[str] = set()
        primary_keys = 0
        for definition in stmt.columns:
            _validate_identifier(definition.name, "columna")
            if definition.name in seen_names:
                raise ProgrammingError(f"columna repetida en CREATE TABLE: '{definition.name}'")
            seen_names.add(definition.name)
            type_name = definition.data_type.name.upper()
            column_type = SQL_TYPE_TO_COLUMN_TYPE.get(type_name)
            if column_type is None:
                raise NotSupportedError(
                    f"el tipo {type_name} es SQL válido pero el motor vectorizado no "
                    "tiene representación columnar para él"
                )
            if definition.primary_key:
                primary_keys += 1
                if primary_keys > 1:
                    raise NotSupportedError(
                        "claves primarias compuestas no soportadas: declara una única "
                        "columna PRIMARY KEY (o un índice UNIQUE multicolumna)"
                    )
            default_value: ColumnValue = None
            has_default = definition.default is not None
            column = ColumnSchema(
                name=definition.name,
                column_type=column_type,
                not_null=definition.not_null or definition.primary_key,
                primary_key=definition.primary_key,
                unique=definition.unique,
                default=None,
                has_default=has_default,
            )
            if has_default and definition.default is not None:
                default_value = self._coerce_value(
                    stmt.table, column, evaluate_constant(definition.default)
                )
                column = ColumnSchema(
                    name=column.name,
                    column_type=column.column_type,
                    not_null=column.not_null,
                    primary_key=column.primary_key,
                    unique=column.unique,
                    default=default_value,
                    has_default=True,
                )
            columns.append(column)

        implicit_indexes = self._apply_table_constraints(stmt, columns, primary_keys)
        self._catalog.add_table(TableSchema(name=stmt.table, columns=tuple(columns)))
        for index in implicit_indexes:
            self._catalog.add_index(index)
        self._next_rowid.setdefault(stmt.table, 1)
        self._live_counts[stmt.table] = 0
        self._persist_catalog()
        return StatementResult(kind="create_table", rowcount=0)

    def _apply_table_constraints(
        self,
        stmt: pm.CreateTableStatement,
        columns: list[ColumnSchema],
        column_level_pks: int,
    ) -> list[IndexSchema]:
        """Restricciones de tabla (`PRIMARY KEY (..)`, `UNIQUE (..)`), la forma
        que emiten los ORMs. Una PK de una sola columna equivale exactamente
        a la PK de columna. Una PK compuesta se materializa como columnas NOT
        NULL más un índice único implícito — en nanosql la PK no tiene
        significado físico (las filas se ordenan por rowid), así que ambas
        formas imponen la misma restricción. Los nombres implícitos siguen la
        convención de PostgreSQL (`<tabla>_pkey`, `<tabla>_<cols>_key`).
        Modifica `columns` in situ y devuelve los índices a registrar."""
        by_name = {column.name: i for i, column in enumerate(columns)}
        indexes: list[IndexSchema] = []
        index_names: set[str] = set()
        primary_seen = column_level_pks > 0
        for constraint in stmt.constraints:
            for name in constraint.columns:
                if name not in by_name:
                    raise ProgrammingError(
                        f"la restricción de '{stmt.table}' referencia la columna "
                        f"inexistente '{name}'",
                        sqlstate="42703",
                    )
            if len(set(constraint.columns)) != len(constraint.columns):
                raise ProgrammingError("columna repetida en una restricción de tabla")
            if constraint.kind is pm.TableConstraintKind.PRIMARY_KEY:
                if primary_seen:
                    raise ProgrammingError(
                        f"la tabla '{stmt.table}' declara más de una clave primaria",
                        sqlstate="42P16",
                    )
                primary_seen = True
                for name in constraint.columns:
                    position = by_name[name]
                    columns[position] = replace(
                        columns[position],
                        not_null=True,
                        primary_key=len(constraint.columns) == 1,
                    )
                if len(constraint.columns) == 1:
                    continue
                index_name = constraint.name or f"{stmt.table}_pkey"
            else:
                if len(constraint.columns) == 1 and constraint.name is None:
                    position = by_name[constraint.columns[0]]
                    columns[position] = replace(columns[position], unique=True)
                    continue
                index_name = constraint.name or (f"{stmt.table}_{'_'.join(constraint.columns)}_key")
            _validate_identifier(index_name, "índice")
            if index_name in self._catalog.indexes or index_name in index_names:
                raise ProgrammingError(f"el índice '{index_name}' ya existe", sqlstate="42P07")
            index_names.add(index_name)
            indexes.append(
                IndexSchema(
                    name=index_name, table=stmt.table, columns=constraint.columns, unique=True
                )
            )
        return indexes

    def _execute_create_index(
        self, ddl_txn: TransactionId, stmt: pm.CreateIndexStatement
    ) -> StatementResult:
        _validate_identifier(stmt.index_name, "índice")
        if stmt.index_name in self._catalog.indexes:
            if stmt.if_not_exists:
                return StatementResult(kind="create_index", rowcount=0)
            raise ProgrammingError(f"el índice '{stmt.index_name}' ya existe", sqlstate="42P07")
        table = self._catalog.table(stmt.table)
        if stmt.unique:
            # Un índice UNIQUE sobre datos existentes sólo es válido si esos
            # datos ya son únicos; se comprueba con el snapshot del propio
            # txn del DDL antes de registrar nada.
            rows = self._visible_table_rows(ddl_txn, stmt.table)
            seen: set[tuple[ColumnValue, ...]] = set()
            for _, row in rows:
                key = tuple(row.get(col) for col in stmt.columns)
                if None in key:
                    continue
                if key in seen:
                    raise IntegrityError(
                        f"no se puede crear el índice único '{stmt.index_name}': "
                        f"valor duplicado {key!r} en los datos existentes",
                        sqlstate="23505",
                    )
                seen.add(key)
        self._catalog.add_index(
            IndexSchema(
                name=stmt.index_name,
                table=table.name,
                columns=stmt.columns,
                unique=stmt.unique,
            )
        )
        self._persist_catalog()
        return StatementResult(kind="create_index", rowcount=0)

    def _execute_drop_table(self, stmt: pm.DropTableStatement) -> StatementResult:
        """`DROP TABLE` bajo el lock EXCLUSIVE de la tabla (ninguna
        transacción en vuelo la está usando).

        Orden deliberado para la recuperación ante fallos: primero se borran
        todas las filas en una transacción normal (WAL + commit: duradero),
        y sólo después se retira el esquema del catálogo. Un crash entre
        ambos pasos deja la tabla vacía pero existente (basta repetir el
        DROP); el orden inverso podría dejar filas huérfanas que una futura
        `CREATE TABLE` con el mismo nombre "resucitaría". Los rowids no se
        reutilizan (el contador de la tabla no se reinicia) y el GC de MVCC
        poda las versiones borradas más tarde, como con cualquier DELETE."""
        if stmt.table not in self._catalog.tables:
            if stmt.if_exists:
                return StatementResult(kind="drop_table", rowcount=0)
            raise ProgrammingError(f"la tabla '{stmt.table}' no existe", sqlstate="42P01")
        txn = self.begin_transaction(IsolationLevel.READ_COMMITTED)
        try:
            for rowid, _ in self._visible_table_rows(txn, stmt.table):
                self._mvcc.delete(txn, make_row_id(stmt.table, rowid))
                self._record_write(txn, stmt.table, rowid)
        except BaseException:
            self.rollback(txn)
            raise
        self.commit(txn)
        self._catalog.drop_table(stmt.table)
        self._live_counts.pop(stmt.table, None)
        self._persist_catalog()
        return StatementResult(kind="drop_table", rowcount=0)

    def _persist_catalog(self) -> None:
        if self._path is not None:
            save_catalog(
                self._path / CATALOG_FILENAME, self._catalog, backend_name=self._backend_name
            )

    # ------------------------------------------------------------------
    # Cierre
    # ------------------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._closed:
            raise OperationalError("la base de datos ya está cerrada")

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._durability is not None:
                self._durability.checkpoint(self._kv)
                self._durability.close()
            else:
                self._kv.flush()
            self._kv.close()
