"""Driver Python con la forma de DB-API 2.0 (simplificada y documentada).

`connect()` devuelve una `Connection`; sus cursores ejecutan SQL contra el
`DatabaseEngine` compartido. Varias conexiones del mismo proceso al mismo
path comparten un único engine (registro con refcount): es lo que permite
transacciones concurrentes reales — cada conexión con la suya — sobre los
mismos datos sin corromper el buffer pool. Conexiones desde *procesos*
distintos al mismo directorio no están soportadas (no hay file locking) y
se documenta como limitación.

Control de transacciones: además de `Connection.commit()/rollback()`, el
driver intercepta las sentencias `BEGIN`/`COMMIT`/`ROLLBACK` antes del
parser (no forman parte de la gramática de `sql-query-parser`; gestionarlas
en el cliente es un patrón normal de driver — psycopg hace lo mismo). El
mecanismo transaccional subyacente es siempre `mvcc-transaction-manager`.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable, Sequence
from pathlib import Path
from types import TracebackType

import sql_query_parser.models as pm
from mvcc_transaction_manager import IsolationLevel, TransactionId
from sql_query_parser import (
    SqlParserError,
    SqlSyntaxError,
    UnsupportedFeatureError,
    parse,
    parse_many,
    tokenize,
)
from sql_query_parser.protocols import CaretErrorFormatter

from .catalog import ColumnValue
from .engine import (
    DEFAULT_CHECKPOINT_INTERVAL,
    DEFAULT_GC_INTERVAL,
    MEMORY_PATH,
    DatabaseEngine,
    StatementResult,
    parse_isolation_level,
)
from .errors import (
    InterfaceError,
    NotSupportedError,
    ProgrammingError,
)
from .params import bind_statement

apilevel = "2.0"
# Estilo de parámetros DB-API: signos de interrogación posicionales.
paramstyle = "qmark"
# Nivel 1 de DB-API: se pueden compartir el módulo y (vía el registro de
# engines) la base de datos entre hilos, pero no una misma Connection.
threadsafety = 1

_TXN_CONTROL_SUFFIXES = frozenset({"", "TRANSACTION", "WORK"})
_error_formatter = CaretErrorFormatter()


def _format_syntax_error(exc: SqlSyntaxError, sql: str) -> str:
    return _error_formatter.format(exc, sql)


def classify_transaction_control(sql: str) -> str | None:
    """Devuelve "begin"/"commit"/"rollback" si `sql` es una sentencia de
    control de transacciones, `None` si debe ir al parser. Reutiliza el
    lexer real del ecosistema en vez de expresiones regulares: comentarios,
    mayúsculas y espaciado quedan resueltos por el mismo código que el
    resto del SQL."""
    try:
        tokens = tokenize(sql)
    except SqlParserError:
        return None  # que el error lo produzca (bien formateado) el parser
    meaningful = [t for t in tokens if t.type not in (pm.TokenType.EOF, pm.TokenType.SEMICOLON)]
    if not meaningful or meaningful[0].type is not pm.TokenType.IDENTIFIER:
        return None
    head = meaningful[0].lexeme.upper()
    if head not in ("BEGIN", "COMMIT", "ROLLBACK"):
        return None
    if len(meaningful) > 2:
        raise ProgrammingError(f"sentencia {head} malformada: {sql.strip()!r}")
    if len(meaningful) == 2:
        tail = meaningful[1]
        if (
            tail.type is not pm.TokenType.IDENTIFIER
            or tail.lexeme.upper() not in _TXN_CONTROL_SUFFIXES
        ):
            raise ProgrammingError(f"sentencia {head} malformada: {sql.strip()!r}")
    return head.lower()


def strip_explain_prefix(sql: str) -> str | None:
    """Si `sql` empieza por `EXPLAIN`, devuelve el resto de la sentencia;
    si no, `None`. Igual que el control de transacciones, EXPLAIN es una
    orden del driver (no gramática del parser), detectada con el lexer real
    para que comentarios y mayúsculas no importen."""
    try:
        tokens = tokenize(sql)
    except SqlParserError:
        return None
    meaningful = [t for t in tokens if t.type is not pm.TokenType.EOF]
    if (
        len(meaningful) < 2
        or meaningful[0].type is not pm.TokenType.IDENTIFIER
        or meaningful[0].lexeme.upper() != "EXPLAIN"
    ):
        return None
    # Offset absoluto del segundo token para rebanar el texto original.
    second = meaningful[1].position
    line_offsets = [0]
    for line in sql.splitlines(keepends=True):
        line_offsets.append(line_offsets[-1] + len(line))
    cut = line_offsets[second.line - 1] + second.column - 1
    return sql[cut:]


# ---------------------------------------------------------------------------
# Registro de engines compartidos por proceso
# ---------------------------------------------------------------------------

_registry_lock = threading.Lock()
_registry: dict[str, tuple[DatabaseEngine, int]] = {}


def _acquire_engine(
    database: str | Path,
    *,
    backend: str | None,
    lock_timeout: float,
    gc_interval: int,
    checkpoint_interval: int,
) -> tuple[DatabaseEngine, str | None]:
    """Devuelve `(engine, clave_de_registro)`. Las bases `:memory:` nunca se
    comparten (cada una es un universo privado de la conexión): clave None."""
    if str(database) == MEMORY_PATH:
        engine = DatabaseEngine.open(
            database,
            backend=backend,
            lock_timeout=lock_timeout,
            gc_interval=gc_interval,
            checkpoint_interval=checkpoint_interval,
        )
        return engine, None
    key = str(Path(database).expanduser().resolve())
    with _registry_lock:
        entry = _registry.get(key)
        if entry is not None:
            engine, refcount = entry
            if backend is not None and backend != engine.backend_name:
                raise ProgrammingError(
                    f"la base de datos ya está abierta en este proceso con el backend "
                    f"'{engine.backend_name}'; no puede reabrirse como '{backend}'"
                )
            _registry[key] = (engine, refcount + 1)
            return engine, key
        engine = DatabaseEngine.open(
            database,
            backend=backend,
            lock_timeout=lock_timeout,
            gc_interval=gc_interval,
            checkpoint_interval=checkpoint_interval,
        )
        _registry[key] = (engine, 1)
        return engine, key


def _release_engine(key: str | None, engine: DatabaseEngine) -> None:
    if key is None:
        engine.close()
        return
    with _registry_lock:
        entry = _registry.get(key)
        if entry is None:
            return
        current, refcount = entry
        if refcount <= 1:
            del _registry[key]
            current.close()
        else:
            _registry[key] = (current, refcount - 1)


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------


class Cursor:
    """Cursor DB-API simplificado. No es thread-safe (como su conexión)."""

    def __init__(self, connection: Connection) -> None:
        self._connection: Connection | None = connection
        self._result: StatementResult | None = None
        self._fetch_position = 0
        self.arraysize = 1

    # -- estado ----------------------------------------------------------

    def _require_connection(self) -> Connection:
        if self._connection is None:
            raise InterfaceError("el cursor está cerrado")
        return self._connection

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, None, None, None]] | None:
        """Sólo para SELECT: 7-tuplas `(nombre, tipo, ...)` al estilo DB-API,
        con el tipo como nombre del `ColumnType` del ejecutor."""
        if self._result is None or self._result.kind not in ("select", "explain"):
            return None
        return [
            (name, type_name, None, None, None, None, None)
            for name, type_name in zip(self._result.columns, self._result.column_types, strict=True)
        ]

    @property
    def rowcount(self) -> int:
        return -1 if self._result is None else self._result.rowcount

    # -- ejecución ---------------------------------------------------------

    def execute(self, sql: str, params: Sequence[ColumnValue] | None = None) -> Cursor:
        connection = self._require_connection()
        self._result = connection._execute_sql(sql, params)
        self._fetch_position = 0
        return self

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[ColumnValue]]) -> Cursor:
        """Ejecuta `sql` una vez por cada tupla de parámetros. `rowcount`
        agrega los de todas las ejecuciones (comportamiento DB-API)."""
        connection = self._require_connection()
        total = 0
        last: StatementResult | None = None
        for params in seq_of_params:
            last = connection._execute_sql(sql, params)
            if last.rowcount > 0:
                total += last.rowcount
        if last is None:
            raise ProgrammingError("executemany requiere al menos una tupla de parámetros")
        self._result = StatementResult(
            kind=last.kind,
            columns=last.columns,
            column_types=last.column_types,
            rows=last.rows,
            rowcount=total,
        )
        self._fetch_position = 0
        return self

    def executescript(self, script: str) -> Cursor:
        """Ejecuta varias sentencias separadas por `;` (vía `parse_many` del
        parser). Devuelve el resultado de la última."""
        connection = self._require_connection()
        for result in connection._execute_script(script):
            self._result = result
        self._fetch_position = 0
        return self

    # -- lectura de resultados ---------------------------------------------

    def _select_rows(self) -> list[tuple[ColumnValue, ...]]:
        if self._result is None or self._result.kind not in ("select", "explain"):
            raise InterfaceError("no hay un resultado de SELECT que leer")
        return self._result.rows

    def fetchone(self) -> tuple[ColumnValue, ...] | None:
        rows = self._select_rows()
        if self._fetch_position >= len(rows):
            return None
        row = rows[self._fetch_position]
        self._fetch_position += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[ColumnValue, ...]]:
        rows = self._select_rows()
        count = self.arraysize if size is None else size
        chunk = rows[self._fetch_position : self._fetch_position + count]
        self._fetch_position += len(chunk)
        return chunk

    def fetchall(self) -> list[tuple[ColumnValue, ...]]:
        rows = self._select_rows()
        chunk = rows[self._fetch_position :]
        self._fetch_position = len(rows)
        return chunk

    def __iter__(self) -> Cursor:
        return self

    def __next__(self) -> tuple[ColumnValue, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self) -> None:
        self._connection = None
        self._result = None


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


class Connection:
    """Una conexión a una base de datos nanosql.

    Semántica transaccional (documentada, estilo sqlite3/DB-API):

    * Sin `autocommit`, la primera sentencia abre una transacción implícita
      que dura hasta `commit()`/`rollback()` (o sus formas SQL).
    * Con `autocommit=True`, cada sentencia se confirma sola, salvo dentro
      de un bloque `BEGIN ... COMMIT/ROLLBACK` explícito.
    * El DDL (`CREATE TABLE`/`CREATE INDEX`) confirma implícitamente la
      transacción abierta antes de ejecutarse (como MySQL) — es lo que le
      permite tomar el lock EXCLUSIVE de tabla sin auto-bloquearse.
    """

    def __init__(
        self,
        engine: DatabaseEngine,
        registry_key: str | None,
        isolation: IsolationLevel,
        autocommit: bool,
        *,
        owns_engine: bool = True,
    ) -> None:
        self._engine: DatabaseEngine | None = engine
        self._registry_key = registry_key
        self._isolation = isolation
        self._autocommit = autocommit
        self._txn: TransactionId | None = None
        self._explicit_txn = False
        self._owns_engine = owns_engine

    # -- estado ------------------------------------------------------------

    def _require_engine(self) -> DatabaseEngine:
        if self._engine is None:
            raise InterfaceError("la conexión está cerrada")
        return self._engine

    @property
    def in_transaction(self) -> bool:
        return self._txn is not None

    @property
    def isolation_level(self) -> str:
        return self._isolation.name.lower()

    @property
    def backend(self) -> str:
        return self._require_engine().backend_name

    # -- API pública -------------------------------------------------------

    def cursor(self) -> Cursor:
        self._require_engine()
        return Cursor(self)

    def sibling(
        self, *, isolation_level: str | None = None, autocommit: bool = False
    ) -> Connection:
        """Otra conexión sobre el MISMO engine, con su propia transacción.

        Es lo que usa el servidor de protocolo Postgres para dar a cada
        sesión de red su conexión: funciona también para `:memory:` (que
        `connect()` nunca comparte). La conexión hermana no es dueña del
        engine — cerrarla no lo cierra; lo cierra la conexión original.
        """
        engine = self._require_engine()
        isolation = parse_isolation_level(isolation_level) if isolation_level else self._isolation
        return Connection(engine, None, isolation, autocommit, owns_engine=False)

    def begin(self, isolation_level: str | None = None) -> None:
        """`BEGIN` explícito con un nivel de aislamiento opcional para ESTA
        transacción (el de la conexión no cambia)."""
        engine = self._require_engine()
        if self._txn is not None:
            raise ProgrammingError("ya hay una transacción activa", sqlstate="25001")
        isolation = parse_isolation_level(isolation_level) if isolation_level else self._isolation
        self._txn = engine.begin_transaction(isolation)
        self._explicit_txn = True

    def execute(self, sql: str, params: Sequence[ColumnValue] | None = None) -> Cursor:
        """Atajo DB-API: crea un cursor y ejecuta `sql` en él."""
        return self.cursor().execute(sql, params)

    def commit(self) -> None:
        engine = self._require_engine()
        if self._txn is None:
            return  # commit sin transacción abierta es un no-op, como en sqlite3
        txn, self._txn, self._explicit_txn = self._txn, None, False
        engine.commit(txn)

    def rollback(self) -> None:
        engine = self._require_engine()
        if self._txn is None:
            return
        txn, self._txn, self._explicit_txn = self._txn, None, False
        engine.rollback(txn)

    def close(self) -> None:
        if self._engine is None:
            return
        engine = self._engine
        try:
            if self._txn is not None:
                # Cierre con transacción abierta = rollback, nunca commit
                # silencioso de trabajo a medias.
                engine.rollback(self._txn)
                self._txn = None
        finally:
            self._engine = None
            if self._owns_engine:
                _release_engine(self._registry_key, engine)

    def __enter__(self) -> Connection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            if self._engine is not None:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            self.close()

    # -- ejecución interna ---------------------------------------------------

    def _ensure_txn(self, engine: DatabaseEngine) -> TransactionId:
        if self._txn is None:
            self._txn = engine.begin_transaction(self._isolation)
        return self._txn

    def _execute_sql(
        self, sql: str, params: Sequence[ColumnValue] | None = None
    ) -> StatementResult:
        engine = self._require_engine()
        control = classify_transaction_control(sql)
        if control is not None:
            if params:
                raise ProgrammingError(f"{control.upper()} no admite parámetros")
            return self._execute_transaction_control(engine, control)
        explained = strip_explain_prefix(sql)
        to_parse = explained if explained is not None else sql
        try:
            statement = parse(to_parse)
        except UnsupportedFeatureError as exc:
            raise NotSupportedError(_format_syntax_error(exc, to_parse)) from exc
        except SqlSyntaxError as exc:
            raise ProgrammingError(_format_syntax_error(exc, to_parse), sqlstate="42601") from exc
        except SqlParserError as exc:
            raise ProgrammingError(str(exc)) from exc
        statement = bind_statement(statement, params)
        if explained is not None:
            if not isinstance(statement, pm.SelectStatement):
                raise NotSupportedError("EXPLAIN solo soporta sentencias SELECT")
            return engine.explain_select(statement)
        return self._execute_parsed(engine, statement)

    def _execute_script(self, script: str) -> list[StatementResult]:
        engine = self._require_engine()
        try:
            statements = parse_many(script)
        except UnsupportedFeatureError as exc:
            raise NotSupportedError(_format_syntax_error(exc, script)) from exc
        except SqlSyntaxError as exc:
            raise ProgrammingError(_format_syntax_error(exc, script), sqlstate="42601") from exc
        except SqlParserError as exc:
            raise ProgrammingError(str(exc)) from exc
        # Nota: parse_many no puede contener BEGIN/COMMIT (no son gramática
        # del parser); un script transaccional se gestiona con la conexión.
        return [self._execute_parsed(engine, statement) for statement in statements]

    def _execute_transaction_control(self, engine: DatabaseEngine, control: str) -> StatementResult:
        if control == "begin":
            if self._txn is not None:
                raise ProgrammingError(
                    "ya hay una transacción activa (BEGIN dentro de BEGIN, o tras "
                    "una transacción implícita abierta por una sentencia anterior)"
                )
            self._txn = engine.begin_transaction(self._isolation)
            self._explicit_txn = True
            return StatementResult(kind="begin", rowcount=0)
        if control == "commit":
            if self._txn is None:
                raise ProgrammingError("COMMIT sin transacción activa")
            self.commit()
            return StatementResult(kind="commit", rowcount=0)
        if self._txn is None:
            raise ProgrammingError("ROLLBACK sin transacción activa")
        self.rollback()
        return StatementResult(kind="rollback", rowcount=0)

    def _execute_parsed(self, engine: DatabaseEngine, statement: pm.Statement) -> StatementResult:
        if isinstance(statement, pm.CreateTableStatement | pm.CreateIndexStatement):
            # DDL: confirma la transacción abierta (autocommit de DDL) y
            # ejecuta fuera de transacción de usuario.
            self.commit()
            return engine.execute_ddl(statement)

        txn = self._ensure_txn(engine)
        try:
            result = engine.execute_statement(txn, statement)
        except Exception:
            # Un TransactionConflictError implica que el engine ya abortó la
            # transacción; el resto de errores la dejan viva (el usuario
            # decide si hace rollback), pero hay que resincronizar el estado
            # local si el engine la mató.
            self._sync_txn_liveness(engine)
            raise
        if self._autocommit and not self._explicit_txn:
            self.commit()
        return result

    def _sync_txn_liveness(self, engine: DatabaseEngine) -> None:
        if self._txn is not None and not engine.transaction_active(self._txn):
            self._txn = None
            self._explicit_txn = False


def connect(
    database: str | Path,
    *,
    backend: str | None = None,
    isolation_level: str = "repeatable_read",
    autocommit: bool = False,
    lock_timeout: float = 10.0,
    gc_interval: int = DEFAULT_GC_INTERVAL,
    checkpoint_interval: int = DEFAULT_CHECKPOINT_INTERVAL,
) -> Connection:
    """Abre (o crea) una base de datos nanosql.

    `database` es un directorio (se crea si no existe) o `":memory:"`.
    `backend` es `"btree"` (por defecto) o `"lsm"`; al reabrir una base
    existente puede omitirse — queda registrado en su catálogo.
    """
    isolation = parse_isolation_level(isolation_level)
    engine, key = _acquire_engine(
        database,
        backend=backend,
        lock_timeout=lock_timeout,
        gc_interval=gc_interval,
        checkpoint_interval=checkpoint_interval,
    )
    return Connection(engine, key, isolation, autocommit)
