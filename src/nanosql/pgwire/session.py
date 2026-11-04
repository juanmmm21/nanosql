"""Una sesión del protocolo v3: arranque, autenticación y bucle de mensajes.

Cada conexión TCP es un hilo con su propia `Connection` de nanosql (hermana
de la del servidor, sobre el mismo engine): transacciones independientes con
aislamiento MVCC real entre sesiones, exactamente igual que varias conexiones
del driver embebido.

Estado de transacción del protocolo (`ReadyForQuery`):

* ``I`` — sin bloque de transacción: cada sentencia se confirma sola
  (autocommit de nanosql).
* ``T`` — dentro de `BEGIN`: todo se acumula en una transacción de nanosql.
* ``E`` — un error dentro del bloque lo marcó como fallido: sólo se acepta
  `ROLLBACK` (o `COMMIT`, que hace rollback), igual que PostgreSQL.
"""

from __future__ import annotations

import logging
import secrets
import socket
import struct
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..catalog import ColumnValue
from ..driver import Connection
from ..engine import StatementResult
from ..errors import (
    NanosqlError,
    NotSupportedError,
    ProgrammingError,
)
from . import protocol as p
from .auth import MECHANISM, AuthenticationFailedError, ScramServerExchange, ScramVerifier
from .catalog import (
    PG_COMPAT_VERSION,
    CatalogEmulator,
    CatalogResult,
    SessionInfo,
    references_system_catalog,
    select_items,
)
from .sqltext import (
    PreparedText,
    Tok,
    head_words,
    meaningful,
    prepare_text,
    split_statements,
    substitute_params,
    tokenize,
)
from .types import (
    NANOSQL_TYPE_TO_OID,
    OID_TEXT,
    decode_binary_value,
    decode_text_value,
    encode_value,
    type_size,
)

if TYPE_CHECKING:
    from .server import PgServer

log = logging.getLogger("nanosql.pgwire")

_ISOLATION_NAMES = {
    "read uncommitted": "read_committed",  # PostgreSQL también la trata como RC
    "read committed": "read_committed",
    "repeatable read": "repeatable_read",
    "serializable": "serializable",
}
_WRITE_HEADS = frozenset({"INSERT", "UPDATE", "DELETE", "CREATE", "DROP"})


class _FatalError(Exception):
    """Error que termina la sesión (autenticación, protocolo)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(slots=True)
class Outcome:
    """Resultado de una sentencia, listo para el cable."""

    columns: tuple[tuple[str, int], ...] | None  # None = no devuelve filas
    rows: list[tuple[ColumnValue, ...]] = field(default_factory=list)
    tag: str = ""
    empty: bool = False
    notices: list[tuple[str, str, str]] = field(default_factory=list)  # (sev, code, msg)


@dataclass(slots=True)
class PreparedStatement:
    query: str
    param_oids: tuple[int, ...]
    text: PreparedText | None  # None para sentencias que no van al driver
    heads: list[str]
    catalog_candidate: bool


@dataclass(slots=True)
class Portal:
    statement: PreparedStatement
    params: list[ColumnValue]
    result_formats: tuple[int, ...]
    outcome: Outcome | None = None
    position: int = 0


def pg_column_name(name: str) -> str:
    """Nombres de columna con la convención de PostgreSQL: `t.id` → `id`,
    `COUNT(*)` → `count`, expresiones → `?column?`."""
    if "." in name and name.replace(".", "").replace("_", "").isalnum():
        return name.rsplit(".", 1)[1]
    if "(" in name and name.split("(", 1)[0].replace("_", "").isalnum() and name[0] != "(":
        return name.split("(", 1)[0].lower()
    if name.replace("_", "").isalnum():
        return name
    return "?column?"


def _command_tag(result: StatementResult) -> str:
    kind = result.kind
    if kind == "select":
        return f"SELECT {len(result.rows)}"
    if kind == "explain":
        return "EXPLAIN"
    if kind == "insert":
        return f"INSERT 0 {result.rowcount}"
    if kind == "update":
        return f"UPDATE {result.rowcount}"
    if kind == "delete":
        return f"DELETE {result.rowcount}"
    if kind == "create_table":
        return "CREATE TABLE"
    if kind == "create_index":
        return "CREATE INDEX"
    if kind == "drop_table":
        return "DROP TABLE"
    return kind.upper()


class Session:
    def __init__(self, sock: socket.socket, server: PgServer, pid: int) -> None:
        self._sock = sock
        self._server = server
        self.pid = pid
        self.secret = secrets.randbits(31)
        self._out: list[bytes] = []
        self._conn: Connection | None = None
        self._status = "I"
        self._txn_isolation: str | None = None
        self._txn_read_only = False
        self._txn_statements = 0
        self._txn_writes = 0
        self._savepoints: list[tuple[str, int]] = []
        self._statements: dict[str, PreparedStatement] = {}
        self._portals: dict[str, Portal] = {}
        self._skip_until_sync = False
        self._info = SessionInfo(user="", database="", pid=pid)
        self._info.transaction_isolation = self._current_isolation_name

    # ------------------------------------------------------------------
    # E/S
    # ------------------------------------------------------------------

    def _send(self, data: bytes) -> None:
        self._out.append(data)

    def _flush(self) -> None:
        if self._out:
            payload = b"".join(self._out)
            self._out = []
            self._sock.sendall(payload)

    def _send_error(self, exc: BaseException) -> None:
        if isinstance(exc, NanosqlError):
            code, text = exc.sqlstate, str(exc)
        elif isinstance(exc, p.ProtocolError):
            code, text = "08P01", str(exc)
        else:
            log.exception("error interno en la sesión %s", self.pid)
            code, text = "XX000", f"error interno: {type(exc).__name__}: {exc}"
        first, _, detail = text.partition("\n")
        self._send(p.error_response(code, first, detail=detail or None))

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    def run(self) -> None:
        try:
            if not self._startup():
                return
            self._loop()
        except (p.ConnectionClosedError, ConnectionResetError, BrokenPipeError):
            pass
        except _FatalError as exc:
            try:
                self._send(p.error_response(exc.code, str(exc), severity="FATAL"))
                self._flush()
            except OSError:
                pass
        except p.ProtocolError as exc:
            try:
                self._send(p.error_response("08P01", str(exc), severity="FATAL"))
                self._flush()
            except OSError:
                pass
        finally:
            self._close()

    def _close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()  # cierre con transacción abierta = rollback
            except NanosqlError:
                pass
            self._conn = None
        try:
            self._sock.close()
        except OSError:
            pass

    def _startup(self) -> bool:
        while True:
            code, body = p.read_startup_packet(self._sock)
            if code in (p.SSL_REQUEST_CODE, p.GSSENC_REQUEST_CODE):
                self._sock.sendall(b"N")  # sin TLS ni GSSAPI: el cliente sigue en claro
                continue
            if code == p.CANCEL_REQUEST_CODE:
                pid, secret = struct.unpack("!ii", body[:8])
                self._server.cancel(pid, secret)
                return False
            break
        major, minor = code >> 16, code & 0xFFFF
        if major != 3:
            raise _FatalError("0A000", f"protocolo {major}.{minor} no soportado")
        params = p.parse_startup_parameters(body)
        if minor > 0 or any(key.startswith("_pq_.") for key in params):
            unknown = [key for key in params if key.startswith("_pq_.")]
            payload = struct.pack("!ii", 0, len(unknown)) + b"".join(p.cstr(k) for k in unknown)
            self._send(p.message(b"v", payload))
        user = params.get("user", "")
        if not user:
            raise _FatalError("28000", "no se indicó usuario en el arranque")
        database = params.get("database") or user
        self._authenticate(user)
        self._info.user = user
        self._info.database = database
        if params.get("application_name"):
            self._info.settings["application_name"] = params["application_name"]
        self._conn = self._server.new_connection()
        self._send(p.authentication_ok())
        statuses = {
            "application_name": params.get("application_name", ""),
            "client_encoding": "UTF8",
            "DateStyle": "ISO, MDY",
            "default_transaction_read_only": "off",
            "in_hot_standby": "off",
            "integer_datetimes": "on",
            "IntervalStyle": "postgres",
            "is_superuser": "on",
            "server_encoding": "UTF8",
            "server_version": PG_COMPAT_VERSION,
            "session_authorization": user,
            "standard_conforming_strings": "on",
            "TimeZone": "UTC",
        }
        for name, value in statuses.items():
            self._send(p.parameter_status(name, value))
        self._send(p.backend_key_data(self.pid, self.secret))
        self._send(p.ready_for_query(self._status))
        self._flush()
        log.info("sesión %s: usuario=%s base=%s", self.pid, user, database)
        return True

    def _authenticate(self, user: str) -> None:
        method = self._server.auth_method
        if method == "trust":
            return
        verifier = self._server.users.get(user)
        # Usuario desconocido: el intercambio sigue con un verificador
        # aleatorio y falla al final, sin revelar qué usuarios existen.
        real = verifier is not None
        exchange = ScramServerExchange(
            verifier or ScramVerifier.from_password(secrets.token_hex(16))
        )
        self._send(p.authentication_sasl([MECHANISM]))
        self._flush()
        try:
            kind, body = p.read_message(self._sock)
            if kind != b"p":
                raise AuthenticationFailedError("se esperaba SASLInitialResponse")
            reader = p.Reader(body)
            mechanism = reader.cstring()
            if mechanism != MECHANISM:
                raise AuthenticationFailedError(f"mecanismo SASL no soportado: {mechanism}")
            size = reader.int32()
            initial = reader.raw(size) if size >= 0 else b""
            self._send(p.authentication_sasl_continue(exchange.handle_client_first(initial)))
            self._flush()
            kind, body = p.read_message(self._sock)
            if kind != b"p":
                raise AuthenticationFailedError("se esperaba SASLResponse")
            final = exchange.handle_client_final(body)
            if not real:
                raise AuthenticationFailedError("usuario desconocido")
        except (AuthenticationFailedError, UnicodeDecodeError) as exc:
            log.info("sesión %s: autenticación fallida para %s: %s", self.pid, user, exc)
            raise _FatalError(
                "28P01", f'la autenticación por contraseña falló para el usuario "{user}"'
            ) from exc
        self._send(p.authentication_sasl_final(final))

    # ------------------------------------------------------------------
    # Bucle principal
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        handlers: dict[bytes, Callable[[bytes], None]] = {
            b"Q": self._on_query,
            b"P": self._on_parse,
            b"B": self._on_bind,
            b"D": self._on_describe,
            b"E": self._on_execute,
            b"C": self._on_close,
        }
        while True:
            kind, body = p.read_message(self._sock)
            if kind == b"X":
                return
            if kind == b"S":
                self._skip_until_sync = False
                self._send(p.ready_for_query(self._status))
                self._flush()
                continue
            if kind == b"H":
                self._flush()
                continue
            if self._skip_until_sync and kind != b"Q":
                continue
            handler = handlers.get(kind)
            if handler is None:
                self._send(
                    p.error_response("0A000", f"mensaje {kind!r} no soportado (COPY no existe)")
                )
                self._skip_until_sync = kind != b"Q"
                self._flush()
                continue
            if kind == b"Q":
                handler(body)
                continue
            try:
                handler(body)
            except Exception as exc:  # noqa: BLE001 — todo error va al cliente
                self._on_statement_error(exc)
                self._send_error(exc)
                self._skip_until_sync = True

    # ------------------------------------------------------------------
    # Protocolo simple
    # ------------------------------------------------------------------

    def _on_query(self, body: bytes) -> None:
        query = p.Reader(body).cstring()
        log.debug("sesión %s Q: %s", self.pid, query)
        try:
            statements = split_statements(query)
        except NanosqlError as exc:
            self._send_error(exc)
            statements = None
        if statements is not None and not statements:
            self._send(p.empty_query_response())
        for sql in statements or []:
            try:
                prepared = self._prepare(sql, ())
                outcome = self._execute(prepared, [])
            except Exception as exc:  # noqa: BLE001
                self._on_statement_error(exc)
                self._send_error(exc)
                break
            self._emit_notices(outcome)
            if outcome.empty:
                self._send(p.empty_query_response())
                continue
            if outcome.columns is not None:
                formats = (0,) * len(outcome.columns)
                self._send(self._row_description(outcome.columns, formats))
                for row in outcome.rows:
                    self._send(self._data_row(row, outcome.columns, formats))
            self._send(p.command_complete(outcome.tag))
        self._send(p.ready_for_query(self._status))
        self._flush()

    # ------------------------------------------------------------------
    # Protocolo extendido
    # ------------------------------------------------------------------

    def _on_parse(self, body: bytes) -> None:
        message = p.decode_parse(body)
        log.debug("sesión %s P[%s]: %s", self.pid, message.statement, message.query)
        statements = split_statements(message.query)
        if len(statements) > 1:
            raise ProgrammingError(
                "no se pueden insertar varias sentencias en una sentencia preparada",
                sqlstate="42601",
            )
        query = statements[0] if statements else ""
        if message.statement and message.statement in self._statements:
            raise ProgrammingError(
                f'la sentencia preparada "{message.statement}" ya existe', sqlstate="42P05"
            )
        self._statements[message.statement] = self._prepare(query, message.param_oids)
        self._send(p.parse_complete())

    def _on_bind(self, body: bytes) -> None:
        message = p.decode_bind(body)
        statement = self._statements.get(message.statement)
        if statement is None:
            raise ProgrammingError(
                f'la sentencia preparada "{message.statement}" no existe', sqlstate="26000"
            )
        formats = p.expand_formats(message.param_formats, len(message.params))
        values: list[ColumnValue] = []
        for index, (raw, fmt) in enumerate(zip(message.params, formats, strict=True)):
            oid = self._param_oid(statement, index)
            if raw is None:
                values.append(None)
            elif fmt == 1:
                values.append(decode_binary_value(raw, oid))
            else:
                values.append(decode_text_value(raw.decode("utf-8"), oid))
        if message.portal in self._portals and message.portal:
            raise ProgrammingError(f'el portal "{message.portal}" ya existe', sqlstate="42P03")
        self._portals[message.portal] = Portal(
            statement=statement, params=values, result_formats=message.result_formats
        )
        self._send(p.bind_complete())

    def _param_oid(self, statement: PreparedStatement, index: int) -> int:
        if index < len(statement.param_oids) and statement.param_oids[index]:
            return statement.param_oids[index]
        if statement.text is not None and index in statement.text.param_casts:
            return statement.text.param_casts[index]
        return 0

    def _on_describe(self, body: bytes) -> None:
        reader = p.Reader(body)
        target = reader.byte()
        name = reader.cstring()
        if target == b"S":
            statement = self._statements.get(name)
            if statement is None:
                raise ProgrammingError(
                    f'la sentencia preparada "{name}" no existe', sqlstate="26000"
                )
            count = statement.text.param_count if statement.text is not None else 0
            count = max(count, len(statement.param_oids))
            oids = [self._param_oid(statement, i) or OID_TEXT for i in range(count)]
            self._send(p.parameter_description(oids))
            columns = self._describe_statement(statement)
            if columns is None:
                self._send(p.no_data())
            else:
                self._send(self._row_description(columns, (0,) * len(columns)))
            return
        portal = self._portals.get(name)
        if portal is None:
            raise ProgrammingError(f'el portal "{name}" no existe', sqlstate="34000")
        outcome = self._run_portal(portal)
        if outcome.columns is None:
            self._send(p.no_data())
        else:
            formats = p.expand_formats(portal.result_formats, len(outcome.columns))
            self._send(self._row_description(outcome.columns, formats))

    def _on_execute(self, body: bytes) -> None:
        reader = p.Reader(body)
        name = reader.cstring()
        max_rows = reader.int32()
        portal = self._portals.get(name)
        if portal is None:
            raise ProgrammingError(f'el portal "{name}" no existe', sqlstate="34000")
        outcome = self._run_portal(portal)
        if outcome.empty:
            self._send(p.empty_query_response())
            return
        if outcome.columns is not None:
            formats = p.expand_formats(portal.result_formats, len(outcome.columns))
            end = len(outcome.rows)
            if max_rows > 0:
                end = min(end, portal.position + max_rows)
            for row in outcome.rows[portal.position : end]:
                self._send(self._data_row(row, outcome.columns, formats))
            portal.position = end
            if end < len(outcome.rows):
                self._send(p.portal_suspended())
                return
        self._send(p.command_complete(outcome.tag))

    def _on_close(self, body: bytes) -> None:
        reader = p.Reader(body)
        target = reader.byte()
        name = reader.cstring()
        if target == b"S":
            self._statements.pop(name, None)
        else:
            self._portals.pop(name, None)
        self._send(p.close_complete())

    def _run_portal(self, portal: Portal) -> Outcome:
        if portal.outcome is None:
            portal.outcome = self._execute(portal.statement, portal.params)
            self._emit_notices(portal.outcome)
        return portal.outcome

    def _describe_statement(
        self, statement: PreparedStatement
    ) -> tuple[tuple[str, int], ...] | None:
        """RowDescription de una sentencia aún sin parámetros. Para SELECT se
        ejecuta en seco con parámetros NULL en una conexión hermana dentro de
        una transacción que se descarta: una lectura sin efectos."""
        heads = statement.heads
        if not heads or heads[0] not in ("SELECT", "SHOW", "WITH", "EXPLAIN"):
            return None
        count = statement.text.param_count if statement.text is not None else 0
        nulls: list[ColumnValue] = [None] * count
        if heads[0] == "SHOW" or statement.catalog_candidate:
            outcome = self._execute(statement, nulls, dry_run=True)
            return outcome.columns
        assert self._conn is not None
        probe = self._conn.sibling(autocommit=False)
        try:
            assert statement.text is not None
            result = probe._execute_sql(statement.text.sql, statement.text.bind(nulls))
            return self._result_columns(result)
        finally:
            probe.rollback()
            probe.close()

    # ------------------------------------------------------------------
    # Ejecución de sentencias
    # ------------------------------------------------------------------

    def _prepare(self, sql: str, param_oids: tuple[int, ...]) -> PreparedStatement:
        tokens = meaningful(tokenize(sql))
        heads = head_words(sql, 4)
        catalog_candidate = (
            bool(tokens)
            and tokens[0].upper in ("SELECT", "WITH")
            and (references_system_catalog(tokens) or not select_items(tokens)[1])
        )
        text: PreparedText | None = None
        if heads and heads[0] not in (
            "BEGIN",
            "START",
            "COMMIT",
            "END",
            "ROLLBACK",
            "ABORT",
            "SET",
            "RESET",
            "SHOW",
            "DISCARD",
            "DEALLOCATE",
        ):
            text = prepare_text(sql)
        return PreparedStatement(
            query=sql,
            param_oids=param_oids,
            text=text,
            heads=heads,
            catalog_candidate=catalog_candidate,
        )

    def _execute(
        self, statement: PreparedStatement, params: list[ColumnValue], *, dry_run: bool = False
    ) -> Outcome:
        heads = statement.heads
        if not heads and not meaningful(tokenize(statement.query)):
            return Outcome(columns=None, empty=True)
        head = heads[0] if heads else ""
        if self._status == "E" and head not in ("ROLLBACK", "ABORT", "COMMIT", "END"):
            raise ProgrammingError(
                "la transacción actual está abortada; se ignoran las órdenes hasta el "
                "final del bloque de transacción",
                sqlstate="25P02",
            )
        if head in ("BEGIN", "START"):
            return self._begin(statement.query)
        if head in ("COMMIT", "END"):
            return self._commit(statement.query)
        if head in ("ROLLBACK", "ABORT"):
            return self._rollback(statement.query)
        if head == "SET":
            return self._set(statement.query)
        if head == "RESET":
            return self._reset(statement.query)
        if head == "SHOW":
            return self._show(statement.query)
        if head == "DISCARD":
            self._statements = {k: v for k, v in self._statements.items() if k == ""}
            self._portals.clear()
            self._info.settings.clear()
            return Outcome(columns=None, tag="DISCARD ALL")
        if head == "DEALLOCATE":
            return self._deallocate(statement.query)
        if head == "SAVEPOINT":
            return self._savepoint(statement.query)
        if head == "RELEASE":
            return self._release(statement.query)
        if statement.catalog_candidate:
            text = substitute_params(statement.query, params)
            emulator = CatalogEmulator(self._server.catalog_snapshot(), self._info)
            emulated = emulator.handle(text)
            if emulated is not None:
                return self._from_catalog(emulated)
        if dry_run:
            return Outcome(columns=None)
        return self._execute_sql(statement, params)

    def _execute_sql(self, statement: PreparedStatement, params: list[ColumnValue]) -> Outcome:
        assert self._conn is not None and statement.text is not None
        heads = statement.heads
        if self._txn_read_only and heads and heads[0] in _WRITE_HEADS:
            raise ProgrammingError(
                f"no se puede ejecutar {heads[0]} en una transacción de sólo lectura",
                sqlstate="25006",
            )
        bound = statement.text.bind(params)
        result = self._conn._execute_sql(statement.text.sql, bound)
        outcome = Outcome(columns=self._result_columns(result), tag=_command_tag(result))
        if outcome.columns is not None:
            outcome.rows = result.rows
        self._txn_statements += 1
        if result.kind in ("insert", "update", "delete") and result.rowcount > 0:
            self._txn_writes += 1
        if self._status == "T" and not self._conn.in_transaction:
            # El DDL de nanosql confirma la transacción abierta (autocommit
            # de DDL estilo MySQL). Se reabre otra para que el resto del
            # bloque siga siendo atómico, y se avisa al cliente.
            self._conn.begin(self._txn_isolation)
            self._savepoints.clear()  # el trabajo anterior ya está confirmado
            outcome.notices.append(
                (
                    "NOTICE",
                    "01000",
                    "el DDL de nanosql confirmó el trabajo previo del bloque de transacción; "
                    "el bloque continúa en una transacción nueva",
                )
            )
        return outcome

    @staticmethod
    def _result_columns(result: StatementResult) -> tuple[tuple[str, int], ...] | None:
        if result.kind not in ("select", "explain"):
            return None
        return tuple(
            (pg_column_name(name), NANOSQL_TYPE_TO_OID.get(type_name, OID_TEXT))
            for name, type_name in zip(result.columns, result.column_types, strict=True)
        )

    @staticmethod
    def _from_catalog(result: CatalogResult) -> Outcome:
        return Outcome(columns=result.columns, rows=result.rows, tag=result.tag)

    def _on_statement_error(self, exc: BaseException) -> None:
        if self._conn is None:
            return
        if self._status == "T":
            self._status = "E"
        elif self._status == "I" and self._conn.in_transaction:
            # Sentencia autocommit fallida: su transacción implícita se
            # descarta entera (nunca se confirma con la siguiente sentencia).
            try:
                self._conn.rollback()
            except NanosqlError:
                pass

    def _emit_notices(self, outcome: Outcome) -> None:
        for severity, code, text in outcome.notices:
            self._send(p.notice_response(code, text, severity=severity))

    # ------------------------------------------------------------------
    # Control de transacciones
    # ------------------------------------------------------------------

    def _current_isolation_name(self) -> str:
        if self._status in ("T", "E") and self._txn_isolation is not None:
            return self._txn_isolation.replace("_", " ")
        default = self._info.settings.get("default_transaction_isolation")
        return default or self._server.default_isolation.replace("_", " ")

    def _parse_txn_modes(self, words: list[str]) -> tuple[str | None, bool | None]:
        isolation: str | None = None
        read_only: bool | None = None
        i = 0
        while i < len(words):
            word = words[i]
            if word == "ISOLATION" and i + 1 < len(words) and words[i + 1] == "LEVEL":
                rest = " ".join(words[i + 2 : i + 4]).lower()
                for name, level in _ISOLATION_NAMES.items():
                    if rest.startswith(name):
                        isolation = level
                        i += 2 + len(name.split())
                        break
                else:
                    raise ProgrammingError(
                        f"nivel de aislamiento desconocido: {rest}", sqlstate="42601"
                    )
                continue
            if word == "READ" and i + 1 < len(words) and words[i + 1] in ("ONLY", "WRITE"):
                read_only = words[i + 1] == "ONLY"
                i += 2
                continue
            if word in ("NOT", "DEFERRABLE", "WORK", "TRANSACTION", ","):
                i += 1
                continue
            raise ProgrammingError(f"opción de transacción desconocida: {word}", sqlstate="42601")
        return isolation, read_only

    @staticmethod
    def _words(sql: str) -> list[str]:
        return [
            t.upper if t.kind == "ident" else t.text
            for t in meaningful(tokenize(sql))
            if t.text != ";"
        ]

    def _begin(self, sql: str) -> Outcome:
        words = self._words(sql)
        body = words[2:] if words[:2] == ["START", "TRANSACTION"] else words[1:]
        isolation, read_only = self._parse_txn_modes(body)
        if self._status != "I":
            return Outcome(
                columns=None,
                tag="BEGIN",
                notices=[("WARNING", "25001", "ya hay una transacción en curso")],
            )
        self._start_block(isolation, bool(read_only))
        return Outcome(columns=None, tag="BEGIN")

    def _start_block(self, isolation: str | None, read_only: bool) -> None:
        assert self._conn is not None
        if self._conn.in_transaction:
            self._conn.rollback()
        level = isolation or _ISOLATION_NAMES.get(self._current_isolation_name(), None)
        self._conn.begin(level)
        self._txn_isolation = level
        self._txn_read_only = read_only
        self._txn_statements = 0
        self._txn_writes = 0
        self._savepoints.clear()
        self._status = "T"

    def _end_block(self) -> None:
        self._status = "I"
        self._txn_isolation = None
        self._txn_read_only = False
        self._savepoints.clear()

    # Savepoints: nanosql no tiene transacciones anidadas. Un savepoint se
    # registra como marca (nº de escrituras de la transacción en ese
    # momento) y ROLLBACK TO sólo se acepta si desde la marca no se escribió
    # nada — entonces deshacer hasta ella es exactamente no hacer nada (y
    # recuperar el bloque tras un error de lectura). Si hubo escrituras, se
    # rechaza con 0A000: nunca un "rollback parcial" que no lo es. Es lo que
    # necesitan psycopg/SQLAlchemy, que envuelven sus consultas de
    # introspección en `conn.transaction()` (SAVEPOINT) dentro de un bloque.

    def _savepoint_name(self, sql: str) -> str:
        tokens = meaningful(tokenize(sql))
        last = tokens[-1] if tokens else None
        if last is None or last.kind not in ("ident", "qident") or len(tokens) < 2:
            raise ProgrammingError("falta el nombre del savepoint", sqlstate="42601")
        return last.value if last.kind == "qident" else last.text.lower()

    def _savepoint(self, sql: str) -> Outcome:
        if self._status == "I":
            raise ProgrammingError(
                "SAVEPOINT sólo puede usarse dentro de un bloque de transacción",
                sqlstate="25P01",
            )
        self._savepoints.append((self._savepoint_name(sql), self._txn_writes))
        return Outcome(columns=None, tag="SAVEPOINT")

    def _find_savepoint(self, name: str) -> int:
        for index in range(len(self._savepoints) - 1, -1, -1):
            if self._savepoints[index][0] == name:
                return index
        raise ProgrammingError(f'el savepoint "{name}" no existe', sqlstate="3B001")

    def _release(self, sql: str) -> Outcome:
        if self._status == "I":
            raise ProgrammingError(
                "RELEASE SAVEPOINT sólo puede usarse dentro de un bloque de transacción",
                sqlstate="25P01",
            )
        index = self._find_savepoint(self._savepoint_name(sql))
        del self._savepoints[index:]
        return Outcome(columns=None, tag="RELEASE")

    def _rollback_to(self, sql: str) -> Outcome:
        assert self._conn is not None
        if self._status == "I":
            raise ProgrammingError(
                "ROLLBACK TO SAVEPOINT sólo puede usarse dentro de un bloque de transacción",
                sqlstate="25P01",
            )
        index = self._find_savepoint(self._savepoint_name(sql))
        name, writes_at_mark = self._savepoints[index]
        if self._txn_writes != writes_at_mark:
            raise NotSupportedError(
                f'ROLLBACK TO SAVEPOINT "{name}" no soportado: la transacción escribió '
                "después del savepoint y nanosql no tiene transacciones anidadas",
                sqlstate="0A000",
            )
        if not self._conn.in_transaction:
            raise ProgrammingError(
                "la transacción fue abortada por un conflicto y no puede recuperarse",
                sqlstate="25P02",
            )
        del self._savepoints[index + 1 :]
        self._status = "T"
        return Outcome(columns=None, tag="ROLLBACK")

    def _chain(self, words: list[str]) -> bool:
        return "CHAIN" in words and "NO" not in words

    def _commit(self, sql: str) -> Outcome:
        assert self._conn is not None
        words = self._words(sql)
        if self._status == "I":
            return Outcome(
                columns=None,
                tag="COMMIT",
                notices=[("WARNING", "25P01", "no hay ninguna transacción en curso")],
            )
        isolation, read_only = self._txn_isolation, self._txn_read_only
        if self._status == "E":
            self._conn.rollback()
            self._end_block()
            return Outcome(columns=None, tag="ROLLBACK")
        try:
            self._conn.commit()
        finally:
            self._end_block()
        if self._chain(words):
            self._start_block(isolation, read_only)
        return Outcome(columns=None, tag="COMMIT")

    def _rollback(self, sql: str) -> Outcome:
        assert self._conn is not None
        words = self._words(sql)
        if "TO" in words:
            return self._rollback_to(sql)
        if self._status == "I":
            return Outcome(
                columns=None,
                tag="ROLLBACK",
                notices=[("WARNING", "25P01", "no hay ninguna transacción en curso")],
            )
        isolation, read_only = self._txn_isolation, self._txn_read_only
        self._conn.rollback()
        self._end_block()
        if self._chain(words):
            self._start_block(isolation, read_only)
        return Outcome(columns=None, tag="ROLLBACK")

    # ------------------------------------------------------------------
    # SET / RESET / SHOW / DEALLOCATE
    # ------------------------------------------------------------------

    def _set(self, sql: str) -> Outcome:
        tokens = meaningful(tokenize(sql))
        words = self._words(sql)
        rest = words[1:]
        if rest and rest[0] in ("SESSION", "LOCAL") and rest[1:2] != ["CHARACTERISTICS"]:
            rest = rest[1:]
        if rest[:4] == ["SESSION", "CHARACTERISTICS", "AS", "TRANSACTION"]:
            isolation, _ = self._parse_txn_modes(rest[4:])
            if isolation is not None:
                self._info.settings["default_transaction_isolation"] = isolation.replace("_", " ")
            return Outcome(columns=None, tag="SET")
        if rest[:1] == ["TRANSACTION"]:
            isolation, read_only = self._parse_txn_modes(rest[1:])
            if self._status == "I":
                return Outcome(
                    columns=None,
                    tag="SET",
                    notices=[
                        (
                            "WARNING",
                            "25P01",
                            "SET TRANSACTION sólo tiene efecto dentro de un bloque de transacción",
                        )
                    ],
                )
            if isolation is not None and isolation != self._txn_isolation:
                if self._txn_statements:
                    raise ProgrammingError(
                        "SET TRANSACTION ISOLATION LEVEL debe ejecutarse antes de cualquier "
                        "consulta",
                        sqlstate="25001",
                    )
                self._start_block(isolation, self._txn_read_only)
            if read_only is not None:
                self._txn_read_only = read_only
            return Outcome(columns=None, tag="SET")
        if rest[:2] == ["TIME", "ZONE"]:
            name, value_tokens = "timezone", tokens[3:]
        else:
            if not tokens[1:]:
                raise ProgrammingError("SET sin parámetro", sqlstate="42601")
            start = 1
            if tokens[1].upper in ("SESSION", "LOCAL"):
                start = 2
            name = tokens[start].text.lower()
            value_tokens = tokens[start + 1 :]
            if (
                value_tokens
                and value_tokens[0].text in ("=",)
                or (value_tokens and value_tokens[0].upper == "TO")
            ):
                value_tokens = value_tokens[1:]
        value = ", ".join(
            "".join(t.value if t.kind in ("string", "qident") else t.text for t in part)
            for part in _split_commas(value_tokens)
        )
        if value.upper() == "DEFAULT":
            self._info.settings.pop(name, None)
        else:
            if name in ("transaction_isolation", "default_transaction_isolation"):
                if value.lower() not in _ISOLATION_NAMES:
                    raise ProgrammingError(
                        f'valor inválido para el parámetro "{name}": "{value}"', sqlstate="22023"
                    )
                if name == "transaction_isolation":
                    return self._set(f"SET TRANSACTION ISOLATION LEVEL {value}")
            self._info.settings[name] = value
        return Outcome(columns=None, tag="SET")

    def _reset(self, sql: str) -> Outcome:
        words = self._words(sql)
        if words[1:2] == ["ALL"]:
            self._info.settings.clear()
        elif len(words) > 1:
            self._info.settings.pop(words[1].lower(), None)
        return Outcome(columns=None, tag="RESET")

    def _show(self, sql: str) -> Outcome:
        words = self._words(sql)
        emulator = CatalogEmulator(self._server.catalog_snapshot(), self._info)
        if words[1:2] == ["ALL"]:
            return self._from_catalog(emulator.show_all())
        name = " ".join(words[1:]).lower()
        tokens = meaningful(tokenize(sql))
        display = tokens[1].text if len(tokens) == 2 else name
        value = emulator.show_value(name)
        column = "transaction_isolation" if name == "transaction isolation level" else display
        return Outcome(columns=((column, type_text()),), rows=[(value,)], tag="SHOW")

    def _deallocate(self, sql: str) -> Outcome:
        words = self._words(sql)
        target = words[-1] if len(words) > 1 else ""
        if target == "ALL":
            self._statements = {k: v for k, v in self._statements.items() if k == ""}
        else:
            tokens = meaningful(tokenize(sql))
            self._statements.pop(tokens[-1].value or tokens[-1].text, None)
        return Outcome(columns=None, tag="DEALLOCATE")

    # ------------------------------------------------------------------
    # Codificación de filas
    # ------------------------------------------------------------------

    @staticmethod
    def _row_description(columns: tuple[tuple[str, int], ...], formats: tuple[int, ...]) -> bytes:
        return p.row_description(
            [
                p.FieldDescription(
                    name=name, type_oid=oid, type_size=type_size(oid), format_code=fmt
                )
                for (name, oid), fmt in zip(columns, formats, strict=True)
            ]
        )

    @staticmethod
    def _data_row(
        row: tuple[ColumnValue, ...],
        columns: tuple[tuple[str, int], ...],
        formats: tuple[int, ...],
    ) -> bytes:
        return p.data_row(
            [
                encode_value(value, oid, fmt)
                for value, (_, oid), fmt in zip(row, columns, formats, strict=True)
            ]
        )


def type_text() -> int:
    return OID_TEXT


def _split_commas(tokens: list[Tok]) -> list[list[Tok]]:
    parts: list[list[Tok]] = [[]]
    for tok in tokens:
        if tok.text == ",":
            parts.append([])
        else:
            parts[-1].append(tok)
    return [part for part in parts if part]
