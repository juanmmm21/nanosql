"""Servidor TCP del protocolo de PostgreSQL sobre una base de datos nanosql.

Un hilo por conexión (`socketserver.ThreadingTCPServer`): el engine de
nanosql ya serializa la ejecución de sentencias con su lock interno y las
sesiones sólo comparten el engine, nunca una `Connection`. Las esperas por
locks de tabla (DDL concurrente) bloquean sólo el hilo de esa sesión.
"""

from __future__ import annotations

import itertools
import logging
import os
import socket
import socketserver
import threading
from collections.abc import Mapping
from pathlib import Path

from ..catalog import Catalog
from ..driver import Connection, connect
from .auth import ScramVerifier
from .session import Session

log = logging.getLogger("nanosql.pgwire")

AUTH_METHODS = ("trust", "scram-sha-256")


class _TcpServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: tuple[str, int], owner: PgServer) -> None:
        self.owner = owner
        super().__init__(address, _Handler)


class _Handler(socketserver.BaseRequestHandler):
    server: _TcpServer

    def handle(self) -> None:
        sock: socket.socket = self.request
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.server.owner.run_session(sock)


class PgServer:
    """Sirve una base de datos nanosql por el protocolo v3 de PostgreSQL.

    `users` mapea usuario → contraseña (se convierten a verificadores SCRAM
    al construir; las contraseñas en claro no se guardan). Con
    `auth="trust"` se acepta cualquier usuario sin contraseña.
    """

    def __init__(
        self,
        database: str | Path,
        *,
        host: str = "127.0.0.1",
        port: int = 5432,
        auth: str = "trust",
        users: Mapping[str, str] | None = None,
        backend: str | None = None,
        isolation_level: str = "read_committed",
    ) -> None:
        if auth not in AUTH_METHODS:
            raise ValueError(f"método de autenticación desconocido: {auth!r}")
        if auth == "scram-sha-256" and not users:
            raise ValueError("scram-sha-256 requiere al menos un usuario con contraseña")
        self.auth_method = auth
        self.users = {
            name: ScramVerifier.from_password(password) for name, password in (users or {}).items()
        }
        self.default_isolation = isolation_level
        self._anchor: Connection = connect(
            database, backend=backend, isolation_level=isolation_level, autocommit=True
        )
        self._tcp = _TcpServer((host, port), self)
        self._pids = itertools.count(os.getpid() * 1000 % 2_000_000_000 + 1)
        self._sessions: dict[int, Session] = {}
        self._sessions_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._tcp.server_address[:2]
        return str(host), int(port)

    # -- servicios para las sesiones ---------------------------------------

    def new_connection(self) -> Connection:
        return self._anchor.sibling(isolation_level=self.default_isolation, autocommit=True)

    def catalog_snapshot(self) -> Catalog:
        return self._anchor._require_engine().catalog_snapshot()

    def cancel(self, pid: int, secret: int) -> None:
        """CancelRequest: nanosql no puede interrumpir una sentencia en curso
        (no hay puntos de cancelación en el ejecutor); se registra y se
        ignora, como permite el protocolo (la cancelación es best-effort)."""
        with self._sessions_lock:
            session = self._sessions.get(pid)
        if session is not None and session.secret == secret:
            log.info("CancelRequest para la sesión %s ignorada (no soportada)", pid)

    def run_session(self, sock: socket.socket) -> None:
        pid = next(self._pids)
        session = Session(sock, self, pid)
        with self._sessions_lock:
            self._sessions[pid] = session
        try:
            session.run()
        finally:
            with self._sessions_lock:
                self._sessions.pop(pid, None)

    # -- ciclo de vida -------------------------------------------------------

    def serve_forever(self) -> None:
        log.info("nanosql escuchando en %s:%s (auth=%s)", *self.address, self.auth_method)
        try:
            self._tcp.serve_forever(poll_interval=0.2)
        finally:
            self._tcp.server_close()

    def start(self) -> PgServer:
        """Arranca en un hilo de fondo (para tests y uso embebido)."""
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._tcp.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._tcp.server_close()
        self._anchor.close()

    def __enter__(self) -> PgServer:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()
