"""Cliente del clúster nanosql: descubre al líder y reintenta.

Cada operación lleva `(client_id, seq)`; todos los reintentos de una misma
operación reutilizan esa pareja, así que la máquina de estados la aplica
como mucho una vez aunque el cliente no sepa si un intento anterior llegó a
comprometerse. El cliente sigue las redirecciones (``redirect``) al líder
que le indica un seguidor, rota entre nodos ante errores de conexión o
``retry``, y si el plazo vence sin una respuesta definitiva lanza
`IndeterminateError`: la operación puede haberse aplicado o no — el checker
de linealizabilidad la trata así, nunca como fallo.
"""

from __future__ import annotations

import json
import socket
import time
import uuid
from collections.abc import Sequence
from typing import Any

from ..catalog import ColumnValue
from ..errors import DatabaseError, OperationalError

Address = tuple[str, int]


class IndeterminateError(OperationalError):
    """Sin respuesta definitiva dentro del plazo: resultado desconocido."""

    default_sqlstate = "08006"


class ClusterSqlError(DatabaseError):
    """La sentencia se aplicó en el clúster y falló de forma determinista
    (p.ej. violación de unicidad): definitivamente sin efecto."""


class _Connection:
    def __init__(self, address: Address, timeout: float) -> None:
        self.address = address
        self._sock = socket.create_connection(address, timeout=timeout)
        self._sock.settimeout(timeout)
        self._reader = self._sock.makefile("rb")

    def call(self, request: dict[str, Any]) -> dict[str, Any]:
        self._sock.sendall(json.dumps(request, separators=(",", ":")).encode() + b"\n")
        line = self._reader.readline()
        if not line:
            raise ConnectionError("el nodo cerró la conexión")
        response: dict[str, Any] = json.loads(line)
        return response

    def close(self) -> None:
        try:
            self._reader.close()
            self._sock.close()
        except OSError:
            pass


class ClusterClient:
    def __init__(
        self,
        endpoints: Sequence[Address],
        *,
        client_id: str | None = None,
        deadline_seconds: float = 10.0,
        request_timeout: float = 6.0,
    ) -> None:
        if not endpoints:
            raise ValueError("hace falta al menos un nodo")
        self.endpoints = list(endpoints)
        self.client_id = client_id or uuid.uuid4().hex
        self.deadline_seconds = deadline_seconds
        self.request_timeout = request_timeout
        self._seq = 0
        self._preferred = 0
        self._connections: dict[Address, _Connection] = {}

    # -- API ------------------------------------------------------------------

    def execute(self, sql: str, params: Sequence[ColumnValue] = ()) -> int:
        """Sentencia de escritura replicada; devuelve las filas afectadas."""
        result = self._run("exec", sql, params)
        return int(result.get("rowcount", -1))

    def query(self, sql: str, params: Sequence[ColumnValue] = ()) -> list[tuple[ColumnValue, ...]]:
        """Lectura linealizable (pasa por el log del líder)."""
        result = self._run("query", sql, params)
        return [tuple(row) for row in result.get("rows", [])]

    def status(self, address: Address) -> dict[str, Any]:
        return self._connection(address).call({"type": "status"})

    def close(self) -> None:
        for connection in self._connections.values():
            connection.close()
        self._connections.clear()

    # -- internos -------------------------------------------------------------

    def _connection(self, address: Address) -> _Connection:
        connection = self._connections.get(address)
        if connection is None:
            connection = _Connection(address, self.request_timeout)
            self._connections[address] = connection
        return connection

    def _drop(self, address: Address) -> None:
        connection = self._connections.pop(address, None)
        if connection is not None:
            connection.close()

    def _run(self, op: str, sql: str, params: Sequence[ColumnValue]) -> dict[str, Any]:
        self._seq += 1
        request = {
            "type": "command",
            "c": self.client_id,
            "s": self._seq,
            "op": op,
            "sql": sql,
            "p": list(params),
        }
        deadline = time.monotonic() + self.deadline_seconds
        target = self.endpoints[self._preferred % len(self.endpoints)]
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            try:
                response = self._connection(target).call(request)
            except (OSError, ConnectionError, ValueError):
                self._drop(target)
                target = self._next_endpoint(target)
                time.sleep(min(0.05 * attempt, 0.3))
                continue
            if "redirect" in response:
                hint = response.get("leader_client")
                target = (str(hint[0]), int(hint[1])) if hint else self._next_endpoint(target)
                if not hint:
                    time.sleep(0.05)
                continue
            if response.get("retry"):
                target = self._next_endpoint(target)
                time.sleep(0.05)
                continue
            self._preferred = self.endpoints.index(target) if target in self.endpoints else 0
            if response.get("ok"):
                return response
            raise ClusterSqlError(
                str(response.get("error", "error SQL")),
                sqlstate=str(response.get("sqlstate", "XX000")),
            )
        raise IndeterminateError(
            f"sin respuesta definitiva en {self.deadline_seconds:.0f} s para la operación "
            f"{self.client_id}/{self._seq}"
        )

    def _next_endpoint(self, current: Address) -> Address:
        if current in self.endpoints:
            return self.endpoints[(self.endpoints.index(current) + 1) % len(self.endpoints)]
        return self.endpoints[0]
