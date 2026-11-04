"""Máquina de estados replicada del clúster nanosql: una base de datos
nanosql completa más una tabla de sesiones de cliente.

Cada comando del log de Raft es un JSON
``{"c": cliente, "s": secuencia, "op": "exec"|"query", "sql": ..., "p": [...]}``.
La tabla de sesiones implementa la semántica de "exactamente una vez" de
§8 del paper de Raft: un cliente reintenta una operación con la MISMA
secuencia cuando no sabe si se aplicó (líder caído tras comprometer pero
antes de responder); si esa secuencia ya se aplicó, la réplica devuelve el
resultado guardado sin re-ejecutar — sin esto, un `UPDATE ... SET n = n + 1`
reintentado se aplicaría dos veces y la historia dejaría de ser
linealizable. Las lecturas también pasan por el log (lecturas linealizables
sin lease ni read-index): cuestan un round-trip de quorum, a cambio no
pueden servir un dato rancio desde un líder depuesto.

El resultado de cada comando (filas o error SQL tipado) es determinista y
se guarda por `(cliente, secuencia)`; los errores SQL se replican igual que
los éxitos — la misma sentencia falla igual en todas las réplicas.
"""

from __future__ import annotations

import json
from typing import Any

from ..catalog import ColumnValue
from ..distributed import NanosqlStateMachine
from ..errors import NanosqlError

Result = dict[str, Any]


def encode_command(
    client: str, seq: int, op: str, sql: str, params: list[ColumnValue] | None = None
) -> bytes:
    return json.dumps(
        {"c": client, "s": seq, "op": op, "sql": sql, "p": params or []},
        separators=(",", ":"),
    ).encode("utf-8")


class ClusterStateMachine:
    """`raft_replication_log.protocols.StateMachine` con sesiones."""

    def __init__(self, *, unsafe_local_reads: bool = False) -> None:
        self._db = NanosqlStateMachine()
        # cliente -> (última secuencia aplicada, su resultado)
        self._sessions: dict[str, tuple[int, Result]] = {}
        self.applied = 0
        # Sólo para demostrar que el checker detecta violaciones: ver
        # `ClusterNodeServer` (lecturas servidas sin pasar por el log).
        self.unsafe_local_reads = unsafe_local_reads

    # -- StateMachine -----------------------------------------------------

    def apply(self, command: bytes) -> None:
        self.applied += 1
        try:
            payload = json.loads(command.decode("utf-8"))
            client, seq = str(payload["c"]), int(payload["s"])
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            return  # comando ilegible: idéntico en todas las réplicas, se ignora
        session = self._sessions.get(client)
        if session is not None and seq <= session[0]:
            return  # duplicado (reintento) o reintento antiguo: nunca se re-aplica
        result = self.execute_local(str(payload.get("sql", "")), list(payload.get("p", [])))
        self._sessions[client] = (seq, result)

    def execute_local(self, sql: str, params: list[ColumnValue]) -> Result:
        try:
            cursor = self._db.connection.execute(sql, params)
        except NanosqlError as exc:
            return {"ok": False, "error": str(exc).splitlines()[0], "sqlstate": exc.sqlstate}
        if cursor.description is not None:
            return {
                "ok": True,
                "columns": [d[0] for d in cursor.description],
                "rows": [list(row) for row in cursor.fetchall()],
            }
        return {"ok": True, "rowcount": cursor.rowcount}

    def take_snapshot(self) -> bytes:
        return json.dumps(
            {
                "db": self._db.take_snapshot().decode("utf-8"),
                "sessions": {c: [s, r] for c, (s, r) in self._sessions.items()},
                "applied": self.applied,
            },
            ensure_ascii=False,
        ).encode("utf-8")

    def restore_snapshot(self, state: bytes) -> None:
        payload = json.loads(state.decode("utf-8"))
        self._db.restore_snapshot(payload["db"].encode("utf-8"))
        self._sessions = {c: (int(v[0]), v[1]) for c, v in payload["sessions"].items()}
        self.applied = int(payload.get("applied", 0))

    # -- consultas para el servidor de clientes ---------------------------

    def result_for(self, client: str, seq: int) -> Result | None:
        session = self._sessions.get(client)
        if session is None or session[0] < seq:
            return None
        if session[0] == seq:
            return session[1]
        # El cliente ya avanzó más allá: la secuencia antigua se descartó o
        # se aplicó antes; su resultado ya no se conserva.
        return {"ok": False, "error": "secuencia antigua", "sqlstate": "40003"}

    def close(self) -> None:
        self._db.close()
