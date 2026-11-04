"""Un nodo del clúster nanosql como proceso independiente.

``python -m nanosql.cluster.node --config cluster.json --id n1 --data-dir d/n1``

Combina un `raft_replication_log.network.NetworkNode` (Raft por TCP con log
persistente en ``<data-dir>/raft``) con un servidor de clientes TCP de
líneas JSON:

* ``{"type": "status"}`` → rol, término, líder, índices.
* ``{"type": "command", "c": cliente, "s": seq, "op": "exec"|"query",
  "sql": ..., "p": [...]}`` → en el líder, el comando se propone, se espera
  a que se comprometa y se aplique, y se devuelve su resultado; en un
  seguidor, ``{"redirect": líder, "leader_client": [host, port]}``; si el
  líder pierde el liderazgo o vence el plazo, ``{"retry": true}`` — el
  cliente reintenta con la misma secuencia, y la tabla de sesiones de la
  máquina de estados garantiza que nunca se aplica dos veces.

La base de datos de cada réplica vive en memoria y se reconstruye al
arrancar desde el snapshot y el log de Raft en disco: la durabilidad la da
el log replicado y sincronizado con fsync, no un WAL por réplica.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import socket
import socketserver
import sys
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from raft_replication_log.errors import NotLeaderError
from raft_replication_log.models import RaftRole
from raft_replication_log.network import NetworkNode
from raft_replication_log.node import RaftNode

from .statemachine import ClusterStateMachine, encode_command

log = logging.getLogger("nanosql.cluster")

COMMIT_WAIT_SECONDS = 5.0


def _address(raw: Sequence[Any]) -> tuple[str, int]:
    return str(raw[0]), int(raw[1])


class _ClientHandler(socketserver.StreamRequestHandler):
    server: _ClientServer

    def handle(self) -> None:
        node = self.server.owner
        for line in self.rfile:
            try:
                request = json.loads(line)
                response = node.handle_client(request)
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                response = {"ok": False, "error": f"petición inválida: {exc}", "sqlstate": "08P01"}
            try:
                self.wfile.write(json.dumps(response, separators=(",", ":")).encode() + b"\n")
                self.wfile.flush()
            except OSError:
                return


class _ClientServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: tuple[str, int], owner: ClusterNode) -> None:
        self.owner = owner
        super().__init__(address, _ClientHandler)


class ClusterNode:
    def __init__(
        self,
        config: dict[str, Any],
        node_id: str,
        data_dir: Path,
        *,
        unsafe_local_reads: bool = False,
        snapshot_threshold: int | None = 2000,
    ) -> None:
        nodes = config["nodes"]
        me = nodes[node_id]
        self.node_id = node_id
        self.client_addresses = {n: _address(spec["client"]) for n, spec in nodes.items()}
        self.machine = ClusterStateMachine(unsafe_local_reads=unsafe_local_reads)
        peers = {p: _address(a) for p, a in me["peers"].items()}
        self.raft = NetworkNode(
            node_id,
            _address(me["raft"]),
            peers,
            data_dir=str(data_dir / "raft"),
            state_machine=self.machine,
            snapshot_threshold=snapshot_threshold,
        )
        self.clients = _ClientServer(_address(me["client"]), self)

    def start(self) -> None:
        self.raft.start()
        threading.Thread(target=self.clients.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self.clients.shutdown()
        self.clients.server_close()
        self.raft.stop()

    def handle_client(self, request: dict[str, Any]) -> dict[str, Any]:
        kind = request.get("type")
        if kind == "status":
            status = self.raft.status()
            status["applied_commands"] = self.machine.applied
            return status
        if kind != "command":
            return {"ok": False, "error": f"tipo de petición desconocido: {kind!r}"}
        client, seq = str(request["c"]), int(request["s"])
        op, sql, params = str(request["op"]), str(request["sql"]), list(request.get("p", []))
        if op == "query" and self.machine.unsafe_local_reads:
            # Modo deliberadamente INCORRECTO (sólo para la demostración del
            # checker): la lectura se sirve del estado local sin pasar por el
            # log, así que un líder depuesto aislado devuelve datos rancios.
            return self.raft.run(lambda _node: self.machine.execute_local(sql, params))
        done = self.raft.run(lambda _node: self.machine.result_for(client, seq))
        if done is not None:
            return done
        try:
            _, term = self.raft.submit(encode_command(client, seq, op, sql, params))
        except NotLeaderError as exc:
            leader = exc.known_leader
            return {
                "redirect": leader,
                "leader_client": list(self.client_addresses[leader]) if leader else None,
            }

        def settled(node: RaftNode) -> bool:
            return (
                self.machine.result_for(client, seq) is not None
                or node.current_term != term
                or node.role is not RaftRole.LEADER
            )

        finished = self.raft.wait_for(settled, COMMIT_WAIT_SECONDS)
        result = self.raft.run(lambda _node: self.machine.result_for(client, seq))
        if result is not None:
            return result
        return {"retry": True, "reason": "timeout" if not finished else "leadership lost"}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m nanosql.cluster.node")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--id", required=True)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--unsafe-local-reads", action="store_true")
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.WARNING))
    config = json.loads(args.config.read_text(encoding="utf-8"))
    node = ClusterNode(config, args.id, args.data_dir, unsafe_local_reads=args.unsafe_local_reads)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    node.start()
    print(f"ready {args.id}", flush=True)
    stop.wait()
    node.stop()
    return 0


def wait_until_listening(address: tuple[str, int], timeout: float) -> bool:
    """Utilidad para lanzadores: ¿acepta conexiones ya el puerto?"""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(address, timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


if __name__ == "__main__":
    sys.exit(main())
