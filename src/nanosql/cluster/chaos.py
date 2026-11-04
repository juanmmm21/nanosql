"""Lanzador de clústeres multiproceso y arnés de caos con verificación de
linealizabilidad.

* `LocalCluster` arranca N procesos `python -m nanosql.cluster.node` y un
  `LinkProxy` por enlace dirigido entre nodos (el nodo i ve al peer j en la
  dirección de su proxy i→j), de modo que una partición es bloquear
  proxies. También mata (`SIGKILL`), pausa (`SIGSTOP`/`SIGCONT`) y
  reinicia nodos con su mismo directorio de datos.
* `run_chaos` ejecuta una carga SQL de registros (`UPDATE registers SET v =
  ? WHERE k = ?` / `SELECT v ...`) y contadores (`UPDATE counters SET n = n
  + 1` / `SELECT n ...`) desde varios clientes concurrentes mientras un
  "nemesis" inyecta un fallo cada pocos segundos, registra cada operación
  con sus instantes de invocación y respuesta y, al final, comprueba la
  historia con el checker WGL de `linearizability.py`.
"""

from __future__ import annotations

import json
import os
import random
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .client import ClusterClient, ClusterSqlError, IndeterminateError
from .linearizability import INFINITY, CheckResult, Operation, check_history
from .node import wait_until_listening
from .proxy import LinkProxy

HOST = "127.0.0.1"


def free_ports(count: int) -> list[int]:
    sockets = []
    for _ in range(count):
        sock = socket.socket()
        sock.bind((HOST, 0))
        sockets.append(sock)
    ports = [int(s.getsockname()[1]) for s in sockets]
    for sock in sockets:
        sock.close()
    return ports


class LocalCluster:
    def __init__(
        self, size: int, root: Path, *, unsafe_local_reads: bool = False, log_level: str = "ERROR"
    ) -> None:
        if not 3 <= size <= 5:
            raise ValueError("el clúster estático admite 3 a 5 nodos")
        self.root = root
        self.ids = [f"n{i}" for i in range(1, size + 1)]
        ports = free_ports(2 * size)
        self.raft_addr = {n: (HOST, ports[i]) for i, n in enumerate(self.ids)}
        self.client_addr = {n: (HOST, ports[size + i]) for i, n in enumerate(self.ids)}
        self.proxies: dict[tuple[str, str], LinkProxy] = {}
        for src in self.ids:
            for dst in self.ids:
                if src != dst:
                    proxy = LinkProxy((HOST, 0), self.raft_addr[dst]).start()
                    self.proxies[(src, dst)] = proxy
        config = {
            "nodes": {
                n: {
                    "raft": list(self.raft_addr[n]),
                    "client": list(self.client_addr[n]),
                    "peers": {p: list(self.proxies[(n, p)].address) for p in self.ids if p != n},
                }
                for n in self.ids
            }
        }
        root.mkdir(parents=True, exist_ok=True)
        self.config_path = root / "cluster.json"
        self.config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        self.unsafe_local_reads = unsafe_local_reads
        self.log_level = log_level
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.paused: set[str] = set()

    @property
    def endpoints(self) -> list[tuple[str, int]]:
        return [self.client_addr[n] for n in self.ids]

    # -- procesos -----------------------------------------------------------

    def start_node(self, node_id: str) -> None:
        args = [
            sys.executable,
            "-m",
            "nanosql.cluster.node",
            "--config",
            str(self.config_path),
            "--id",
            node_id,
            "--data-dir",
            str(self.root / node_id),
            "--log-level",
            self.log_level,
        ]
        if self.unsafe_local_reads:
            args.append("--unsafe-local-reads")
        log_file = open(self.root / f"{node_id}.log", "ab")  # noqa: SIM115
        self.processes[node_id] = subprocess.Popen(args, stdout=log_file, stderr=subprocess.STDOUT)
        log_file.close()
        if not wait_until_listening(self.client_addr[node_id], timeout=30):
            raise RuntimeError(f"el nodo {node_id} no arrancó (ver {node_id}.log)")

    def start(self) -> LocalCluster:
        for node_id in self.ids:
            self.start_node(node_id)
        return self

    def kill(self, node_id: str) -> None:
        process = self.processes.pop(node_id, None)
        if process is not None:
            if node_id in self.paused:
                os.kill(process.pid, signal.SIGCONT)
                self.paused.discard(node_id)
            process.send_signal(signal.SIGKILL)
            process.wait(timeout=10)

    def pause(self, node_id: str) -> None:
        process = self.processes.get(node_id)
        if process is not None:
            os.kill(process.pid, signal.SIGSTOP)
            self.paused.add(node_id)

    def resume(self, node_id: str) -> None:
        process = self.processes.get(node_id)
        if process is not None and node_id in self.paused:
            os.kill(process.pid, signal.SIGCONT)
            self.paused.discard(node_id)

    def partition(self, groups: list[set[str]]) -> None:
        group_of = {n: i for i, group in enumerate(groups) for n in group}
        for (src, dst), proxy in self.proxies.items():
            proxy.set_blocked(group_of.get(src) != group_of.get(dst))

    def heal(self) -> None:
        for proxy in self.proxies.values():
            proxy.set_blocked(False)

    def statuses(self) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for node_id in self.processes:
            if node_id in self.paused:
                continue
            try:
                with socket.create_connection(self.client_addr[node_id], timeout=0.5) as sock:
                    sock.settimeout(0.5)
                    sock.sendall(b'{"type":"status"}\n')
                    line = sock.makefile("rb").readline()
                result[node_id] = json.loads(line)
            except (OSError, ValueError):
                continue
        return result

    def leader(self, timeout: float = 10.0) -> str | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            statuses = self.statuses()
            leaders = [n for n, s in statuses.items() if s.get("role") == "leader"]
            if leaders:
                # con varios "líderes" (uno depuesto aún no enterado) gana el de mayor término
                return max(leaders, key=lambda n: int(statuses[n].get("term", 0)))
            time.sleep(0.05)
        return None

    def stop(self) -> None:
        for node_id in list(self.processes):
            self.resume(node_id)
            process = self.processes.pop(node_id)
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        for proxy in self.proxies.values():
            proxy.stop()


# ---------------------------------------------------------------------------
# Carga y nemesis
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ChaosReport:
    nodes: int
    duration_seconds: float
    seed: int
    operations: int = 0
    ok: int = 0
    indeterminate: int = 0
    failed: int = 0
    faults: Counter[str] = field(default_factory=Counter)
    check: CheckResult | None = None
    check_seconds: float = 0.0
    latencies_ms: list[float] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        lat = sorted(self.latencies_ms)

        def pct(p: float) -> float | None:
            return round(lat[min(len(lat) - 1, int(p * len(lat)))], 1) if lat else None

        check = self.check
        return {
            "nodes": self.nodes,
            "duration_seconds": round(self.duration_seconds, 1),
            "seed": self.seed,
            "operations": self.operations,
            "ok": self.ok,
            "indeterminate": self.indeterminate,
            "failed": self.failed,
            "throughput_ops_per_s": round(self.ok / self.duration_seconds, 1)
            if self.duration_seconds
            else 0,
            "latency_ms": {"p50": pct(0.5), "p99": pct(0.99), "max": pct(1.0)},
            "faults": dict(self.faults),
            "linearizable": check.linearizable if check else None,
            "violations": [p.key for p in check.violations] if check else [],
            "undecided": [p.key for p in check.undecided] if check else [],
            "keys_checked": len(check.partitions) if check else 0,
            "check_seconds": round(self.check_seconds, 2),
        }


def _setup_tables(endpoints: list[tuple[str, int]], keys: int) -> None:
    client = ClusterClient(endpoints, deadline_seconds=60)
    try:
        client.execute("CREATE TABLE IF NOT EXISTS registers (k INTEGER PRIMARY KEY, v INTEGER)")
        client.execute("CREATE TABLE IF NOT EXISTS counters (k INTEGER PRIMARY KEY, n INTEGER)")
        values = ", ".join(f"({k}, 0)" for k in range(keys))
        client.execute(f"INSERT INTO registers (k, v) VALUES {values}")
        client.execute(f"INSERT INTO counters (k, n) VALUES {values}")
    finally:
        client.close()


def _worker(
    process: int,
    endpoints: list[tuple[str, int]],
    keys: int,
    seed: int,
    stop: threading.Event,
    history: list[Operation],
    lock: threading.Lock,
    latencies: list[float],
    failures: list[int],
) -> None:
    rng = random.Random(seed * 1000 + process)
    client = ClusterClient(endpoints, client_id=f"w{seed}-{process}", deadline_seconds=10)
    next_value = process * 1_000_000
    try:
        while not stop.is_set():
            key = rng.randrange(keys)
            choice = rng.random()
            call = time.monotonic()
            output: int | None = None
            value: int | None = None
            try:
                if choice < 0.3:
                    kind, name = "write", f"reg:{key}"
                    next_value += 1
                    value = next_value
                    client.execute("UPDATE registers SET v = ? WHERE k = ?", (value, key))
                elif choice < 0.6:
                    kind, name = "read", f"reg:{key}"
                    rows = client.query("SELECT v FROM registers WHERE k = ?", (key,))
                    output = int(rows[0][0])  # type: ignore[arg-type]
                elif choice < 0.8:
                    kind, name = "inc", f"cnt:{key}"
                    client.execute("UPDATE counters SET n = n + 1 WHERE k = ?", (key,))
                else:
                    kind, name = "read", f"cnt:{key}"
                    rows = client.query("SELECT n FROM counters WHERE k = ?", (key,))
                    output = int(rows[0][0])  # type: ignore[arg-type]
                ret = time.monotonic()
                latencies.append((ret - call) * 1000)
            except IndeterminateError:
                ret = INFINITY
            except ClusterSqlError:
                with lock:
                    failures.append(1)
                continue  # falló de forma definitiva: sin efecto, fuera de la historia
            with lock:
                history.append(Operation(process, name, kind, value, output, call, ret))
    finally:
        client.close()


def _nemesis(
    cluster: LocalCluster,
    rng: random.Random,
    stop: threading.Event,
    faults: Counter[str],
    interval: tuple[float, float],
    kinds: tuple[str, ...],
) -> None:
    while not stop.wait(rng.uniform(*interval)):
        kind = rng.choice(kinds)
        hold = rng.uniform(1.5, 4.0)
        if kind == "kill-leader":
            leader = cluster.leader(timeout=5)
            if leader is None:
                continue
            cluster.kill(leader)
            faults[kind] += 1
            stop.wait(hold)
            cluster.start_node(leader)
        elif kind == "partition":
            ids = list(cluster.ids)
            rng.shuffle(ids)
            cut = rng.randint(1, len(ids) // 2)
            cluster.partition([set(ids[:cut]), set(ids[cut:])])
            faults[kind] += 1
            stop.wait(hold)
            cluster.heal()
        elif kind == "partition-leader":
            leader = cluster.leader(timeout=5)
            if leader is None:
                continue
            cluster.partition([{leader}, set(cluster.ids) - {leader}])
            faults[kind] += 1
            stop.wait(hold)
            cluster.heal()
        elif kind == "pause":
            victim = rng.choice([n for n in cluster.ids if n in cluster.processes])
            cluster.pause(victim)
            faults[kind] += 1
            stop.wait(hold)
            cluster.resume(victim)


def run_chaos(
    *,
    nodes: int = 3,
    duration: float = 60.0,
    clients: int = 6,
    keys: int = 4,
    seed: int = 1,
    root: Path,
    fault_interval: tuple[float, float] = (2.0, 5.0),
    fault_kinds: tuple[str, ...] = ("kill-leader", "partition", "partition-leader", "pause"),
    unsafe_local_reads: bool = False,
    progress: bool = False,
) -> tuple[ChaosReport, list[Operation]]:
    report = ChaosReport(nodes=nodes, duration_seconds=0.0, seed=seed)
    cluster = LocalCluster(nodes, root, unsafe_local_reads=unsafe_local_reads).start()
    history: list[Operation] = []
    lock = threading.Lock()
    latencies: list[float] = []
    failures: list[int] = []
    stop = threading.Event()
    try:
        _setup_tables(cluster.endpoints, keys)
        workers = [
            threading.Thread(
                target=_worker,
                args=(p, cluster.endpoints, keys, seed, stop, history, lock, latencies, failures),
                daemon=True,
            )
            for p in range(clients)
        ]
        nemesis = threading.Thread(
            target=_nemesis,
            args=(cluster, random.Random(seed), stop, report.faults, fault_interval, fault_kinds),
            daemon=True,
        )
        started = time.monotonic()
        for thread in workers:
            thread.start()
        if fault_kinds:
            nemesis.start()
        while time.monotonic() - started < duration:
            time.sleep(min(5.0, duration))
            if progress:
                with lock:
                    count = len(history)
                elapsed = time.monotonic() - started
                print(
                    f"[{elapsed:6.0f}s] operaciones={count} fallos={dict(report.faults)}",
                    flush=True,
                )
        stop.set()
        for thread in workers:
            thread.join(timeout=30)
        if fault_kinds:
            nemesis.join(timeout=30)
        report.duration_seconds = time.monotonic() - started
    finally:
        stop.set()
        cluster.stop()
    with lock:
        operations = list(history)
    report.operations = len(operations)
    report.indeterminate = sum(1 for o in operations if o.pending)
    report.ok = report.operations - report.indeterminate
    report.failed = len(failures)
    report.latencies_ms = latencies
    checked = time.monotonic()
    report.check = check_history(operations)
    report.check_seconds = time.monotonic() - checked
    return report, operations
