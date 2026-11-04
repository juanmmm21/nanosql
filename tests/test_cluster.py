"""Clúster nanosql de verdad: un proceso por nodo, Raft por TCP (a través
de proxies de partición), log persistente en disco.

* operaciones SQL básicas y descubrimiento del líder desde un seguidor;
* `kill -9` del líder: el clúster sigue, el nodo reinicia desde disco y se
  pone al día, nada confirmado se pierde ni se duplica;
* el checker detecta una violación INYECTADA en un clúster real (modo
  `--unsafe-local-reads`: lecturas servidas sin pasar por el log) y la
  misma secuencia es linealizable en modo normal;
* una corrida corta de caos (kill -9, particiones, SIGSTOP) es linealizable.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from nanosql.cluster import ClusterClient, IndeterminateError
from nanosql.cluster.chaos import LocalCluster, run_chaos
from nanosql.cluster.linearizability import INFINITY, Operation, check_history


@pytest.fixture()
def cluster(tmp_path: Path) -> Iterator[LocalCluster]:
    local = LocalCluster(3, tmp_path).start()
    yield local
    local.stop()


def test_sql_round_trip_and_leader_discovery(cluster: LocalCluster) -> None:
    leader = cluster.leader()
    assert leader is not None
    follower = next(n for n in cluster.ids if n != leader)
    # el cliente sólo conoce a un seguidor: la redirección le lleva al líder
    client = ClusterClient([cluster.client_addr[follower]])
    try:
        client.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)")
        assert client.execute("INSERT INTO kv (k, v) VALUES (?, ?), (?, ?)", (1, "a", 2, "b")) == 2
        assert client.query("SELECT v FROM kv ORDER BY k") == [("a",), ("b",)]
        with pytest.raises(Exception) as info:
            client.execute("INSERT INTO kv (k, v) VALUES (1, 'dup')")
        assert getattr(info.value, "sqlstate", None) == "23505"
    finally:
        client.close()


def test_leader_kill9_failover_and_restart_from_disk(cluster: LocalCluster) -> None:
    client = ClusterClient(cluster.endpoints)
    try:
        client.execute("CREATE TABLE c (k INTEGER PRIMARY KEY, n INTEGER)")
        client.execute("INSERT INTO c (k, n) VALUES (0, 0)")
        for _ in range(20):
            client.execute("UPDATE c SET n = n + 1 WHERE k = 0")
        leader = cluster.leader()
        assert leader is not None
        cluster.kill(leader)  # SIGKILL: sin oportunidad de escribir nada más
        for _ in range(20):
            client.execute("UPDATE c SET n = n + 1 WHERE k = 0")
        assert client.query("SELECT n FROM c WHERE k = 0") == [(40,)]
        cluster.start_node(leader)
        # el nodo reiniciado reconstruye su base de datos desde su log en disco
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            statuses = cluster.statuses()
            if (
                leader in statuses
                and statuses[leader]["last_applied"]
                >= statuses[cluster.leader() or leader]["commit_index"]
            ):
                break
            time.sleep(0.1)
        cluster.kill(next(n for n in cluster.ids if n != leader and n in cluster.processes))
        assert client.query("SELECT n FROM c WHERE k = 0") == [(40,)]
    finally:
        client.close()


def _stale_read_scenario(tmp_path: Path, *, unsafe: bool) -> list[Operation]:
    """Escribir 1, aislar al líder, escribir 2 en la mayoría y leer DEL
    LÍDER AISLADO. Con lecturas por el log la lectura no puede completarse
    hasta curar la partición; con lecturas locales devuelve el 1 rancio."""
    local = LocalCluster(3, tmp_path, unsafe_local_reads=unsafe).start()
    history: list[Operation] = []
    try:
        setup = ClusterClient(local.endpoints)
        setup.execute("CREATE TABLE r (k INTEGER PRIMARY KEY, v INTEGER)")
        setup.execute("INSERT INTO r (k, v) VALUES (0, 0)")
        call = time.monotonic()
        setup.execute("UPDATE r SET v = 1 WHERE k = 0")
        history.append(Operation(0, "reg:0", "write", 1, None, call, time.monotonic()))
        old_leader = local.leader()
        assert old_leader is not None
        # que el antiguo líder aplique ya el 1 antes de aislarlo
        time.sleep(0.3)
        local.partition([{old_leader}, set(local.ids) - {old_leader}])
        majority = [local.client_addr[n] for n in local.ids if n != old_leader]
        writer = ClusterClient(majority)
        call = time.monotonic()
        writer.execute("UPDATE r SET v = 2 WHERE k = 0")
        history.append(Operation(1, "reg:0", "write", 2, None, call, time.monotonic()))
        reader = ClusterClient([local.client_addr[old_leader]], deadline_seconds=3)
        call = time.monotonic()
        try:
            rows = reader.query("SELECT v FROM r WHERE k = 0")
            history.append(
                Operation(2, "reg:0", "read", None, int(rows[0][0]), call, time.monotonic())  # type: ignore[arg-type]
            )
        except IndeterminateError:
            history.append(Operation(2, "reg:0", "read", None, None, call, INFINITY))
        for client in (setup, writer, reader):
            client.close()
    finally:
        local.heal()
        local.stop()
    return history


def test_checker_detects_injected_stale_read_on_a_real_cluster(tmp_path: Path) -> None:
    history = _stale_read_scenario(tmp_path / "unsafe", unsafe=True)
    read = history[-1]
    assert read.output == 1  # el líder aislado sirvió el valor rancio
    result = check_history(history)
    assert not result.linearizable
    assert [p.key for p in result.violations] == ["reg:0"]


def test_same_scenario_is_linearizable_with_reads_through_the_log(tmp_path: Path) -> None:
    history = _stale_read_scenario(tmp_path / "safe", unsafe=False)
    assert history[-1].pending  # el líder aislado no puede comprometer la lectura
    assert check_history(history).linearizable


def test_short_chaos_run_is_linearizable(tmp_path: Path) -> None:
    report, history = run_chaos(
        nodes=3,
        duration=20,
        clients=4,
        keys=3,
        seed=11,
        root=tmp_path,
        fault_interval=(1.0, 2.5),
    )
    summary = report.to_json()
    assert sum(report.faults.values()) >= 3
    assert summary["ok"] > 100
    assert summary["linearizable"] is True, summary
