"""Modo distribuido opcional: replicación Raft y sharding con fan-out/merge.

Estos tests requieren el extra `nanosql[distributed]` (raft-replication-log
y distributed-query-router instalados).
"""

from __future__ import annotations

import pytest
from distributed_query_router.models import AggregateFunction

import nanosql
from nanosql.distributed import (
    NanosqlStateMachine,
    ReplicatedNanosql,
    ShardedNanosql,
)

DDL = "CREATE TABLE m (id INTEGER PRIMARY KEY, grupo VARCHAR(10) NOT NULL, v FLOAT NOT NULL)"


class TestReplicated:
    @pytest.fixture()
    def cluster(self) -> ReplicatedNanosql:  # type: ignore[misc]
        replicated = ReplicatedNanosql(num_nodes=3, seed=11)
        yield replicated
        replicated.close()

    def test_writes_replicate_to_all_nodes(self, cluster: ReplicatedNanosql) -> None:
        cluster.execute(DDL)
        cluster.execute("INSERT INTO m (id, grupo, v) VALUES (1, 'a', 1.5), (2, 'b', 2.5)")
        expected = [(2,)]
        for node_id in cluster.node_ids:
            assert cluster.query("SELECT COUNT(*) FROM m", node_id=node_id) == expected

    def test_survives_leader_crash(self, cluster: ReplicatedNanosql) -> None:
        cluster.execute(DDL)
        cluster.execute("INSERT INTO m (id, grupo, v) VALUES (1, 'a', 1.0)")
        old_leader = cluster.leader()
        cluster.crash_node(old_leader)
        # Con 2 de 3 nodos sigue habiendo quorum: la escritura debe avanzar.
        cluster.execute("INSERT INTO m (id, grupo, v) VALUES (2, 'b', 2.0)")
        new_leader = cluster.leader()
        assert new_leader != old_leader
        assert cluster.query("SELECT COUNT(*) FROM m") == [(2,)]
        # El nodo recuperado converge al mismo estado que el resto.
        cluster.recover_node(old_leader)
        assert cluster.query("SELECT COUNT(*) FROM m", node_id=old_leader) == [(2,)]

    def test_state_machine_snapshot_round_trip(self) -> None:
        machine = NanosqlStateMachine()
        machine.apply(DDL.encode())
        machine.apply(b"CREATE UNIQUE INDEX ix_g ON m (grupo)")
        machine.apply(b"INSERT INTO m (id, grupo, v) VALUES (1, 'a''b', 1.5)")
        snapshot = machine.take_snapshot()

        restored = NanosqlStateMachine()
        restored.restore_snapshot(snapshot)
        assert restored.connection.execute("SELECT grupo, v FROM m").fetchall() == [("a'b", 1.5)]
        # El índice único también viaja en el snapshot.
        with pytest.raises(nanosql.IntegrityError):
            restored.connection.execute("INSERT INTO m (id, grupo, v) VALUES (2, 'a''b', 9.0)")
        machine.close()
        restored.close()


class TestSharded:
    @pytest.fixture()
    def sharded(self) -> ShardedNanosql:  # type: ignore[misc]
        db = ShardedNanosql(num_shards=4, seed=5)
        db.create_table(DDL)
        for n in range(40):
            db.insert(
                "m",
                {"id": n, "grupo": f"g{n % 3}", "v": float(n)},
                shard_key="id",
            )
        yield db
        db.close()

    def test_rows_are_actually_partitioned(self, sharded: ShardedNanosql) -> None:
        counts = sharded.shard_row_counts("m")
        assert sum(counts.values()) == 40
        assert sum(1 for c in counts.values() if c > 0) > 1  # más de un shard con datos

    def test_select_fans_out_and_concatenates(self, sharded: ShardedNanosql) -> None:
        result = sharded.select("SELECT id FROM m WHERE v >= 35")
        assert result.is_partial is False
        assert sorted(row["id"] for row in result.rows) == [35, 36, 37, 38, 39]

    def test_distributed_aggregates_match_single_node_reference(
        self, sharded: ShardedNanosql
    ) -> None:
        # Referencia: la misma carga en un nanosql de nodo único.
        reference = nanosql.connect(":memory:", autocommit=True)
        reference.execute(DDL)
        values = ", ".join(f"({n}, 'g{n % 3}', {float(n)})" for n in range(40))
        reference.execute(f"INSERT INTO m (id, grupo, v) VALUES {values}")
        expected = {
            row[0]: row[1:]
            for row in reference.execute(
                "SELECT grupo, COUNT(*) AS n, SUM(v) AS total, AVG(v) AS media,"
                " MIN(v) AS minimo, MAX(v) AS maximo FROM m GROUP BY grupo"
            ).fetchall()
        }
        reference.close()

        result = sharded.aggregate(
            "m",
            group_by=["grupo"],
            aggregates=[
                (AggregateFunction.COUNT, None, "n"),
                (AggregateFunction.SUM, "v", "total"),
                (AggregateFunction.AVG, "v", "media"),
                (AggregateFunction.MIN, "v", "minimo"),
                (AggregateFunction.MAX, "v", "maximo"),
            ],
        )
        assert result.is_partial is False
        assert len(result.rows) == len(expected)
        for row in result.rows:
            n, total, media, minimo, maximo = expected[row["grupo"]]
            assert row["n"] == n
            assert row["total"] == pytest.approx(total)
            # La comprobación clave del merge distribuido: AVG ponderado
            # exacto aunque los shards tengan tamaños distintos.
            assert row["media"] == pytest.approx(media)
            assert row["minimo"] == pytest.approx(minimo)
            assert row["maximo"] == pytest.approx(maximo)

    def test_global_aggregate_without_group_by(self, sharded: ShardedNanosql) -> None:
        result = sharded.aggregate(
            "m",
            aggregates=[
                (AggregateFunction.COUNT, None, "n"),
                (AggregateFunction.AVG, "v", "media"),
            ],
        )
        assert result.rows[0]["n"] == 40
        assert result.rows[0]["media"] == pytest.approx(sum(range(40)) / 40)

    def test_failed_shard_yields_partial_result_not_crash(self, sharded: ShardedNanosql) -> None:
        sharded.fail_shard(sharded.shard_ids[0])
        result = sharded.select("SELECT id FROM m")
        assert result.is_partial is True
        assert len(result.failures) == 1
        assert 0 < len(result.rows) < 40
        sharded.heal_shard(sharded.shard_ids[0])
        healed = sharded.select("SELECT id FROM m")
        assert healed.is_partial is False
        assert len(healed.rows) == 40

    def test_where_pushdown_to_shards(self, sharded: ShardedNanosql) -> None:
        result = sharded.aggregate(
            "m",
            aggregates=[(AggregateFunction.COUNT, None, "n")],
            where="v < 10",
        )
        assert result.rows[0]["n"] == 10
