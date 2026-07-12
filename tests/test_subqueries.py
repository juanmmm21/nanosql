"""Subconsultas no correlacionadas: materialización bottom-up end-to-end.

Cubre las semánticas SQL delicadas (lógica trivalente de `NOT IN` con NULLs,
subconsulta escalar vacía → NULL, cardinalidad >1 → error de datos), la
detección estática de correlación, el aislamiento (la subconsulta ve el MISMO
snapshot que su consulta externa) y los límites honestos (EXPLAIN, modo
shardeado, tablas derivadas en FROM).
"""

from __future__ import annotations

import pytest

import nanosql
from nanosql import DataError, NotSupportedError, ProgrammingError
from nanosql.distributed import ShardedNanosql


@pytest.fixture()
def conn() -> nanosql.Connection:  # type: ignore[misc]
    connection = nanosql.connect(":memory:", autocommit=True)
    connection.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50), age INTEGER)")
    connection.execute("CREATE TABLE vips (uid INTEGER, level INTEGER)")
    connection.execute(
        "INSERT INTO users (id, name, age) VALUES (1, 'ana', 30), (2, 'bob', 40), (3, 'eva', 50)"
    )
    connection.execute("INSERT INTO vips (uid, level) VALUES (1, 10), (3, 20)")
    yield connection
    connection.close()


class TestInSubquery:
    def test_in_subquery(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT name FROM users WHERE id IN (SELECT uid FROM vips) ORDER BY id"
        ).fetchall()
        assert rows == [("ana",), ("eva",)]

    def test_not_in_subquery(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT name FROM users WHERE id NOT IN (SELECT uid FROM vips)"
        ).fetchall()
        assert rows == [("bob",)]

    def test_in_subquery_with_filter_inside(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT name FROM users WHERE id IN (SELECT uid FROM vips WHERE level > 15)"
        ).fetchall()
        assert rows == [("eva",)]

    def test_in_empty_subquery_is_false_for_every_row(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE TABLE empty_t (v INTEGER)")
        assert (
            conn.execute("SELECT name FROM users WHERE id IN (SELECT v FROM empty_t)").fetchall()
            == []
        )

    def test_not_in_empty_subquery_is_true_for_every_row(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE TABLE empty_t (v INTEGER)")
        rows = conn.execute(
            "SELECT name FROM users WHERE id NOT IN (SELECT v FROM empty_t) ORDER BY id"
        ).fetchall()
        assert rows == [("ana",), ("bob",), ("eva",)]

    def test_in_subquery_requires_exactly_one_column(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError, match="exactamente 1 columna"):
            conn.execute("SELECT name FROM users WHERE id IN (SELECT uid, level FROM vips)")


class TestNotInWithNulls:
    """`x NOT IN (..., NULL)` nunca es TRUE (lógica trivalente): si ningún
    elemento coincide, el resultado es NULL (desconocido), no TRUE — la fila
    no pasa el filtro. Verificado contra el ejecutor real, no asumido."""

    def test_not_in_with_null_in_subquery_result_matches_nothing(
        self, conn: nanosql.Connection
    ) -> None:
        conn.execute("CREATE TABLE maybe (v INTEGER)")
        conn.execute("INSERT INTO maybe (v) VALUES (2), (NULL)")
        rows = conn.execute(
            "SELECT name FROM users WHERE id NOT IN (SELECT v FROM maybe)"
        ).fetchall()
        assert rows == []

    def test_in_with_null_in_subquery_result_still_matches_equal_rows(
        self, conn: nanosql.Connection
    ) -> None:
        conn.execute("CREATE TABLE maybe (v INTEGER)")
        conn.execute("INSERT INTO maybe (v) VALUES (2), (NULL)")
        rows = conn.execute("SELECT name FROM users WHERE id IN (SELECT v FROM maybe)").fetchall()
        assert rows == [("bob",)]


class TestScalarSubquery:
    def test_scalar_subquery_in_where(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT name FROM users WHERE age > (SELECT AVG(age) FROM users)"
        ).fetchall()
        assert rows == [("eva",)]

    def test_scalar_subquery_in_select_list(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT name, (SELECT MAX(level) FROM vips) AS top FROM users WHERE id = 1"
        ).fetchall()
        assert rows == [("ana", 20)]

    def test_scalar_subquery_with_zero_rows_is_null(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE TABLE empty_t (v INTEGER)")
        # age > NULL es NULL para toda fila: ninguna pasa el filtro.
        assert (
            conn.execute("SELECT name FROM users WHERE age > (SELECT v FROM empty_t)").fetchall()
            == []
        )
        # Y en la lista de proyección, el NULL se ve directamente.
        rows = conn.execute(
            "SELECT (SELECT v FROM empty_t) AS nothing FROM users WHERE id = 1"
        ).fetchall()
        assert rows == [(None,)]

    def test_scalar_subquery_with_more_than_one_row_is_a_data_error(
        self, conn: nanosql.Connection
    ) -> None:
        with pytest.raises(DataError, match="devolvió 3 filas"):
            conn.execute("SELECT name FROM users WHERE age > (SELECT age FROM users)")

    def test_scalar_subquery_requires_exactly_one_column(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError, match="exactamente 1 columna"):
            conn.execute("SELECT name FROM users WHERE age > (SELECT uid, level FROM vips)")

    def test_scalar_subquery_in_arithmetic(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT name FROM users WHERE age = (SELECT MIN(age) FROM users) + 10"
        ).fetchall()
        assert rows == [("bob",)]


class TestNestingAndParameters:
    def test_nested_subqueries(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT name FROM users WHERE id IN "
            "(SELECT uid FROM vips WHERE level > (SELECT MIN(level) FROM vips))"
        ).fetchall()
        assert rows == [("eva",)]

    def test_placeholders_inside_subqueries(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT name FROM users WHERE age > ? "
            "AND id IN (SELECT uid FROM vips WHERE level >= ?)",
            (20, 20),
        ).fetchall()
        assert rows == [("eva",)]

    def test_placeholder_arity_counts_subquery_placeholders(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute(
                "SELECT name FROM users WHERE id IN (SELECT uid FROM vips WHERE level > ?)"
            )


class TestSubqueriesInDml:
    def test_update_with_scalar_subquery_in_set_and_in_subquery_in_where(
        self, conn: nanosql.Connection
    ) -> None:
        cur = conn.execute(
            "UPDATE users SET age = (SELECT MAX(level) FROM vips) "
            "WHERE id IN (SELECT uid FROM vips)"
        )
        assert cur.rowcount == 2
        rows = conn.execute("SELECT id, age FROM users ORDER BY id").fetchall()
        assert rows == [(1, 20), (2, 40), (3, 20)]

    def test_delete_with_in_subquery(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("DELETE FROM users WHERE id NOT IN (SELECT uid FROM vips)")
        assert cur.rowcount == 1
        assert conn.execute("SELECT COUNT(*) FROM users").fetchall() == [(2,)]

    def test_insert_with_scalar_subquery_in_values(self, conn: nanosql.Connection) -> None:
        conn.execute(
            "INSERT INTO users (id, name, age) VALUES (9, 'zoe', (SELECT MAX(age) FROM users))"
        )
        assert conn.execute("SELECT age FROM users WHERE id = 9").fetchall() == [(50,)]


class TestCorrelationDetection:
    def test_qualified_reference_to_outer_table_is_rejected(self, conn: nanosql.Connection) -> None:
        with pytest.raises(NotSupportedError, match="correlacionada"):
            conn.execute(
                "SELECT name FROM users WHERE id IN "
                "(SELECT uid FROM vips WHERE vips.level > users.age)"
            )

    def test_unqualified_reference_to_outer_only_column_is_rejected(
        self, conn: nanosql.Connection
    ) -> None:
        # `age` solo existe en `users` (la consulta externa): correlación.
        with pytest.raises(NotSupportedError, match="correlacionada"):
            conn.execute("SELECT name FROM users WHERE id IN (SELECT uid FROM vips WHERE age > 35)")

    def test_correlation_in_update_where_subquery_is_rejected(
        self, conn: nanosql.Connection
    ) -> None:
        with pytest.raises(NotSupportedError, match="correlacionada"):
            conn.execute(
                "UPDATE users SET age = 0 WHERE id IN "
                "(SELECT uid FROM vips WHERE level > users.age)"
            )

    def test_inner_column_shadows_outer_column_with_same_name(
        self, conn: nanosql.Connection
    ) -> None:
        # `other` también tiene una columna `id`: dentro de la subconsulta,
        # `id` resuelve contra `other` (sombra del ámbito interno), no es
        # correlación — igual que en SQL estándar.
        conn.execute("CREATE TABLE other (id INTEGER)")
        conn.execute("INSERT INTO other (id) VALUES (2)")
        rows = conn.execute("SELECT name FROM users WHERE id IN (SELECT id FROM other)").fetchall()
        assert rows == [("bob",)]

    def test_typo_inside_subquery_is_an_unresolved_column_not_a_correlation_error(
        self, conn: nanosql.Connection
    ) -> None:
        # Una columna que no existe en NINGÚN ámbito es una errata: debe
        # diagnosticarse como columna no resuelta, no como correlación.
        with pytest.raises(ProgrammingError, match="no resuelta"):
            conn.execute("SELECT name FROM users WHERE id IN (SELECT uid FROM vips WHERE nope > 1)")


class TestSnapshotIsolation:
    def test_subquery_sees_the_same_snapshot_as_its_outer_query(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        """En repeatable_read, la subconsulta debe ver el snapshot de la
        transacción de su consulta externa — no lo último confirmado por
        otras conexiones a mitad de transacción."""
        db = tmp_path / "snap"
        writer = nanosql.connect(db)
        reader = nanosql.connect(db, isolation_level="repeatable_read")
        try:
            writer.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            writer.execute("CREATE TABLE picked (id INTEGER)")
            writer.execute("INSERT INTO t (id) VALUES (1), (2)")
            writer.execute("INSERT INTO picked (id) VALUES (1)")
            writer.commit()

            # Congela el snapshot de la transacción del lector.
            before = reader.execute(
                "SELECT id FROM t WHERE id IN (SELECT id FROM picked)"
            ).fetchall()
            assert before == [(1,)]

            writer.execute("INSERT INTO picked (id) VALUES (2)")
            writer.commit()

            # El mismo SELECT dentro de la MISMA transacción del lector no
            # ve la fila nueva de `picked` (ni la vería la consulta externa).
            after = reader.execute(
                "SELECT id FROM t WHERE id IN (SELECT id FROM picked)"
            ).fetchall()
            assert after == [(1,)]
            reader.rollback()
        finally:
            reader.close()
            writer.close()


class TestHonestLimits:
    def test_explain_of_a_query_with_subqueries_is_rejected(self, conn: nanosql.Connection) -> None:
        with pytest.raises(NotSupportedError, match="EXPLAIN"):
            conn.execute("EXPLAIN SELECT name FROM users WHERE id IN (SELECT uid FROM vips)")

    def test_derived_table_in_from_is_rejected_with_scope_error(
        self, conn: nanosql.Connection
    ) -> None:
        with pytest.raises(NotSupportedError):
            conn.execute("SELECT * FROM (SELECT id FROM users) d")

    def test_sharded_mode_rejects_subqueries(self) -> None:
        sharded = ShardedNanosql(num_shards=2)
        try:
            sharded.create_table("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            sharded.insert("t", {"id": 1, "v": 10}, shard_key="id")
            with pytest.raises(NotSupportedError, match="shardeado"):
                sharded.select("SELECT v FROM t WHERE id IN (SELECT id FROM t)")
        finally:
            sharded.close()
