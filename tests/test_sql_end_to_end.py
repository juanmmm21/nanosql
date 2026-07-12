"""SQL de extremo a extremo contra el motor ensamblado completo.

Cada consulta de este archivo atraviesa el pipeline real:
parser → traducción → optimizador → traducción → ejecutor vectorizado →
snapshot MVCC → row store durable → backend B+Tree.
"""

from __future__ import annotations

import pytest

import nanosql


@pytest.fixture()
def conn() -> nanosql.Connection:  # type: ignore[misc]
    connection = nanosql.connect(":memory:")
    connection.execute(
        "CREATE TABLE users ("
        " id INTEGER PRIMARY KEY,"
        " name VARCHAR(50) NOT NULL,"
        " age INTEGER,"
        " active BOOLEAN DEFAULT TRUE)"
    )
    connection.execute(
        "CREATE TABLE orders ("
        " id INTEGER PRIMARY KEY,"
        " user_id INTEGER NOT NULL,"
        " amount FLOAT NOT NULL,"
        " item VARCHAR(50))"
    )
    connection.execute(
        "INSERT INTO users (id, name, age) VALUES"
        " (1, 'ada', 36), (2, 'grace', 45), (3, 'edsger', NULL), (4, 'barbara', 29)"
    )
    connection.execute(
        "INSERT INTO orders (id, user_id, amount, item) VALUES"
        " (1, 1, 12.5, 'libro'), (2, 1, 30.0, 'teclado'), (3, 2, 5.0, 'cable'),"
        " (4, 2, 99.9, 'monitor'), (5, 4, 1.5, NULL)"
    )
    connection.commit()
    yield connection
    connection.close()


class TestSelect:
    def test_projection_and_alias(self, conn: nanosql.Connection) -> None:
        rows = conn.execute("SELECT name AS quien, age FROM users WHERE id = 2").fetchall()
        assert rows == [("grace", 45)]

    def test_star(self, conn: nanosql.Connection) -> None:
        rows = conn.execute("SELECT * FROM users WHERE id = 1").fetchall()
        assert rows == [(1, "ada", 36, True)]

    def test_arithmetic_expression(self, conn: nanosql.Connection) -> None:
        rows = conn.execute("SELECT amount * 2 AS doble FROM orders WHERE id = 1").fetchall()
        assert rows == [(25.0,)]

    def test_where_and_or_not(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT id FROM orders WHERE (amount > 10 AND user_id = 1) OR amount > 90"
        ).fetchall()
        assert sorted(rows) == [(1,), (2,), (4,)]

    def test_between_in_like(self, conn: nanosql.Connection) -> None:
        assert conn.execute(
            "SELECT id FROM orders WHERE amount BETWEEN 5 AND 30 ORDER BY id"
        ).fetchall() == [(1,), (2,), (3,)]
        assert conn.execute(
            "SELECT id FROM orders WHERE user_id IN (2, 4) ORDER BY id"
        ).fetchall() == [(3,), (4,), (5,)]
        assert conn.execute(
            "SELECT name FROM users WHERE name LIKE '%ra%' ORDER BY name"
        ).fetchall() == [("barbara",), ("grace",)]

    def test_is_null_and_is_not_null(self, conn: nanosql.Connection) -> None:
        assert conn.execute("SELECT name FROM users WHERE age IS NULL").fetchall() == [("edsger",)]
        assert conn.execute("SELECT COUNT(*) FROM users WHERE age IS NOT NULL").fetchall() == [(3,)]

    def test_order_by_limit_offset(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT name FROM users WHERE age IS NOT NULL ORDER BY age DESC LIMIT 2 OFFSET 1"
        ).fetchall()
        assert rows == [("ada",), ("barbara",)]

    def test_order_by_qualified_column_of_projected_item(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT u.name FROM users u WHERE u.age IS NOT NULL ORDER BY u.name DESC"
        ).fetchall()
        assert rows == [("grace",), ("barbara",), ("ada",)]


class TestJoins:
    def test_inner_join(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT u.name, o.item FROM users u JOIN orders o ON u.id = o.user_id "
            "WHERE o.amount > 20 ORDER BY o.amount"
        ).fetchall()
        assert rows == [("ada", "teclado"), ("grace", "monitor")]

    def test_left_join_pads_with_nulls(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT u.name, o.id FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.id = 3"
        ).fetchall()
        assert rows == [("edsger", None)]

    def test_three_way_join(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE TABLE tags (user_id INTEGER, tag VARCHAR(20))")
        conn.execute("INSERT INTO tags (user_id, tag) VALUES (1, 'pionera'), (2, 'almirante')")
        rows = conn.execute(
            "SELECT u.name, t.tag, o.item FROM users u "
            "JOIN tags t ON u.id = t.user_id "
            "JOIN orders o ON u.id = o.user_id "
            "WHERE o.amount > 20 ORDER BY u.name"
        ).fetchall()
        assert rows == [("ada", "pionera", "teclado"), ("grace", "almirante", "monitor")]


class TestAggregates:
    def test_global_aggregates(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM orders"
        ).fetchall()
        assert rows == [(5, 148.9, 1.5, 99.9)]

    def test_group_by_with_having(self, conn: nanosql.Connection) -> None:
        rows = conn.execute(
            "SELECT user_id, COUNT(*) AS n, SUM(amount) AS total FROM orders "
            "GROUP BY user_id HAVING COUNT(*) > 1 ORDER BY total DESC"
        ).fetchall()
        assert rows == [(2, 2, 104.9), (1, 2, 42.5)]

    def test_avg_skips_nulls_and_count_star_does_not(self, conn: nanosql.Connection) -> None:
        rows = conn.execute("SELECT COUNT(*), AVG(age) FROM users").fetchall()
        assert rows[0][0] == 4
        assert rows[0][1] == pytest.approx((36 + 45 + 29) / 3)

    def test_count_distinct(self, conn: nanosql.Connection) -> None:
        rows = conn.execute("SELECT COUNT(DISTINCT user_id) FROM orders").fetchall()
        assert rows == [(3,)]

    def test_aggregate_over_empty_table(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE TABLE vacia (x INTEGER)")
        assert conn.execute("SELECT COUNT(*) FROM vacia").fetchall() == [(0,)]


class TestDistinct:
    def test_distinct_deduplicates_rows(self, conn: nanosql.Connection) -> None:
        rows = conn.execute("SELECT DISTINCT user_id FROM orders").fetchall()
        assert sorted(rows) == [(1,), (2,), (4,)]

    def test_distinct_treats_nulls_as_equal(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO orders (id, user_id, amount, item) VALUES (90, 4, 2.0, NULL)")
        rows = conn.execute("SELECT DISTINCT item FROM orders WHERE user_id = 4").fetchall()
        assert rows == [(None,)]

    def test_distinct_with_order_by_projected_column(self, conn: nanosql.Connection) -> None:
        rows = conn.execute("SELECT DISTINCT user_id FROM orders ORDER BY user_id DESC").fetchall()
        assert rows == [(4,), (2,), (1,)]

    def test_distinct_multi_column(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO orders (id, user_id, amount, item) VALUES (91, 1, 12.5, 'libro')")
        rows = conn.execute(
            "SELECT DISTINCT user_id, item FROM orders WHERE item = 'libro'"
        ).fetchall()
        assert rows == [(1, "libro")]


class TestDml:
    def test_insert_with_defaults(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO users (id, name) VALUES (10, 'alan')")
        rows = conn.execute("SELECT age, active FROM users WHERE id = 10").fetchall()
        assert rows == [(None, True)]

    def test_insert_without_column_list(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO users VALUES (11, 'lynn', 60, FALSE)")
        rows = conn.execute("SELECT name, active FROM users WHERE id = 11").fetchall()
        assert rows == [("lynn", False)]

    def test_update_with_expression_and_where(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("UPDATE orders SET amount = amount + 1 WHERE user_id = 1")
        assert cur.rowcount == 2
        rows = conn.execute("SELECT amount FROM orders WHERE user_id = 1 ORDER BY id").fetchall()
        assert rows == [(13.5,), (31.0,)]

    def test_update_all_rows(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("UPDATE users SET active = FALSE")
        assert cur.rowcount == 4

    def test_update_to_null(self, conn: nanosql.Connection) -> None:
        conn.execute("UPDATE users SET age = NULL WHERE id = 1")
        assert conn.execute("SELECT age FROM users WHERE id = 1").fetchall() == [(None,)]

    def test_delete_with_where_and_rowcount(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("DELETE FROM orders WHERE amount < 10")
        assert cur.rowcount == 2
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchall() == [(3,)]

    def test_delete_all(self, conn: nanosql.Connection) -> None:
        conn.execute("DELETE FROM orders")
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchall() == [(0,)]

    def test_insert_visible_within_own_transaction(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO users (id, name) VALUES (20, 'temporal')")
        assert conn.execute("SELECT name FROM users WHERE id = 20").fetchall() == [("temporal",)]
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM users WHERE id = 20").fetchall() == [(0,)]


class TestDdl:
    def test_create_table_if_not_exists(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER)")
        # La tabla original sigue intacta (4 columnas, no 1).
        assert len(conn.execute("SELECT * FROM users WHERE id = 1").fetchall()[0]) == 4

    def test_create_index_feeds_the_optimizer(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE INDEX ix_orders_user ON orders (user_id)")
        rows = conn.execute("SELECT id FROM orders WHERE user_id = 1 ORDER BY id").fetchall()
        assert rows == [(1,), (2,)]

    def test_create_unique_index_rejects_existing_duplicates(
        self, conn: nanosql.Connection
    ) -> None:
        with pytest.raises(nanosql.IntegrityError):
            conn.execute("CREATE UNIQUE INDEX ix_dup ON orders (user_id)")

    def test_unique_index_enforced_on_insert(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE UNIQUE INDEX ix_item ON orders (item)")
        with pytest.raises(nanosql.IntegrityError):
            conn.execute(
                "INSERT INTO orders (id, user_id, amount, item) VALUES (90, 1, 2.0, 'libro')"
            )
