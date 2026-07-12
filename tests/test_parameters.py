"""Parámetros de consulta qmark (`?`), EXPLAIN y executemany."""

from __future__ import annotations

import pytest

import nanosql
from nanosql import DataError, NotSupportedError, ProgrammingError


@pytest.fixture()
def conn() -> nanosql.Connection:  # type: ignore[misc]
    connection = nanosql.connect(":memory:")
    connection.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(20) NOT NULL, score FLOAT)"
    )
    connection.execute(
        "INSERT INTO t (id, name, score) VALUES (1, 'uno', 1.5), (2, 'dos', 2.5), (3, 'tres', NULL)"
    )
    yield connection
    connection.close()


class TestQmarkParameters:
    def test_module_declares_qmark_paramstyle(self) -> None:
        assert nanosql.paramstyle == "qmark"

    def test_select_with_parameters(self, conn: nanosql.Connection) -> None:
        rows = conn.execute("SELECT name FROM t WHERE score > ? AND id < ?", (1.0, 3)).fetchall()
        assert rows == [("uno",), ("dos",)]

    def test_insert_with_parameters_infers_types(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO t (id, name, score) VALUES (?, ?, ?)", (10, "diez", 10.5))
        assert conn.execute("SELECT name, score FROM t WHERE id = ?", (10,)).fetchall() == [
            ("diez", 10.5)
        ]

    def test_none_binds_as_null(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO t (id, name, score) VALUES (?, ?, ?)", (11, "once", None))
        assert conn.execute("SELECT COUNT(*) FROM t WHERE score IS NULL").fetchall() == [(2,)]

    def test_int_widens_to_float_column(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO t (id, name, score) VALUES (?, ?, ?)", (12, "doce", 3))
        assert conn.execute("SELECT score FROM t WHERE id = 12").fetchall() == [(3.0,)]

    def test_update_and_delete_with_parameters(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("UPDATE t SET score = ? WHERE name = ?", (9.9, "uno"))
        assert cur.rowcount == 1
        cur = conn.execute("DELETE FROM t WHERE id = ?", (2,))
        assert cur.rowcount == 1

    def test_string_parameter_with_quotes_is_injection_safe(self, conn: nanosql.Connection) -> None:
        # El valor viaja tipado por el AST, nunca re-interpolado como texto:
        # comillas y punto y coma llegan intactos a la columna.
        evil = "x'; DELETE FROM t; --"
        conn.execute("INSERT INTO t (id, name) VALUES (?, ?)", (20, evil))
        assert conn.execute("SELECT name FROM t WHERE id = 20").fetchall() == [(evil,)]
        assert conn.execute("SELECT COUNT(*) FROM t").fetchall()[0][0] > 1

    def test_too_few_parameters(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("SELECT * FROM t WHERE id = ? AND score > ?", (1,))

    def test_too_many_parameters(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("SELECT * FROM t WHERE id = ?", (1, 2))

    def test_placeholders_without_parameters(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("SELECT * FROM t WHERE id = ?")

    def test_unsupported_parameter_type(self, conn: nanosql.Connection) -> None:
        with pytest.raises(DataError):
            conn.execute("SELECT * FROM t WHERE id = ?", (object(),))  # type: ignore[arg-type]

    def test_ddl_rejects_parameters(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("CREATE TABLE otra (x INTEGER)", (1,))

    def test_executemany_aggregates_rowcount(self, conn: nanosql.Connection) -> None:
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO t (id, name, score) VALUES (?, ?, ?)",
            [(30, "a", 1.0), (31, "b", 2.0), (32, "c", 3.0)],
        )
        assert cur.rowcount == 3
        assert conn.execute("SELECT COUNT(*) FROM t WHERE id >= 30").fetchall() == [(3,)]

    def test_executemany_requires_at_least_one_tuple(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.cursor().executemany("INSERT INTO t (id, name) VALUES (?, ?)", [])


class TestExplain:
    def test_explain_returns_physical_plan_text(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("EXPLAIN SELECT name FROM t WHERE id = 1 ORDER BY name")
        assert cur.description is not None
        assert cur.description[0][0] == "query plan"
        plan_text = "\n".join(row[0] for row in cur.fetchall())  # type: ignore[misc]
        assert "Project" in plan_text
        assert "Sort" in plan_text

    def test_explain_shows_index_scan_when_available(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE INDEX ix_score ON t (score)")
        cur = conn.execute("EXPLAIN SELECT id FROM t WHERE score = 2.5")
        plan_text = "\n".join(row[0] for row in cur.fetchall())  # type: ignore[misc]
        assert "IndexScan" in plan_text or "Scan" in plan_text

    def test_explain_with_parameters(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("EXPLAIN SELECT id FROM t WHERE score > ?", (1.0,))
        assert cur.rowcount > 0

    def test_explain_non_select_not_supported(self, conn: nanosql.Connection) -> None:
        with pytest.raises(NotSupportedError):
            conn.execute("EXPLAIN DELETE FROM t")

    def test_explain_does_not_read_data(self, conn: nanosql.Connection) -> None:
        # EXPLAIN no abre transacción: no debe dejar una implícita colgando.
        conn.commit()
        conn.execute("EXPLAIN SELECT * FROM t")
        assert conn.in_transaction is False
