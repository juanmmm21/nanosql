"""Superficie DB-API del driver: connect, Connection, Cursor."""

from __future__ import annotations

from pathlib import Path

import pytest

import nanosql
from nanosql import InterfaceError, ProgrammingError


@pytest.fixture()
def conn() -> nanosql.Connection:  # type: ignore[misc]
    connection = nanosql.connect(":memory:")
    connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name VARCHAR(20), price FLOAT)")
    connection.execute(
        "INSERT INTO items (id, name, price) VALUES"
        " (1, 'pan', 1.2), (2, 'sal', 0.8), (3, 'vino', 7.5)"
    )
    yield connection
    connection.close()


class TestModuleSurface:
    def test_dbapi_globals(self) -> None:
        assert nanosql.apilevel == "2.0"
        assert nanosql.threadsafety == 1

    def test_error_hierarchy_shape(self) -> None:
        assert issubclass(nanosql.ProgrammingError, nanosql.DatabaseError)
        assert issubclass(nanosql.TransactionConflictError, nanosql.OperationalError)
        assert issubclass(nanosql.DatabaseError, nanosql.NanosqlError)


class TestCursor:
    def test_fetchone_and_exhaustion(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("SELECT name FROM items ORDER BY name")
        assert cur.fetchone() == ("pan",)
        assert cur.fetchone() == ("sal",)
        assert cur.fetchone() == ("vino",)
        assert cur.fetchone() is None

    def test_fetchmany_respects_size_and_arraysize(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("SELECT id FROM items ORDER BY id")
        assert cur.fetchmany(2) == [(1,), (2,)]
        assert cur.fetchmany(2) == [(3,)]
        cur.execute("SELECT id FROM items ORDER BY id")
        cur.arraysize = 3
        assert len(cur.fetchmany()) == 3

    def test_iteration_protocol(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("SELECT id FROM items ORDER BY id")
        assert [row[0] for row in cur] == [1, 2, 3]

    def test_description_only_for_select(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("SELECT id, name FROM items ORDER BY id")
        assert cur.description is not None
        assert [d[0] for d in cur.description] == ["id", "name"]
        assert [d[1] for d in cur.description] == ["INTEGER", "STRING"]
        cur.execute("INSERT INTO items (id, name, price) VALUES (9, 'te', 2.0)")
        assert cur.description is None

    def test_rowcount(self, conn: nanosql.Connection) -> None:
        cur = conn.execute("UPDATE items SET price = 0.0 WHERE id > 1")
        assert cur.rowcount == 2
        cur.execute("DELETE FROM items WHERE id = 1")
        assert cur.rowcount == 1

    def test_execute_returns_cursor_for_chaining(self, conn: nanosql.Connection) -> None:
        rows = conn.execute("SELECT COUNT(*) FROM items").fetchall()
        assert rows == [(3,)]

    def test_closed_cursor_raises_interface_error(self, conn: nanosql.Connection) -> None:
        cur = conn.cursor()
        cur.close()
        with pytest.raises(InterfaceError):
            cur.execute("SELECT 1 FROM items")

    def test_fetch_without_select_raises(self, conn: nanosql.Connection) -> None:
        cur = conn.cursor()
        with pytest.raises(InterfaceError):
            cur.fetchall()

    def test_executescript_runs_multiple_statements(self, conn: nanosql.Connection) -> None:
        cur = conn.cursor()
        cur.executescript(
            "INSERT INTO items (id, name, price) VALUES (10, 'a', 1.0);"
            "INSERT INTO items (id, name, price) VALUES (11, 'b', 2.0);"
            "SELECT COUNT(*) FROM items;"
        )
        assert cur.fetchall() == [(5,)]


class TestConnectionLifecycle:
    def test_closed_connection_raises_interface_error(self) -> None:
        connection = nanosql.connect(":memory:")
        connection.close()
        with pytest.raises(InterfaceError):
            connection.execute("SELECT 1 FROM x")

    def test_close_is_idempotent(self) -> None:
        connection = nanosql.connect(":memory:")
        connection.close()
        connection.close()

    def test_context_manager_commits_on_success(self, tmp_path: Path) -> None:
        db = tmp_path / "ctx.db"
        with nanosql.connect(db) as connection:
            connection.execute("CREATE TABLE t (x INTEGER)")
            connection.execute("INSERT INTO t (x) VALUES (1)")
        check = nanosql.connect(db)
        assert check.execute("SELECT COUNT(*) FROM t").fetchall() == [(1,)]
        check.close()

    def test_context_manager_rolls_back_on_error(self, tmp_path: Path) -> None:
        db = tmp_path / "ctx.db"
        with nanosql.connect(db) as connection:
            connection.execute("CREATE TABLE t (x INTEGER)")
        with pytest.raises(RuntimeError):
            with nanosql.connect(db) as connection:
                connection.execute("INSERT INTO t (x) VALUES (1)")
                raise RuntimeError("boom")
        check = nanosql.connect(db)
        assert check.execute("SELECT COUNT(*) FROM t").fetchall() == [(0,)]
        check.close()

    def test_two_connections_share_one_engine(self, tmp_path: Path) -> None:
        db = tmp_path / "shared.db"
        a = nanosql.connect(db)
        b = nanosql.connect(db)
        a.execute("CREATE TABLE t (x INTEGER)")
        a.execute("INSERT INTO t (x) VALUES (7)")
        a.commit()
        assert b.execute("SELECT x FROM t").fetchall() == [(7,)]
        a.close()
        b.close()

    def test_memory_databases_are_private(self) -> None:
        a = nanosql.connect(":memory:")
        b = nanosql.connect(":memory:")
        a.execute("CREATE TABLE t (x INTEGER)")
        with pytest.raises(ProgrammingError):
            b.execute("SELECT x FROM t")
        a.close()
        b.close()

    def test_unknown_isolation_level_rejected(self) -> None:
        with pytest.raises(ProgrammingError):
            nanosql.connect(":memory:", isolation_level="chaotic")
