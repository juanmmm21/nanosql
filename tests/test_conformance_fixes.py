"""Semántica corregida a raíz del run de conformidad sqllogictest:
ORDER BY por posición, `*` en orden del FROM, nombres de salida repetidos,
división entera, predicados de join constantes, GROUP BY repetido,
`SELECT ALL`, `INSERT ... SELECT` y `DROP TABLE`."""

from __future__ import annotations

from pathlib import Path

import pytest

import nanosql
from nanosql import ProgrammingError


@pytest.fixture()
def conn() -> nanosql.Connection:  # type: ignore[misc]
    connection = nanosql.connect(":memory:", autocommit=True)
    connection.execute("CREATE TABLE a (x INTEGER, y INTEGER)")
    connection.execute("CREATE TABLE b (z INTEGER)")
    connection.execute("INSERT INTO a (x, y) VALUES (1, 30), (5, 10), (-3, 20)")
    connection.execute("INSERT INTO b (z) VALUES (7), (8)")
    yield connection
    connection.close()


def rows(conn: nanosql.Connection, sql: str) -> list[tuple[object, ...]]:
    return conn.execute(sql).fetchall()


def test_order_by_position(conn: nanosql.Connection) -> None:
    assert rows(conn, "SELECT x - y FROM a ORDER BY 1") == [(-29,), (-23,), (-5,)]
    assert rows(conn, "SELECT x, y FROM a ORDER BY 2 DESC") == [(1, 30), (-3, 20), (5, 10)]
    assert rows(conn, "SELECT * FROM a ORDER BY 1") == [(-3, 20), (1, 30), (5, 10)]
    with pytest.raises(ProgrammingError) as info:
        conn.execute("SELECT x FROM a ORDER BY 2")
    assert info.value.sqlstate == "42P10"


def test_star_follows_from_order_after_join_reordering(conn: nanosql.Connection) -> None:
    cur = conn.execute("SELECT * FROM a JOIN b ON 1 = 1 ORDER BY x, z")
    assert [d[0] for d in cur.description] == ["a.x", "a.y", "b.z"]
    assert cur.fetchall()[0] == (-3, 20, 7)
    assert rows(conn, "SELECT * FROM a GROUP BY y, x ORDER BY x") == [(-3, 20), (1, 30), (5, 10)]


def test_constant_join_predicates_are_applied(conn: nanosql.Connection) -> None:
    assert rows(conn, "SELECT * FROM a JOIN b ON 1 = 0") == []
    assert rows(conn, "SELECT * FROM a JOIN b ON NULL IS NOT NULL") == []
    assert len(rows(conn, "SELECT * FROM a JOIN b ON NULL IS NULL")) == 6


def test_repeated_output_names_and_group_keys(conn: nanosql.Connection) -> None:
    cur = conn.execute("SELECT 1 AS c, x AS c, x FROM a WHERE x = 5")
    assert [d[0] for d in cur.description] == ["c", "c", "x"]
    assert cur.fetchall() == [(1, 5, 5)]
    assert rows(conn, "SELECT x FROM a GROUP BY x, y, x ORDER BY x") == [(-3,), (1,), (5,)]


def test_integer_division_truncates(conn: nanosql.Connection) -> None:
    assert rows(conn, "SELECT y / x, y % x FROM a ORDER BY x") == [(-6, 2), (30, 0), (2, 0)]
    conn.execute("CREATE TABLE k (v INTEGER, f FLOAT)")
    conn.execute("INSERT INTO k (v, f) VALUES (-7 / 2, 7 / 2.0)")
    assert rows(conn, "SELECT v, f FROM k") == [(-3, 3.5)]


def test_select_all_quantifier(conn: nanosql.Connection) -> None:
    assert rows(conn, "SELECT ALL - x FROM a ORDER BY 1") == [(-5,), (-1,), (3,)]
    assert rows(conn, "SELECT COUNT(ALL x) FROM a") == [(3,)]


def test_insert_select(conn: nanosql.Connection) -> None:
    conn.execute("CREATE TABLE c (x INTEGER, y INTEGER DEFAULT 99, w INTEGER)")
    result = conn.execute("INSERT INTO c (w, x) SELECT y, x FROM a WHERE x > ?", (0,))
    assert result.rowcount == 2
    assert rows(conn, "SELECT x, y, w FROM c ORDER BY x") == [(1, 99, 30), (5, 99, 10)]
    conn.execute("INSERT INTO c SELECT x, y, x FROM a WHERE x < 0")
    assert rows(conn, "SELECT COUNT(*) FROM c") == [(3,)]
    with pytest.raises(ProgrammingError):
        conn.execute("INSERT INTO c (x) SELECT x, y FROM a")


def test_insert_select_is_transactional() -> None:
    conn = nanosql.connect(":memory:")
    conn.execute("CREATE TABLE s (v INTEGER)")
    conn.execute("CREATE TABLE t (v INTEGER)")
    conn.execute("INSERT INTO s (v) VALUES (1), (2)")
    conn.commit()
    conn.execute("INSERT INTO t SELECT v FROM s")
    conn.rollback()
    assert conn.execute("SELECT COUNT(*) FROM t").fetchall() == [(0,)]
    conn.close()


def test_drop_table(conn: nanosql.Connection) -> None:
    conn.execute("CREATE INDEX a_x ON a (x)")
    conn.execute("DROP TABLE a")
    with pytest.raises(ProgrammingError) as info:
        conn.execute("SELECT * FROM a")
    assert info.value.sqlstate == "42P01"
    assert "a_x" not in conn._require_engine().catalog.indexes
    conn.execute("DROP TABLE IF EXISTS a")
    with pytest.raises(ProgrammingError):
        conn.execute("DROP TABLE a")
    # recrear con el mismo nombre no resucita filas antiguas
    conn.execute("CREATE TABLE a (x INTEGER, y INTEGER)")
    assert rows(conn, "SELECT * FROM a") == []
    conn.execute("INSERT INTO a (x, y) VALUES (42, 0)")
    assert rows(conn, "SELECT x FROM a") == [(42,)]


@pytest.mark.parametrize("backend", ["btree", "lsm"])
def test_drop_table_survives_reopen(tmp_path: Path, backend: str) -> None:
    path = tmp_path / "d.db"
    with nanosql.connect(path, backend=backend) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO t (id) VALUES (1), (2)")
        conn.commit()
        conn.execute("DROP TABLE t")
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO t (id) VALUES (3)")
    with nanosql.connect(path) as conn:
        assert conn.execute("SELECT id FROM t").fetchall() == [(3,)]


def test_in_between_isnull_filters_apply_inside_joins(conn: nanosql.Connection) -> None:
    """Regresión del optimizador: en un join, estos filtros se descartaban."""
    base = "SELECT x FROM a JOIN b ON a.x + 6 = b.z WHERE "
    assert rows(conn, base + "a.y IN (30, 99) ORDER BY x") == [(1,)]
    assert rows(conn, base + "a.y BETWEEN 0 AND 15") == []
    assert rows(conn, base + "a.y IS NULL") == []
