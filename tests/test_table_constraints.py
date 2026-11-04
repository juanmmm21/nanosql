"""Restricciones de tabla en CREATE TABLE (la forma que emiten los ORMs)."""

from __future__ import annotations

import pytest

import nanosql
from nanosql import IntegrityError, ProgrammingError


@pytest.fixture()
def conn() -> nanosql.Connection:  # type: ignore[misc]
    connection = nanosql.connect(":memory:", autocommit=True)
    yield connection
    connection.close()


def test_single_column_table_pk_equals_column_pk(conn: nanosql.Connection) -> None:
    conn.execute("CREATE TABLE u (id INTEGER NOT NULL, name VARCHAR(50), PRIMARY KEY (id))")
    column = conn._require_engine().catalog.table("u").column("id")
    assert column.primary_key and column.not_null
    conn.execute("INSERT INTO u (id, name) VALUES (1, 'a')")
    with pytest.raises(IntegrityError) as info:
        conn.execute("INSERT INTO u (id, name) VALUES (1, 'b')")
    assert info.value.sqlstate == "23505"
    with pytest.raises(IntegrityError) as null_info:
        conn.execute("INSERT INTO u (name) VALUES ('c')")
    assert null_info.value.sqlstate == "23502"


def test_composite_pk_becomes_not_null_columns_and_unique_index(
    conn: nanosql.Connection,
) -> None:
    conn.execute("CREATE TABLE m (a INTEGER, b INTEGER, v TEXT, PRIMARY KEY (a, b))")
    catalog = conn._require_engine().catalog
    assert catalog.table("m").column("a").not_null
    assert catalog.table("m").column("b").not_null
    (index,) = catalog.indexes_of("m")
    assert (index.name, index.columns, index.unique) == ("m_pkey", ("a", "b"), True)
    conn.execute("INSERT INTO m (a, b, v) VALUES (1, 1, 'x'), (1, 2, 'y')")
    with pytest.raises(IntegrityError):
        conn.execute("INSERT INTO m (a, b, v) VALUES (1, 2, 'z')")


def test_unique_constraints_named_and_implicit(conn: nanosql.Connection) -> None:
    conn.execute(
        "CREATE TABLE t (a INTEGER, b INTEGER, c TEXT, UNIQUE (c), CONSTRAINT ab UNIQUE (a, b))"
    )
    catalog = conn._require_engine().catalog
    assert catalog.table("t").column("c").unique
    assert [i.name for i in catalog.indexes_of("t")] == ["ab"]
    conn.execute("INSERT INTO t (a, b, c) VALUES (1, 1, 'x')")
    with pytest.raises(IntegrityError):
        conn.execute("INSERT INTO t (a, b, c) VALUES (1, 1, 'y')")
    with pytest.raises(IntegrityError):
        conn.execute("INSERT INTO t (a, b, c) VALUES (2, 2, 'x')")


def test_constraint_errors(conn: nanosql.Connection) -> None:
    with pytest.raises(ProgrammingError) as missing:
        conn.execute("CREATE TABLE t (a INTEGER, PRIMARY KEY (zz))")
    assert missing.value.sqlstate == "42703"
    with pytest.raises(ProgrammingError) as double:
        conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER, PRIMARY KEY (b))")
    assert double.value.sqlstate == "42P16"
    # nada quedó a medias en el catálogo tras los errores
    assert "t" not in conn._require_engine().catalog.tables


def test_constraints_survive_reopen(tmp_path: object) -> None:
    from pathlib import Path

    assert isinstance(tmp_path, Path)
    path = tmp_path / "c.db"
    with nanosql.connect(path) as conn:
        conn.execute("CREATE TABLE m (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
    with nanosql.connect(path) as conn:
        conn.execute("INSERT INTO m (a, b) VALUES (1, 1)")
        with pytest.raises(IntegrityError):
            conn.execute("INSERT INTO m (a, b) VALUES (1, 1)")
