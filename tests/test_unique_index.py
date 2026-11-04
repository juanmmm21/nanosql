"""Comprobaciones de unicidad apoyadas en `UniqueKeyIndex` (en vez de
decodificar la tabla entera en cada sentencia y en cada commit): mismas
garantías, coste proporcional a las filas escritas."""

from __future__ import annotations

from pathlib import Path

import pytest

import nanosql
from nanosql.errors import IntegrityError
from nanosql.uniqueness import UniqueKeyIndex


def test_index_tracks_owners_and_ignores_null_keys() -> None:
    index = UniqueKeyIndex.build([("k",), ("a", "b")], [(1, {"k": 10, "a": 1, "b": None})])
    assert index.owner(("k",), (10,)) == 1
    assert index.owner(("a", "b"), (1, None)) is None  # keys with NULL never collide
    index.replace(1, {"k": 11, "a": 1, "b": 2})
    assert index.owner(("k",), (10,)) is None
    assert index.owner(("k",), (11,)) == 1
    assert index.owner(("a", "b"), (1, 2)) == 1
    index.replace(1, None)
    assert index.owner(("k",), (11,)) is None


@pytest.fixture
def db(tmp_path: Path) -> str:
    path = str(tmp_path / "db")
    conn = nanosql.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
    conn.execute("INSERT INTO t (id, code) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    conn.commit()
    conn.close()
    return path


def test_duplicate_inside_one_statement_and_against_existing_rows(db: str) -> None:
    conn = nanosql.connect(db)
    with pytest.raises(IntegrityError):
        conn.execute("INSERT INTO t (id, code) VALUES (10, 'x'), (11, 'x')")
    conn.rollback()
    with pytest.raises(IntegrityError):
        conn.execute("INSERT INTO t (id, code) VALUES (12, 'b')")
    conn.rollback()
    conn.close()


def test_shifting_every_key_by_one_in_a_single_update_is_allowed(db: str) -> None:
    conn = nanosql.connect(db)
    conn.execute("UPDATE t SET id = id + 1")
    conn.commit()
    assert conn.execute("SELECT id FROM t ORDER BY id").fetchall() == [(2,), (3,), (4,)]
    with pytest.raises(IntegrityError):
        conn.execute("UPDATE t SET id = 4 WHERE id = 2")
    conn.close()


def test_read_committed_statement_sees_a_key_committed_meanwhile(db: str) -> None:
    reader = nanosql.connect(db, isolation_level="read_committed")
    writer = nanosql.connect(db)
    reader.execute("INSERT INTO t (id, code) VALUES (20, 'r')")  # builds its key index
    writer.execute("INSERT INTO t (id, code) VALUES (21, 'w')")
    writer.commit()
    with pytest.raises(IntegrityError):  # stale index would let this through
        reader.execute("INSERT INTO t (id, code) VALUES (22, 'w')")
    reader.rollback()
    reader.close()
    writer.close()


def test_repeatable_read_duplicate_is_caught_at_commit(db: str) -> None:
    reader = nanosql.connect(db, isolation_level="repeatable_read")
    writer = nanosql.connect(db)
    reader.execute("INSERT INTO t (id, code) VALUES (30, 'r')")
    writer.execute("INSERT INTO t (id, code) VALUES (31, 'w')")
    writer.commit()
    reader.execute("INSERT INTO t (id, code) VALUES (32, 'w')")  # invisible to its snapshot
    with pytest.raises(IntegrityError):
        reader.commit()
    reader.close()
    writer.close()


def test_committed_index_follows_deletes_and_new_unique_indexes(db: str) -> None:
    conn = nanosql.connect(db)
    conn.execute("DELETE FROM t WHERE code = 'b'")
    conn.commit()
    conn.execute("INSERT INTO t (id, code) VALUES (2, 'b')")  # free again after the delete
    conn.commit()
    conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT)")
    conn.execute("INSERT INTO u (id, email) VALUES (1, 'x@y'), (2, 'z@y')")
    conn.commit()
    conn.execute("CREATE UNIQUE INDEX u_email ON u (email)")
    with pytest.raises(IntegrityError):
        conn.execute("INSERT INTO u (id, email) VALUES (3, 'x@y')")
    conn.rollback()
    conn.close()
