"""El mismo SQL debe comportarse igual sobre los dos motores de storage:
el backend es intercambiable por diseño, nunca observable en los resultados.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import nanosql

BACKENDS = ["btree", "lsm"]


@pytest.fixture(params=BACKENDS)
def conn(request: pytest.FixtureRequest, tmp_path: Path) -> nanosql.Connection:  # type: ignore[misc]
    connection = nanosql.connect(tmp_path / f"db-{request.param}", backend=request.param)
    yield connection
    connection.close()


class TestBackendParity:
    def test_full_workload_matches(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, grupo INTEGER NOT NULL, v FLOAT)")
        values = ", ".join(f"({n}, {n % 3}, {n * 1.5})" for n in range(60))
        conn.execute(f"INSERT INTO t (id, grupo, v) VALUES {values}")
        conn.execute("UPDATE t SET v = v + 100 WHERE grupo = 1")
        conn.execute("DELETE FROM t WHERE grupo = 2")
        conn.commit()

        rows = conn.execute(
            "SELECT grupo, COUNT(*) AS n, SUM(v) AS total FROM t GROUP BY grupo ORDER BY grupo"
        ).fetchall()
        assert [(g, n) for g, n, _ in rows] == [(0, 20), (1, 20)]
        totals = {g: total for g, _, total in rows}
        assert totals[0] == pytest.approx(sum(n * 1.5 for n in range(60) if n % 3 == 0))
        assert totals[1] == pytest.approx(sum(n * 1.5 + 100 for n in range(60) if n % 3 == 1))

    def test_persistence_across_reopen(self, tmp_path: Path) -> None:
        for backend in BACKENDS:
            db = tmp_path / f"persist-{backend}"
            conn = nanosql.connect(db, backend=backend)
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR(10))")
            conn.execute("INSERT INTO t (id, v) VALUES (1, 'hola')")
            conn.commit()
            conn.close()

            reopened = nanosql.connect(db)  # el backend se recuerda en el catálogo
            assert reopened.backend == backend
            assert reopened.execute("SELECT v FROM t").fetchall() == [("hola",)]
            reopened.close()

    def test_transaction_conflicts_behave_identically(self, tmp_path: Path) -> None:
        for backend in BACKENDS:
            db = tmp_path / f"conflict-{backend}"
            setup = nanosql.connect(db, backend=backend)
            setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            setup.execute("INSERT INTO t (id, v) VALUES (1, 0)")
            setup.commit()

            a = nanosql.connect(db)
            b = nanosql.connect(db)
            a.execute("UPDATE t SET v = 1 WHERE id = 1")
            b.execute("UPDATE t SET v = 2 WHERE id = 1")
            a.commit()
            with pytest.raises(nanosql.TransactionConflictError):
                b.commit()
            for c in (setup, a, b):
                c.close()

    def test_memory_mode_requires_btree(self) -> None:
        with pytest.raises(nanosql.NotSupportedError):
            nanosql.connect(":memory:", backend="lsm")
