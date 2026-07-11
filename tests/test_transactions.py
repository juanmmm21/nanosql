"""Semántica transaccional real vía mvcc-transaction-manager y
lock-manager-deadlock-detector."""

from __future__ import annotations

from pathlib import Path

import pytest

import nanosql
from nanosql import (
    OperationalError,
    ProgrammingError,
    TransactionConflictError,
)


@pytest.fixture()
def db(tmp_path: Path) -> Path:
    path = tmp_path / "txn.db"
    conn = nanosql.connect(path)
    conn.execute("CREATE TABLE cuentas (id INTEGER PRIMARY KEY, saldo INTEGER NOT NULL)")
    conn.execute("INSERT INTO cuentas (id, saldo) VALUES (1, 100), (2, 100)")
    conn.commit()
    conn.close()
    return path


class TestCommitRollback:
    def test_uncommitted_writes_invisible_to_other_connection(self, db: Path) -> None:
        a = nanosql.connect(db)
        b = nanosql.connect(db)
        a.execute("UPDATE cuentas SET saldo = 0 WHERE id = 1")
        assert b.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(100,)]
        a.commit()
        b.commit()  # snapshot nuevo en la siguiente sentencia
        assert b.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(0,)]
        a.close()
        b.close()

    def test_rollback_discards_writes(self, db: Path) -> None:
        conn = nanosql.connect(db)
        conn.execute("UPDATE cuentas SET saldo = 0 WHERE id = 1")
        conn.rollback()
        assert conn.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(100,)]
        conn.close()

    def test_close_with_open_transaction_rolls_back(self, db: Path) -> None:
        conn = nanosql.connect(db)
        conn.execute("UPDATE cuentas SET saldo = 0 WHERE id = 1")
        conn.close()
        check = nanosql.connect(db)
        assert check.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(100,)]
        check.close()

    def test_sql_level_begin_commit(self, db: Path) -> None:
        conn = nanosql.connect(db, autocommit=True)
        conn.execute("BEGIN")
        conn.execute("UPDATE cuentas SET saldo = 50 WHERE id = 1")
        conn.execute("UPDATE cuentas SET saldo = 150 WHERE id = 2")
        conn.execute("COMMIT")
        rows = conn.execute("SELECT saldo FROM cuentas ORDER BY id").fetchall()
        assert rows == [(50,), (150,)]
        conn.close()

    def test_sql_level_rollback(self, db: Path) -> None:
        conn = nanosql.connect(db, autocommit=True)
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DELETE FROM cuentas")
        conn.execute("ROLLBACK")
        assert conn.execute("SELECT COUNT(*) FROM cuentas").fetchall() == [(2,)]
        conn.close()

    def test_begin_inside_transaction_fails(self, db: Path) -> None:
        conn = nanosql.connect(db)
        conn.execute("SELECT COUNT(*) FROM cuentas")  # abre transacción implícita
        with pytest.raises(ProgrammingError):
            conn.execute("BEGIN")
        conn.close()

    def test_commit_without_transaction_fails_as_statement(self, db: Path) -> None:
        conn = nanosql.connect(db)
        with pytest.raises(ProgrammingError):
            conn.execute("COMMIT")
        conn.close()

    def test_autocommit_persists_each_statement(self, db: Path) -> None:
        conn = nanosql.connect(db, autocommit=True)
        conn.execute("UPDATE cuentas SET saldo = 7 WHERE id = 1")
        conn.close()
        check = nanosql.connect(db)
        assert check.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(7,)]
        check.close()


class TestConflicts:
    def test_first_committer_wins(self, db: Path) -> None:
        a = nanosql.connect(db)
        b = nanosql.connect(db)
        a.execute("UPDATE cuentas SET saldo = 10 WHERE id = 1")
        b.execute("UPDATE cuentas SET saldo = 20 WHERE id = 1")
        a.commit()
        with pytest.raises(TransactionConflictError):
            b.commit()
        # El perdedor puede reintentar y ganar.
        b.execute("UPDATE cuentas SET saldo = 20 WHERE id = 1")
        b.commit()
        assert a.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(20,)]
        a.close()
        b.close()

    def test_read_committed_does_not_conflict_on_blind_writes(self, db: Path) -> None:
        a = nanosql.connect(db, isolation_level="read_committed")
        b = nanosql.connect(db, isolation_level="read_committed")
        a.execute("UPDATE cuentas SET saldo = 1 WHERE id = 1")
        b.execute("UPDATE cuentas SET saldo = 2 WHERE id = 1")
        a.commit()
        b.commit()  # last-writer-wins, sin excepción
        assert a.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(2,)]
        a.close()
        b.close()

    def test_serializable_detects_write_skew(self, db: Path) -> None:
        # Escenario clásico de los dos médicos de guardia: cada transacción
        # comprueba que el total de saldos es suficiente y descuenta de una
        # cuenta distinta. En SERIALIZABLE una de las dos debe abortar.
        a = nanosql.connect(db, isolation_level="serializable")
        b = nanosql.connect(db, isolation_level="serializable")
        assert a.execute("SELECT SUM(saldo) FROM cuentas").fetchall() == [(200,)]
        assert b.execute("SELECT SUM(saldo) FROM cuentas").fetchall() == [(200,)]
        a.execute("UPDATE cuentas SET saldo = 0 WHERE id = 1")
        b.execute("UPDATE cuentas SET saldo = 0 WHERE id = 2")
        a.commit()
        with pytest.raises(TransactionConflictError):
            b.commit()
        a.close()
        b.close()

    def test_repeatable_read_allows_write_skew(self, db: Path) -> None:
        a = nanosql.connect(db, isolation_level="repeatable_read")
        b = nanosql.connect(db, isolation_level="repeatable_read")
        a.execute("SELECT SUM(saldo) FROM cuentas").fetchall()
        b.execute("SELECT SUM(saldo) FROM cuentas").fetchall()
        a.execute("UPDATE cuentas SET saldo = 0 WHERE id = 1")
        b.execute("UPDATE cuentas SET saldo = 0 WHERE id = 2")
        a.commit()
        b.commit()  # filas distintas: REPEATABLE_READ lo permite
        a.close()
        b.close()


class TestIsolationLevels:
    def test_repeatable_read_keeps_snapshot(self, db: Path) -> None:
        reader = nanosql.connect(db, isolation_level="repeatable_read")
        assert reader.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(100,)]
        writer = nanosql.connect(db)
        writer.execute("UPDATE cuentas SET saldo = 55 WHERE id = 1")
        writer.commit()
        writer.close()
        assert reader.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(100,)]
        reader.commit()
        assert reader.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(55,)]
        reader.close()

    def test_read_committed_sees_new_commits(self, db: Path) -> None:
        reader = nanosql.connect(db, isolation_level="read_committed")
        assert reader.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(100,)]
        writer = nanosql.connect(db)
        writer.execute("UPDATE cuentas SET saldo = 55 WHERE id = 1")
        writer.commit()
        writer.close()
        assert reader.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(55,)]
        reader.close()

    def test_phantom_reads_prevented_in_repeatable_read(self, db: Path) -> None:
        reader = nanosql.connect(db, isolation_level="repeatable_read")
        assert reader.execute("SELECT COUNT(*) FROM cuentas").fetchall() == [(2,)]
        writer = nanosql.connect(db)
        writer.execute("INSERT INTO cuentas (id, saldo) VALUES (3, 1)")
        writer.commit()
        writer.close()
        assert reader.execute("SELECT COUNT(*) FROM cuentas").fetchall() == [(2,)]
        reader.close()


class TestDdlLocking:
    def test_ddl_waits_for_readers_and_times_out(self, db: Path) -> None:
        # `reader` mantiene un lock SHARED sobre `cuentas` en su transacción
        # abierta; el DDL necesita EXCLUSIVE y debe agotar su timeout.
        reader = nanosql.connect(db, lock_timeout=0.2)
        reader.execute("SELECT COUNT(*) FROM cuentas")
        ddl = nanosql.connect(db, lock_timeout=0.2)
        with pytest.raises(OperationalError):
            ddl.execute("CREATE INDEX ix_saldo ON cuentas (saldo)")
        reader.commit()  # libera el SHARED
        ddl.execute("CREATE INDEX ix_saldo ON cuentas (saldo)")
        reader.close()
        ddl.close()

    def test_ddl_implicitly_commits_own_transaction(self, db: Path) -> None:
        conn = nanosql.connect(db)
        conn.execute("UPDATE cuentas SET saldo = 33 WHERE id = 1")
        # El DDL confirma la transacción abierta (documentado, estilo MySQL):
        # sin ese commit implícito se auto-bloquearía contra su propio SHARED.
        conn.execute("CREATE INDEX ix_mid_txn ON cuentas (saldo)")
        conn.close()
        check = nanosql.connect(db)
        assert check.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(33,)]
        check.close()

    def test_conflict_aborts_transaction_and_connection_recovers(self, db: Path) -> None:
        a = nanosql.connect(db)
        b = nanosql.connect(db)
        a.execute("UPDATE cuentas SET saldo = 1 WHERE id = 1")
        b.execute("UPDATE cuentas SET saldo = 2 WHERE id = 1")
        a.commit()
        with pytest.raises(TransactionConflictError):
            b.commit()
        # Tras el conflicto la conexión b queda utilizable sin rollback manual.
        assert b.execute("SELECT saldo FROM cuentas WHERE id = 1").fetchall() == [(1,)]
        a.close()
        b.close()
