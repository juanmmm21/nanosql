"""Persistencia y recuperación: reaperturas limpias y crashes simulados.

El crash se simula copiando el directorio de la base de datos *mientras la
conexión sigue abierta* (el estado copiado es exactamente lo que habría en
disco si el proceso muriese en ese instante) o destruyendo el archivo de
datos para forzar a la recuperación a reconstruirlo desde el WAL.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from mvcc_transaction_manager import CommitSeq, RowVersion, TransactionId

import nanosql
from nanosql.durability import WalDurability, recover_storage
from nanosql.rowstore import DurableRowStore, make_row_id
from nanosql.storage import BPlusTreeBackend


class TestCleanReopen:
    def test_data_survives_close_and_reopen(self, tmp_path: Path) -> None:
        db = tmp_path / "p.db"
        conn = nanosql.connect(db)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR(10))")
        conn.execute("INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')")
        conn.commit()
        conn.close()

        conn = nanosql.connect(db)
        assert conn.execute("SELECT v FROM t ORDER BY id").fetchall() == [("a",), ("b",)]
        conn.close()

    def test_catalog_survives_reopen(self, tmp_path: Path) -> None:
        db = tmp_path / "p.db"
        conn = nanosql.connect(db)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR(10) DEFAULT 'x')")
        conn.execute("CREATE UNIQUE INDEX ix_v ON t (v)")
        conn.close()

        conn = nanosql.connect(db)
        conn.execute("INSERT INTO t (id) VALUES (1)")
        assert conn.execute("SELECT v FROM t").fetchall() == [("x",)]  # DEFAULT recordado
        conn.close()

    def test_rowids_do_not_collide_after_reopen(self, tmp_path: Path) -> None:
        db = tmp_path / "p.db"
        conn = nanosql.connect(db)
        conn.execute("CREATE TABLE t (v INTEGER)")
        conn.execute("INSERT INTO t (v) VALUES (1), (2), (3)")
        conn.commit()
        conn.close()

        conn = nanosql.connect(db)
        conn.execute("INSERT INTO t (v) VALUES (4)")
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM t").fetchall() == [(4,)]
        conn.close()

    def test_commit_seq_restored_visibility_after_reopen(self, tmp_path: Path) -> None:
        # Si el horizonte MVCC no se restaurase al reabrir, ninguna fila
        # antigua sería visible (commit_seq > horizonte 0) — regresión del
        # workaround documentado en engine.py.
        db = tmp_path / "p.db"
        conn = nanosql.connect(db)
        conn.execute("CREATE TABLE t (v INTEGER)")
        for n in range(5):
            conn.execute(f"INSERT INTO t (v) VALUES ({n})")
            conn.commit()
        conn.close()

        conn = nanosql.connect(db)
        assert conn.execute("SELECT COUNT(*) FROM t").fetchall() == [(5,)]
        conn.execute("INSERT INTO t (v) VALUES (99)")
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM t").fetchall() == [(6,)]
        conn.close()

    def test_backend_mismatch_rejected(self, tmp_path: Path) -> None:
        import pytest

        db = tmp_path / "p.db"
        conn = nanosql.connect(db, backend="btree")
        conn.execute("CREATE TABLE t (v INTEGER)")
        conn.close()
        with pytest.raises(nanosql.NanosqlError):
            nanosql.connect(db, backend="lsm")


class TestCrashRecovery:
    def _snapshot_while_open(self, live: Path, copy_to: Path) -> None:
        """Copia el estado en disco de una base de datos aún abierta: el
        equivalente a que el proceso muera en este preciso instante."""
        shutil.copytree(live, copy_to)

    def test_committed_data_survives_crash_without_close(self, tmp_path: Path) -> None:
        db = tmp_path / "live.db"
        crashed = tmp_path / "crashed.db"
        conn = nanosql.connect(db)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR(10))")
        conn.execute("INSERT INTO t (id, v) VALUES (1, 'durable')")
        conn.commit()
        self._snapshot_while_open(db, crashed)  # crash: sin close(), sin checkpoint
        conn.close()

        recovered = nanosql.connect(crashed)
        assert recovered.execute("SELECT v FROM t WHERE id = 1").fetchall() == [("durable",)]
        recovered.close()

    def test_wal_alone_rebuilds_destroyed_data_file(self, tmp_path: Path) -> None:
        # Prueba dura del invariante write-ahead: se destruye el archivo de
        # datos del B+Tree y toda la base de datos debe reconstruirse desde
        # el WAL (que retiene la historia completa hasta el checkpoint).
        db = tmp_path / "live.db"
        crashed = tmp_path / "crashed.db"
        conn = nanosql.connect(db)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR(10))")
        conn.execute("INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')")
        conn.commit()
        conn.execute("UPDATE t SET v = 'z' WHERE id = 2")
        conn.commit()
        self._snapshot_while_open(db, crashed)
        conn.close()

        (crashed / "data.btree").unlink()  # página cero incluida: no queda nada
        recovered = nanosql.connect(crashed)
        assert recovered.execute("SELECT v FROM t ORDER BY id").fetchall() == [
            ("a",),
            ("z",),
        ]
        recovered.close()

    def test_uncommitted_wal_batch_is_undone(self, tmp_path: Path) -> None:
        # Simulación a nivel de unidad de un crash a mitad de commit: hay
        # registros de escritura en el WAL pero ningún COMMIT. La
        # recuperación debe deshacerlos (fase undo).
        wal_path = tmp_path / "wal.log"
        data_path = tmp_path / "data.btree"

        backend = BPlusTreeBackend(data_path)
        durability = WalDurability(wal_path)
        store = DurableRowStore(backend, durability)
        row = make_row_id("t", 1)

        committed = durability.begin()
        store.begin_logged_batch(committed)
        store.append_version(
            RowVersion(
                row_id=row, value=b"ok", created_by=TransactionId(1), commit_seq=CommitSeq(1)
            )
        )
        store.end_logged_batch()
        durability.commit(committed)

        half_done = durability.begin()
        store.begin_logged_batch(half_done)
        store.append_version(
            RowVersion(
                row_id=row, value=b"perdido", created_by=TransactionId(2), commit_seq=CommitSeq(2)
            )
        )
        store.end_logged_batch()
        # crash: nunca llega durability.commit(half_done)
        backend.flush()
        backend.close()
        durability.close()

        fresh_backend = BPlusTreeBackend(data_path)
        report = recover_storage(wal_path, fresh_backend)
        assert report is not None
        assert report.undone_operations >= 1
        fresh_store = DurableRowStore(fresh_backend, None)
        versions = fresh_store.versions_of(row)
        assert [v.value for v in versions] == [b"ok"]
        fresh_backend.close()
