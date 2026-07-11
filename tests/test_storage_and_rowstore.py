"""Contrato del backend clave-valor y del row store MVCC durable."""

from __future__ import annotations

from pathlib import Path

import pytest
from mvcc_transaction_manager import CommitSeq, RowId, RowVersion, TransactionId

from nanosql.rowstore import (
    DurableRowStore,
    make_row_id,
    split_row_id,
)
from nanosql.storage import BPlusTreeBackend


def _version(row_id: RowId, seq: int, value: bytes | None) -> RowVersion:
    return RowVersion(
        row_id=row_id, value=value, created_by=TransactionId(1), commit_seq=CommitSeq(seq)
    )


class TestBPlusTreeBackend:
    def test_put_get_overwrite(self, tmp_path: Path) -> None:
        backend = BPlusTreeBackend(tmp_path / "kv.db")
        backend.put(b"a", b"1")
        backend.put(b"a", b"2")
        assert backend.get(b"a") == b"2"
        assert backend.get(b"missing") is None
        backend.close()

    def test_delete_is_idempotent(self, tmp_path: Path) -> None:
        backend = BPlusTreeBackend(tmp_path / "kv.db")
        backend.put(b"a", b"1")
        backend.delete(b"a")
        backend.delete(b"a")  # segunda vez: no debe lanzar
        assert backend.get(b"a") is None
        backend.close()

    def test_scan_is_ordered_and_bounded(self, tmp_path: Path) -> None:
        backend = BPlusTreeBackend(tmp_path / "kv.db")
        for key in (b"c", b"a", b"b", b"d"):
            backend.put(key, key)
        assert [k for k, _ in backend.scan()] == [b"a", b"b", b"c", b"d"]
        assert [k for k, _ in backend.scan(b"b", b"d")] == [b"b", b"c"]
        backend.close()

    def test_in_memory_mode(self) -> None:
        backend = BPlusTreeBackend(None)
        backend.put(b"x", b"y")
        assert backend.get(b"x") == b"y"
        backend.close()

    def test_persists_across_reopen(self, tmp_path: Path) -> None:
        path = tmp_path / "kv.db"
        backend = BPlusTreeBackend(path)
        backend.put(b"k", b"v")
        backend.close()
        reopened = BPlusTreeBackend(path)
        assert reopened.get(b"k") == b"v"
        reopened.close()


class TestRowIdCodec:
    def test_round_trip(self) -> None:
        row_id = make_row_id("users", 42)
        assert split_row_id(row_id) == ("users", 42)

    def test_string_order_matches_numeric_order(self) -> None:
        ids = [make_row_id("t", n) for n in (1, 2, 10, 99, 100)]
        assert sorted(ids) == ids


class TestDurableRowStore:
    def _store(self) -> DurableRowStore:
        return DurableRowStore(BPlusTreeBackend(None), durability=None)

    def test_append_and_versions_of(self) -> None:
        store = self._store()
        row = make_row_id("t", 1)
        store.append_version(_version(row, 1, b"v1"))
        store.append_version(_version(row, 2, b"v2"))
        versions = store.versions_of(row)
        assert [v.value for v in versions] == [b"v1", b"v2"]
        assert [int(v.commit_seq) for v in versions] == [1, 2]

    def test_versions_do_not_leak_across_rows(self) -> None:
        store = self._store()
        store.append_version(_version(make_row_id("t", 1), 1, b"a"))
        store.append_version(_version(make_row_id("t", 11), 1, b"b"))
        assert [v.value for v in store.versions_of(make_row_id("t", 1))] == [b"a"]

    def test_high_commit_seq_with_zero_bytes(self) -> None:
        # Un commit_seq cuyo big-endian contiene bytes 0x00 intermedios no
        # debe confundir la decodificación de la clave.
        store = self._store()
        row = make_row_id("t", 1)
        tricky_seq = 0x0100_0000_0200_0003
        store.append_version(_version(row, tricky_seq, b"x"))
        versions = store.versions_of(row)
        assert [int(v.commit_seq) for v in versions] == [tricky_seq]

    def test_tombstone_round_trip(self) -> None:
        store = self._store()
        row = make_row_id("t", 1)
        store.append_version(_version(row, 1, b"v"))
        store.append_version(_version(row, 2, None))
        versions = store.versions_of(row)
        assert versions[-1].value is None

    def test_row_ids_deduplicates(self) -> None:
        store = self._store()
        row = make_row_id("t", 1)
        store.append_version(_version(row, 1, b"a"))
        store.append_version(_version(row, 2, b"b"))
        store.append_version(_version(make_row_id("t", 2), 3, b"c"))
        assert list(store.row_ids()) == [make_row_id("t", 1), make_row_id("t", 2)]

    def test_prune_versions(self) -> None:
        store = self._store()
        row = make_row_id("t", 1)
        v1, v2, v3 = (_version(row, n, f"v{n}".encode()) for n in (1, 2, 3))
        for v in (v1, v2, v3):
            store.append_version(v)
        removed = store.prune_versions(row, keep=[v3])
        assert removed == 2
        assert [v.value for v in store.versions_of(row)] == [b"v3"]

    def test_bootstrap_summary(self) -> None:
        store = self._store()
        store.append_version(_version(make_row_id("a", 1), 1, b"x"))
        store.append_version(_version(make_row_id("a", 7), 2, b"y"))
        store.append_version(_version(make_row_id("a", 7), 5, None))  # borrada
        store.append_version(_version(make_row_id("b", 3), 4, b"z"))
        summary = store.bootstrap_summary()
        assert summary.max_commit_seq == 5
        assert summary.tables["a"].max_rowid == 7
        assert summary.tables["a"].live_rows == 1
        assert summary.tables["b"].live_rows == 1

    def test_live_row_count_sees_latest_version(self) -> None:
        store = self._store()
        row = make_row_id("t", 1)
        store.append_version(_version(row, 1, b"v"))
        assert store.live_row_count("t") == 1
        store.append_version(_version(row, 2, None))
        assert store.live_row_count("t") == 0


class TestCatalogRoundTrip:
    def test_save_and_load(self, tmp_path: Path) -> None:
        from vectorized_execution_engine.models import ColumnType

        from nanosql.catalog import (
            Catalog,
            ColumnSchema,
            IndexSchema,
            TableSchema,
            load_catalog,
            save_catalog,
        )

        catalog = Catalog()
        catalog.add_table(
            TableSchema(
                name="users",
                columns=(
                    ColumnSchema("id", ColumnType.INTEGER, primary_key=True, not_null=True),
                    ColumnSchema("name", ColumnType.STRING, default="anon", has_default=True),
                ),
            )
        )
        catalog.add_index(IndexSchema(name="ix_name", table="users", columns=("name",)))
        path = tmp_path / "catalog.json"
        save_catalog(path, catalog, backend_name="btree")
        loaded, backend = load_catalog(path)
        assert backend == "btree"
        assert loaded.table("users").column("name").default == "anon"
        assert loaded.indexes["ix_name"].columns == ("name",)

    def test_corrupted_catalog_raises_internal_error(self, tmp_path: Path) -> None:
        from nanosql.catalog import load_catalog
        from nanosql.errors import InternalError

        path = tmp_path / "catalog.json"
        path.write_text("{no es json", encoding="utf-8")
        with pytest.raises(InternalError):
            load_catalog(path)
