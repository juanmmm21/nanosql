"""Perfil por componente de nanosql sobre la carga TPC-H-like.

    python bench/profile_components.py --scale 0.01            # consultas
    python bench/profile_components.py --scale 0.005 --load    # solo la carga (INSERT)

Ejecuta cada consulta de `tpch_like.QUERIES` bajo cProfile y reparte el
tiempo *propio* (tottime) de cada función entre el repositorio que la
define: así se ve qué componente del ecosistema es el caliente, que es el
dato con el que se decide qué acelerar. Los tiempos absolutos incluyen la
sobrecarga del perfilador (2-4x); lo que cuenta son las proporciones.
"""

from __future__ import annotations

import argparse
import collections
import cProfile
import pstats
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from tpch_like import INDEXES, QUERIES, SCHEMA, Dataset  # noqa: E402

import nanosql  # noqa: E402

# Del más específico al más general: el primer fragmento que aparece en la
# ruta del fichero decide el cubo.
_BUCKETS = (
    ("nanosql/source", "nanosql.source"),
    ("nanosql/rowstore", "nanosql.rowstore"),
    ("nanosql/storage", "nanosql.storage"),
    ("nanosql/", "nanosql (other)"),
    ("mvcc_transaction_manager", "mvcc-transaction-manager"),
    ("bplus_tree_storage_engine", "bplus-tree-storage-engine"),
    ("lsm_tree_engine", "lsm-tree-engine"),
    ("vectorized_execution_engine", "vectorized-execution-engine"),
    ("cost_based_query_optimizer", "cost-based-query-optimizer"),
    ("sql_query_parser", "sql-query-parser"),
    ("write_ahead_log_recovery", "write-ahead-log-recovery"),
    ("lock_manager_deadlock_detector", "lock-manager-deadlock-detector"),
    ("numpy", "numpy"),
    ("json", "json"),
)


def _bucket(path: str) -> str:
    if path == "~":
        return "builtins"
    for fragment, name in _BUCKETS:
        if fragment in path:
            return name
    return "other"


def _shares(profile: cProfile.Profile, elapsed: float, top: int) -> str:
    totals: collections.Counter[str] = collections.Counter()
    for (path, _line, _fn), (_cc, _nc, tottime, _ct, _callers) in pstats.Stats(
        profile
    ).stats.items():  # type: ignore[attr-defined]
        totals[_bucket(path)] += tottime
    return ", ".join(f"{k} {v / elapsed * 100:.0f}%" for k, v in totals.most_common(top))


def _load(conn: nanosql.Connection, scale: float) -> None:
    for table, ddl in SCHEMA.items():
        conn.execute(f"CREATE TABLE {table} ({ddl})")
    for name, columns, rows in Dataset(scale).tables():
        batch = list(rows)
        placeholders = "(" + ", ".join("?" * len(columns)) + ")"
        for i in range(0, len(batch), 500):
            chunk = batch[i : i + 500]
            conn.execute(
                f"INSERT INTO {name} ({', '.join(columns)}) VALUES "
                + ", ".join([placeholders] * len(chunk)),
                [v for row in chunk for v in row],
            )
        conn.commit()
    for statement in INDEXES:
        conn.execute(statement)
    conn.execute("ANALYZE")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=float, default=0.01)
    parser.add_argument("--load", action="store_true", help="perfila la carga, no las consultas")
    parser.add_argument("--top", type=int, default=5)
    args = parser.parse_args()
    conn = nanosql.connect(":memory:")
    if args.load:
        profile = cProfile.Profile()
        started = time.perf_counter()
        profile.enable()
        _load(conn, args.scale)
        profile.disable()
        elapsed = time.perf_counter() - started
        print(f"load {elapsed:.1f}s | {_shares(profile, elapsed, args.top)}")
        pstats.Stats(profile).sort_stats("tottime").print_stats(12)
        return 0
    started = time.perf_counter()
    _load(conn, args.scale)
    print(f"load {time.perf_counter() - started:.1f}s (sin perfilar)")
    for name, sql in QUERIES.items():
        profile = cProfile.Profile()
        started = time.perf_counter()
        profile.enable()
        rows = conn.execute(sql).fetchall()
        profile.disable()
        elapsed = time.perf_counter() - started
        conn.rollback()
        shares = _shares(profile, elapsed, args.top)
        print(f"{name:6s} {elapsed * 1000:8.1f} ms rows={len(rows):4d} | {shares}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
