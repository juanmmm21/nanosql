"""Benchmark TPC-H-like: nanosql (Python puro y con el acelerador Rust)
frente a SQLite y DuckDB, sobre los mismos datos generados.

    pip install -e ".[bench]"          # duckdb (sqlite3 viene con Python)
    python bench/run.py --scale 0.01 --out bench/results.json

Cada motor corre en su propio subproceso (así `BPLUS_TREE_PURE_PYTHON`
decide de verdad qué códec carga nanosql y ningún motor calienta cachés
del siguiente). Todos usan bases de datos en memoria. Por consulta: una
ejecución de calentamiento y la mediana de `--repeat` ejecuciones. Los
resultados de los cuatro motores se comparan fila a fila (reales con
tolerancia relativa 1e-6): una diferencia se informa, no se oculta.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from tpch_like import INDEXES, QUERIES, SCHEMA, Dataset  # noqa: E402

ENGINES = ("nanosql-python", "nanosql-rust", "sqlite", "duckdb")


def _load(execute: Any, many: Any, scale: float) -> float:
    started = time.perf_counter()
    for name, columns, rows in Dataset(scale).tables():
        many(name, columns, list(rows))
    for statement in INDEXES:
        execute(statement)
    # Estadísticas del optimizador en los tres motores (los tres lo admiten);
    # cuenta como parte de la carga.
    execute("ANALYZE")
    return time.perf_counter() - started


def _run_engine(engine: str, scale: float, repeat: int) -> dict[str, Any]:
    if engine.startswith("nanosql"):
        from bplus_tree_storage_engine import pipeline as bpt

        import nanosql

        conn = nanosql.connect(":memory:")
        for table, ddl in SCHEMA.items():
            conn.execute(f"CREATE TABLE {table} ({ddl})")

        def many(name: str, columns: list[str], rows: list[tuple[object, ...]]) -> None:
            placeholders = "(" + ", ".join("?" * len(columns)) + ")"
            for i in range(0, len(rows), 500):
                chunk = rows[i : i + 500]
                conn.execute(
                    f"INSERT INTO {name} ({', '.join(columns)}) VALUES "
                    + ", ".join([placeholders] * len(chunk)),
                    [v for row in chunk for v in row],
                )
            conn.commit()

        def query(sql: str) -> list[tuple[object, ...]]:
            rows = conn.execute(sql).fetchall()
            conn.rollback()
            return rows

        def execute(sql: str) -> None:
            conn.execute(sql)

        info = {"native_codec": bpt.NATIVE_CODEC}
    elif engine == "sqlite":
        import sqlite3

        db = sqlite3.connect(":memory:")
        for table, ddl in SCHEMA.items():
            db.execute(f"CREATE TABLE {table} ({ddl})")

        def many(name: str, columns: list[str], rows: list[tuple[object, ...]]) -> None:
            db.executemany(
                f"INSERT INTO {name} ({', '.join(columns)}) VALUES "
                f"({', '.join('?' * len(columns))})",
                rows,
            )
            db.commit()

        def query(sql: str) -> list[tuple[object, ...]]:
            return db.execute(sql).fetchall()

        def execute(sql: str) -> None:
            db.execute(sql)

        info = {"sqlite_version": sqlite3.sqlite_version}
    else:
        import duckdb

        db = duckdb.connect(":memory:")
        for table, ddl in SCHEMA.items():
            # FLOAT de DuckDB es de 4 bytes: DOUBLE para la misma precisión.
            db.execute(f"CREATE TABLE {table} ({ddl.replace('FLOAT', 'DOUBLE')})")

        def many(name: str, columns: list[str], rows: list[tuple[object, ...]]) -> None:
            # Carga masiva idiomática de DuckDB: COPY desde un CSV temporal
            # (executemany fila a fila sería un uso artificialmente lento).
            import csv
            import tempfile

            with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as handle:
                csv.writer(handle, delimiter="|").writerows(rows)
                path = handle.name
            try:
                db.execute(f"COPY {name} FROM '{path}' (DELIMITER '|', HEADER false)")
            finally:
                os.unlink(path)

        def query(sql: str) -> list[tuple[object, ...]]:
            return [tuple(r) for r in db.execute(sql).fetchall()]

        def execute(sql: str) -> None:
            db.execute(sql)

        info = {"duckdb_version": duckdb.__version__}
    load_seconds = _load(execute, many, scale)
    results: dict[str, Any] = {}
    for name, sql in QUERIES.items():
        query(sql)  # calentamiento
        times = []
        rows: list[tuple[object, ...]] = []
        for _ in range(repeat):
            started = time.perf_counter()
            rows = query(sql)
            times.append(time.perf_counter() - started)
        results[name] = {
            "median_ms": statistics.median(times) * 1000,
            "runs_ms": [t * 1000 for t in times],
            "rows": [list(r) for r in rows],
        }
    return {"engine": engine, "load_seconds": load_seconds, "queries": results, **info}


def _same(a: list[Any], b: list[Any]) -> bool:
    if len(a) != len(b):
        return False
    for row_a, row_b in zip(a, b, strict=True):
        if len(row_a) != len(row_b):
            return False
        for x, y in zip(row_a, row_b, strict=True):
            if isinstance(x, float) or isinstance(y, float):
                if abs(float(x) - float(y)) > 1e-6 * max(1.0, abs(float(x))):
                    return False
            elif x != y:
                return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=float, default=0.01)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--engines", default=",".join(ENGINES))
    parser.add_argument("--out", type=Path, default=HERE / "results.json")
    parser.add_argument("--worker", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(_run_engine(args.worker, args.scale, args.repeat)))
        return 0
    runs = []
    for engine in args.engines.split(","):
        env = dict(os.environ)
        if engine == "nanosql-python":
            env["BPLUS_TREE_PURE_PYTHON"] = "1"
        else:
            env.pop("BPLUS_TREE_PURE_PYTHON", None)
        print(f"== {engine}", file=sys.stderr, flush=True)
        out = subprocess.run(
            [sys.executable, __file__, "--worker", engine, "--scale", str(args.scale),
             "--repeat", str(args.repeat)],
            env=env, capture_output=True, text=True, check=True,
        )  # fmt: skip
        runs.append(json.loads(out.stdout))
    reference = runs[-1]["queries"]
    for run in runs:
        for name, result in run["queries"].items():
            result["matches_reference"] = _same(result["rows"], reference[name]["rows"])
    machine = {
        "cpu": _cpu_model(),
        "cores": os.cpu_count(),
        "python": platform.python_version(),
        "platform": platform.platform(),
    }
    args.out.write_text(
        json.dumps({"scale": args.scale, "repeat": args.repeat, "machine": machine, "runs": runs},
                   indent=1) + "\n", encoding="utf-8",
    )  # fmt: skip
    print(_table(runs, args.scale))
    return 0


def _cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "desconocida"


def _table(runs: list[dict[str, Any]], scale: float) -> str:
    names = [r["engine"] for r in runs]
    lines = [
        f"| query (SF {scale}) | " + " | ".join(f"{n} (ms)" for n in names) + " |",
        "|---|" + "---:|" * len(names),
    ]
    lines.append("| load (s) | " + " | ".join(f"{r['load_seconds']:.1f}" for r in runs) + " |")
    for query in runs[0]["queries"]:
        cells = []
        for run in runs:
            result = run["queries"][query]
            mark = "" if result["matches_reference"] else " ≠"
            cells.append(f"{result['median_ms']:.1f}{mark}")
        lines.append(f"| {query} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
