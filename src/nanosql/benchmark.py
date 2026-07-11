"""Batería de benchmark del motor ensamblado (la que alimenta el README).

Mide el pipeline completo — parser → optimizador → ejecutor vectorizado →
MVCC → WAL → storage — nunca microbenchmarks de una pieza suelta: para eso
cada subproyecto ya tiene el suyo. Los números salen honestos, no
maquillados: el fsync por registro del WAL domina las escrituras y el
escaneo O(filas) del snapshot MVCC domina las lecturas puntuales, y ambos
se reportan tal cual son.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .driver import connect

INSERT_ROWS = 2_000
INSERT_BATCH = 200
POINT_QUERIES = 100
AGGREGATE_RUNS = 20
JOIN_RUNS = 10


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    backend: str
    operation: str
    total: int
    seconds: float

    @property
    def per_second(self) -> float:
        return self.total / self.seconds if self.seconds > 0 else float("inf")


def _bench_backend(backend: str, directory: Path) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []
    conn = connect(directory / f"bench-{backend}.db", backend=backend)
    try:
        conn.execute(
            "CREATE TABLE eventos ("
            " id INTEGER PRIMARY KEY,"
            " usuario INTEGER NOT NULL,"
            " tipo VARCHAR(20) NOT NULL,"
            " importe FLOAT NOT NULL)"
        )
        conn.execute("CREATE TABLE usuarios (id INTEGER PRIMARY KEY, nombre VARCHAR(30) NOT NULL)")
        conn.commit()

        # 1. INSERT transaccional por lotes (cada commit paga su fsync de WAL).
        started = time.perf_counter()
        for base in range(0, INSERT_ROWS, INSERT_BATCH):
            values = ", ".join(
                f"({n}, {n % 97}, 'tipo{n % 5}', {(n % 1000) / 10.0})"
                for n in range(base, base + INSERT_BATCH)
            )
            conn.execute(f"INSERT INTO eventos (id, usuario, tipo, importe) VALUES {values}")
            conn.commit()
        results.append(
            BenchmarkResult(
                backend, "insert (commit/200 filas)", INSERT_ROWS, time.perf_counter() - started
            )
        )

        values = ", ".join(f"({n}, 'usuario_{n}')" for n in range(97))
        conn.execute(f"INSERT INTO usuarios (id, nombre) VALUES {values}")
        conn.commit()

        # 2. SELECT puntual por clave primaria.
        started = time.perf_counter()
        for n in range(POINT_QUERIES):
            conn.execute(
                f"SELECT importe FROM eventos WHERE id = {(n * 37) % INSERT_ROWS}"
            ).fetchall()
        results.append(
            BenchmarkResult(
                backend, "select puntual por pk", POINT_QUERIES, time.perf_counter() - started
            )
        )

        # 3. Agregación con GROUP BY sobre toda la tabla.
        started = time.perf_counter()
        for _ in range(AGGREGATE_RUNS):
            conn.execute(
                "SELECT tipo, COUNT(*) AS n, SUM(importe) AS total"
                " FROM eventos GROUP BY tipo ORDER BY n DESC"
            ).fetchall()
        results.append(
            BenchmarkResult(
                backend,
                "aggregate group by (tabla completa)",
                AGGREGATE_RUNS,
                time.perf_counter() - started,
            )
        )

        # 4. JOIN de las dos tablas con filtro.
        started = time.perf_counter()
        for _ in range(JOIN_RUNS):
            conn.execute(
                "SELECT u.nombre, COUNT(*) AS n FROM eventos e"
                " JOIN usuarios u ON e.usuario = u.id"
                " WHERE e.importe > 50 GROUP BY u.nombre ORDER BY n DESC LIMIT 10"
            ).fetchall()
        results.append(
            BenchmarkResult(
                backend, "join + aggregate + top-10", JOIN_RUNS, time.perf_counter() - started
            )
        )

        # 5. UPDATE masivo con expresión.
        started = time.perf_counter()
        cur = conn.execute("UPDATE eventos SET importe = importe * 1.1 WHERE usuario < 10")
        conn.commit()
        results.append(
            BenchmarkResult(
                backend,
                "update masivo (con expresión)",
                cur.rowcount,
                time.perf_counter() - started,
            )
        )
    finally:
        conn.close()
    return results


def run_benchmark(*, backend: str | None = None, out: Callable[[str], None] = print) -> None:
    from .cli import format_table

    backends = [backend] if backend is not None else ["btree", "lsm"]
    rows: list[tuple[int | float | str | bool | None, ...]] = []
    for name in backends:
        out(f"Ejecutando benchmark con backend '{name}'...")
        with tempfile.TemporaryDirectory(prefix="nanosql-bench-") as tmp:
            for result in _bench_backend(name, Path(tmp)):
                rows.append(
                    (
                        result.backend,
                        result.operation,
                        result.total,
                        round(result.seconds, 3),
                        round(result.per_second, 1),
                    )
                )
    out("")
    out(format_table(["backend", "operación", "ops", "segundos", "ops/s"], rows))
