"""Estadísticas por columna para el optimizador (`ANALYZE`).

Hasta ahora nanosql le entregaba al optimizador `distinct_count = filas`
para todas las columnas. Con esa suposición una igualdad sobre una columna
de pocos valores (`c_mktsegment = 'BUILDING'`, 5 valores en 1.500 filas) se
estimaba en una fila en vez de 300, y el modelo de coste elegía nested-loop
joins donde un hash join es órdenes de magnitud más barato.

`analyze` recorre la última versión confirmada de cada fila de una tabla y
calcula, por columna: valores distintos y nulos exactos, y un histograma de
equi-altura para las numéricas (el único tipo que el estimador de rangos del
optimizador sabe usar). El engine las cachea y las recalcula solo cuando la
tabla cambió lo bastante desde el último análisis — el mismo umbral que el
autovacuum de PostgreSQL para ANALYZE: 50 filas más un 10 %.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import cost_based_query_optimizer.models as om

from .catalog import ColumnValue

HISTOGRAM_BUCKETS = 32
AUTO_ANALYZE_BASE_ROWS = 50
AUTO_ANALYZE_SCALE = 0.1


@dataclass(frozen=True, slots=True)
class TableStatistics:
    """Resultado de analizar una tabla: filas vivas en ese momento y las
    estadísticas de cada columna."""

    row_count: int
    columns: dict[str, om.ColumnStatistics]

    def needs_refresh(self, live_rows: int) -> bool:
        threshold = AUTO_ANALYZE_BASE_ROWS + AUTO_ANALYZE_SCALE * self.row_count
        return abs(live_rows - self.row_count) > threshold

    def for_optimizer(self, live_rows: int) -> dict[str, om.ColumnStatistics]:
        """Las estadísticas con `distinct_count` acotado por las filas vivas
        actuales: entre dos análisis la tabla puede haber encogido."""
        cap = max(1, live_rows)
        return {
            name: (
                stats
                if stats.distinct_count <= cap
                else om.ColumnStatistics(
                    distinct_count=cap, null_count=stats.null_count, histogram=stats.histogram
                )
            )
            for name, stats in self.columns.items()
        }


def analyze(columns: Sequence[str], rows: Iterable[dict[str, ColumnValue]]) -> TableStatistics:
    values: dict[str, list[ColumnValue]] = {name: [] for name in columns}
    row_count = 0
    for row in rows:
        row_count += 1
        for name in columns:
            values[name].append(row.get(name))
    return TableStatistics(
        row_count=row_count,
        columns={name: _column_statistics(column) for name, column in values.items()},
    )


def _column_statistics(column: list[ColumnValue]) -> om.ColumnStatistics:
    present = [value for value in column if value is not None]
    numeric = [
        float(value)
        for value in present
        if isinstance(value, int | float) and not isinstance(value, bool)
    ]
    histogram = _equi_depth(numeric) if numeric and len(numeric) == len(present) else ()
    return om.ColumnStatistics(
        distinct_count=max(1, len(set(present))),
        null_count=len(column) - len(present),
        histogram=histogram,
    )


def _equi_depth(values: list[float]) -> tuple[om.HistogramBucket, ...]:
    """Cubos con (casi) el mismo número de filas cada uno; los límites son
    los valores mínimo y máximo de cada cubo."""
    values = sorted(values)
    buckets = min(HISTOGRAM_BUCKETS, len(values))
    edges = [round(i * len(values) / buckets) for i in range(buckets + 1)]
    return tuple(
        om.HistogramBucket(
            lower_bound=values[start],
            upper_bound=values[end - 1],
            row_count=end - start,
            distinct_count=len(set(values[start:end])),
        )
        for start, end in zip(edges, edges[1:], strict=False)
        if end > start
    )
