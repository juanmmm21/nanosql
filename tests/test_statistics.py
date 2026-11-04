"""Estadísticas por columna (`ANALYZE` y auto-ANALYZE) y su efecto en los
planes: con `distinct_count = filas` para todo, una igualdad sobre una
columna de pocos valores se estimaba en una fila y el optimizador elegía
nested-loop joins (TPC-H-like Q3/Q5/Q10, hasta 15 s)."""

from __future__ import annotations

from pathlib import Path

import pytest

import nanosql
from nanosql.errors import ProgrammingError
from nanosql.statistics import AUTO_ANALYZE_BASE_ROWS, HISTOGRAM_BUCKETS, analyze


def test_analyze_counts_distinct_values_nulls_and_builds_numeric_histograms() -> None:
    rows = [
        {"id": i, "segment": ["A", "B", "C"][i % 3], "price": float(i % 50), "note": None}
        for i in range(300)
    ]
    rows[0]["segment"] = None
    stats = analyze(["id", "segment", "price", "note"], rows)
    assert stats.row_count == 300
    assert stats.columns["id"].distinct_count == 300
    assert stats.columns["segment"].distinct_count == 3
    assert stats.columns["segment"].null_count == 1
    assert stats.columns["segment"].histogram == ()  # texto: sin histograma
    assert stats.columns["note"].null_count == 300
    price = stats.columns["price"]
    assert price.distinct_count == 50
    assert len(price.histogram) == HISTOGRAM_BUCKETS
    assert sum(bucket.row_count for bucket in price.histogram) == 300
    bounds = [(bucket.lower_bound, bucket.upper_bound) for bucket in price.histogram]
    assert bounds == sorted(bounds)
    assert price.histogram[0].lower_bound == 0.0 and price.histogram[-1].upper_bound == 49.0


def _load(conn: nanosql.Connection) -> None:
    conn.execute("CREATE TABLE customer (c_id INTEGER PRIMARY KEY, c_segment TEXT)")
    conn.execute("CREATE TABLE orders (o_id INTEGER PRIMARY KEY, o_cust INTEGER, o_total FLOAT)")
    segments = ["AUTO", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
    conn.execute(
        "INSERT INTO customer (c_id, c_segment) VALUES "
        + ", ".join(f"({i}, '{segments[i % 5]}')" for i in range(1, 301))
    )
    conn.execute(
        "INSERT INTO orders (o_id, o_cust, o_total) VALUES "
        + ", ".join(f"({i}, {1 + i % 300}, {i * 1.5})" for i in range(1, 3001))
    )
    conn.commit()


QUERY = (
    "SELECT c_id, SUM(o_total) FROM customer, orders "
    "WHERE c_id = o_cust AND c_segment = 'BUILDING' GROUP BY c_id ORDER BY c_id"
)


def test_low_cardinality_filter_is_estimated_from_distinct_values_and_joins_hash() -> None:
    conn = nanosql.connect(":memory:")
    _load(conn)
    plan = "\n".join(row[0] for row in conn.execute("EXPLAIN " + QUERY).fetchall())
    # 300 clientes, 5 segmentos: 60 filas, no 1
    assert "Filter((c_segment = 'BUILDING')) rows=60.0" in plan
    assert "HashJoin" in plan and "NestedLoopJoin" not in plan
    rows = conn.execute(QUERY).fetchall()
    assert len(rows) == 60
    expected = {
        c: sum(i * 1.5 for i in range(1, 3001) if 1 + i % 300 == c)
        for c in range(1, 301)
        if c % 5 == 1
    }
    assert {c: pytest.approx(total) for c, total in rows} == expected


def test_analyze_statement_and_unknown_table() -> None:
    conn = nanosql.connect(":memory:")
    _load(conn)
    conn.execute("ANALYZE")
    conn.execute("ANALYZE customer")
    with pytest.raises(ProgrammingError) as info:
        conn.execute("ANALYZE nope")
    assert info.value.sqlstate == "42P01"


def test_analyze_inside_a_transaction_leaves_it_open() -> None:
    conn = nanosql.connect(":memory:")
    _load(conn)
    conn.execute("INSERT INTO customer (c_id, c_segment) VALUES (1000, 'AUTO')")
    conn.execute("ANALYZE customer")
    # the pending insert is still there, uncommitted, and still ours
    assert conn.execute("SELECT COUNT(*) FROM customer").fetchall() == [(301,)]
    conn.rollback()
    assert conn.execute("SELECT COUNT(*) FROM customer").fetchall() == [(300,)]


def test_statistics_refresh_only_past_the_auto_analyze_threshold() -> None:
    conn = nanosql.connect(":memory:")
    _load(conn)
    engine = conn._engine
    assert engine.table_statistics("customer").row_count == 300
    few = AUTO_ANALYZE_BASE_ROWS  # 50 + 10% of 300 = 80 is the threshold
    conn.execute(
        "INSERT INTO customer (c_id, c_segment) VALUES "
        + ", ".join(f"({i}, 'NEW')" for i in range(1000, 1000 + few))
    )
    conn.commit()
    assert engine.table_statistics("customer").row_count == 300
    conn.execute(
        "INSERT INTO customer (c_id, c_segment) VALUES "
        + ", ".join(f"({i}, 'NEW')" for i in range(2000, 2100))
    )
    conn.commit()
    stats = engine.table_statistics("customer")
    assert stats.row_count == 300 + few + 100
    assert stats.columns["c_segment"].distinct_count == 6


def test_statistics_survive_reopen_lazily(tmp_path: Path) -> None:
    path = tmp_path / "db"
    conn = nanosql.connect(str(path))
    _load(conn)
    conn.close()
    conn = nanosql.connect(str(path))
    plan = "\n".join(row[0] for row in conn.execute("EXPLAIN " + QUERY).fetchall())
    assert "HashJoin" in plan
    conn.close()
