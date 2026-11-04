"""El runner sqllogictest: lectura del formato, formateo de valores con las
reglas de la implementación de referencia, hash, ordenaciones,
clasificación de fallos y detección de regresiones."""

from __future__ import annotations

import json
from pathlib import Path

from nanosql.sqllogictest import (
    arrange,
    check_regressions,
    format_value,
    hash_values,
    parse_records,
    run_file,
)

SAMPLE = """\
# comentario
statement ok
CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)

statement ok
INSERT INTO t1(a, b, c) VALUES (1, -2, 'x'), (3, 4, ''), (5, 6, NULL)

query I rowsort
SELECT a FROM t1
----
1
3
5

query IT nosort
SELECT b, c FROM t1 ORDER BY 1
----
-2
x
4
(empty)
6
NULL

hash-threshold 2

query I valuesort
SELECT a FROM t1
----
3 values hashing to 2a4ec9f01db4ca16a6e2f5a0f9a9e9f8

skipif nanosql
query I nosort
SELECT nope FROM nowhere
----

onlyif sqlite
statement ok
CREATE VIRTUAL TABLE v USING fts5(x)

statement error
SELECT * FROM missing

query R nosort
SELECT a / 2 FROM t1 ORDER BY a
----
0.000
1.000
2.000

statement ok
UPDATE t1 SET a = 1, a = 2

query I nosort
SELECT COUNT(*) FROM t1 WHERE a = 2
----
3

halt

statement ok
THIS IS NEVER PARSED
"""


def test_parse_records_handles_conditions_and_halt() -> None:
    records = list(parse_records(SAMPLE))
    kinds = [r.kind for r in records]
    assert kinds.count("skip") == 2
    assert "THIS IS NEVER PARSED" not in [r.sql for r in records]
    query = next(r for r in records if r.kind == "query")
    assert (query.types, query.sort_mode, query.expected) == ("I", "rowsort", ["1", "3", "5"])
    hashed = [r for r in records if r.expected_hash is not None]
    assert hashed[0].expected_hash == (3, "2a4ec9f01db4ca16a6e2f5a0f9a9e9f8")
    assert hashed[0].hash_threshold == 2


def test_value_formatting_follows_the_reference_runner() -> None:
    assert format_value(None, "I") == "NULL"
    assert format_value(3.9, "I") == "3"
    assert format_value(-3.9, "I") == "-3"
    assert format_value("12abc", "I") == "12"
    assert format_value(True, "I") == "1"
    assert format_value(2, "R") == "2.000"
    assert format_value("", "T") == "(empty)"
    assert format_value("a\tb", "T") == "a@b"


def test_hash_and_sort_modes() -> None:
    assert hash_values(["1", "2"]) == "6ddb4095eb719e2a9f0a3f95677d24e0"
    assert arrange(["3", "b", "1", "a"], 2, "rowsort") == ["1", "a", "3", "b"]
    assert arrange(["3", "b", "1", "a"], 2, "valuesort") == ["1", "3", "a", "b"]


def test_run_file_classifies_outcomes(tmp_path: Path) -> None:
    path = tmp_path / "sample.test"
    path.write_text(SAMPLE.replace("2a4ec9f01db4ca16a6e2f5a0f9a9e9f8", "0" * 32))
    result = run_file(path, timeout=5)
    assert result.skipped == 2
    categories = {f.line: f.category for f in result.failures}
    # el hash inventado no cuadra: resultado incorrecto
    assert "bug" in categories.values()
    # SET duplicado: nanosql lo rechaza (42601, como PostgreSQL) -> no
    # soportado, y la consulta siguiente sobre t1 hereda la cascada
    assert list(categories.values()).count("unsupported") == 2
    assert result.passed == result.run - len(result.failures)


def test_check_regressions(tmp_path: Path) -> None:
    path = tmp_path / "sample.test"
    path.write_text(SAMPLE)
    result = run_file(path, timeout=5)
    expected = tmp_path / "expected.json"
    expected.write_text(
        json.dumps({"files": {"sample": {"passed": result.passed + 1, "percent": 100.0}}})
    )
    assert check_regressions([result], expected)
    expected.write_text(
        json.dumps({"files": {"sample": {"passed": result.passed, "percent": 0.0}}})
    )
    assert check_regressions([result], expected) == []
