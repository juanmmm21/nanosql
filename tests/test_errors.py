"""Manejo de errores: SQL inválido, esquema inexistente, tipos y
restricciones — siempre una excepción tipada del driver, nunca un crash ni
una excepción cruda de un subproyecto interno."""

from __future__ import annotations

import pytest

import nanosql
from nanosql import (
    DataError,
    IntegrityError,
    NanosqlError,
    NotSupportedError,
    ProgrammingError,
)


@pytest.fixture()
def conn() -> nanosql.Connection:  # type: ignore[misc]
    connection = nanosql.connect(":memory:")
    connection.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(20) NOT NULL, score FLOAT)"
    )
    connection.execute("INSERT INTO t (id, name, score) VALUES (1, 'uno', 1.5)")
    yield connection
    connection.close()


class TestSyntaxErrors:
    def test_syntax_error_is_programming_error_with_position(
        self, conn: nanosql.Connection
    ) -> None:
        with pytest.raises(ProgrammingError) as excinfo:
            conn.execute("SELECT FROM t")
        message = str(excinfo.value)
        assert "line 1" in message
        assert "^" in message  # formato caret del propio parser

    def test_garbage_is_programming_error(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("esto no es sql")

    def test_unterminated_string_is_programming_error(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("SELECT * FROM t WHERE name = 'sin cerrar")

    def test_derived_table_is_not_supported(self, conn: nanosql.Connection) -> None:
        # Las subconsultas en posición de expresión SÍ están soportadas
        # (ver test_subqueries.py); la tabla derivada en FROM sigue fuera
        # del subconjunto y llega como NotSupportedError desde el parser.
        with pytest.raises(NotSupportedError):
            conn.execute("SELECT * FROM (SELECT id FROM t) d")

    def test_distinct_order_by_unprojected_column_rejected(self, conn: nanosql.Connection) -> None:
        # SQL estándar: con DISTINCT, ORDER BY solo puede usar columnas de
        # la lista SELECT (una clave oculta cambiaría la deduplicación).
        with pytest.raises(ProgrammingError):
            conn.execute("SELECT DISTINCT name FROM t ORDER BY score")

    def test_order_by_unprojected_column_works_and_stays_hidden(
        self, conn: nanosql.Connection
    ) -> None:
        # La clave de ordenación no proyectada viaja como columna oculta por
        # el plan y se recorta del resultado: ni aparece en las filas ni en
        # la descripción del cursor.
        conn.execute("INSERT INTO t (id, name, score) VALUES (2, 'dos', 9.9)")
        cur = conn.execute("SELECT name FROM t ORDER BY score DESC")
        assert cur.fetchall() == [("dos",), ("uno",)]
        assert cur.description is not None and [d[0] for d in cur.description] == ["name"]

    def test_malformed_begin_is_programming_error(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("BEGIN nonsense extra")


class TestSchemaErrors:
    def test_unknown_table(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("SELECT * FROM no_existe")

    def test_unknown_column_in_where(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("SELECT * FROM t WHERE fantasma = 1")

    def test_unknown_column_in_insert(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("INSERT INTO t (id, fantasma) VALUES (2, 1)")

    def test_duplicate_table(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("CREATE TABLE t (x INTEGER)")

    def test_duplicate_index(self, conn: nanosql.Connection) -> None:
        conn.execute("CREATE INDEX ix ON t (score)")
        with pytest.raises(ProgrammingError):
            conn.execute("CREATE INDEX ix ON t (score)")

    def test_index_on_unknown_column(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("CREATE INDEX ix2 ON t (fantasma)")

    def test_insert_wrong_value_count(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("INSERT INTO t (id, name) VALUES (2, 'dos', 3.0)")

    def test_unsupported_column_type(self, conn: nanosql.Connection) -> None:
        with pytest.raises(NotSupportedError):
            conn.execute("CREATE TABLE fechas (d DATE)")

    def test_composite_primary_key_not_supported(self, conn: nanosql.Connection) -> None:
        with pytest.raises(NotSupportedError):
            conn.execute("CREATE TABLE c (a INTEGER PRIMARY KEY, b INTEGER PRIMARY KEY)")


class TestDataAndIntegrityErrors:
    def test_not_null_violation(self, conn: nanosql.Connection) -> None:
        with pytest.raises(IntegrityError):
            conn.execute("INSERT INTO t (id, name) VALUES (2, NULL)")

    def test_primary_key_duplicate(self, conn: nanosql.Connection) -> None:
        with pytest.raises(IntegrityError):
            conn.execute("INSERT INTO t (id, name) VALUES (1, 'repetido')")

    def test_primary_key_duplicate_within_statement(self, conn: nanosql.Connection) -> None:
        with pytest.raises(IntegrityError):
            conn.execute("INSERT INTO t (id, name) VALUES (5, 'a'), (5, 'b')")

    def test_update_to_duplicate_primary_key(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO t (id, name) VALUES (2, 'dos')")
        with pytest.raises(IntegrityError):
            conn.execute("UPDATE t SET id = 1 WHERE id = 2")

    def test_type_mismatch_string_into_integer(self, conn: nanosql.Connection) -> None:
        with pytest.raises(DataError):
            conn.execute("INSERT INTO t (id, name) VALUES ('no numero', 'x')")

    def test_type_mismatch_float_into_integer(self, conn: nanosql.Connection) -> None:
        with pytest.raises(DataError):
            conn.execute("INSERT INTO t (id, name) VALUES (1.5, 'x')")

    def test_integer_widens_to_float_column(self, conn: nanosql.Connection) -> None:
        conn.execute("INSERT INTO t (id, name, score) VALUES (3, 'tres', 2)")
        assert conn.execute("SELECT score FROM t WHERE id = 3").fetchall() == [(2.0,)]

    def test_non_constant_insert_value(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("INSERT INTO t (id, name) VALUES (score, 'x')")

    def test_aggregate_in_where_rejected(self, conn: nanosql.Connection) -> None:
        with pytest.raises(ProgrammingError):
            conn.execute("DELETE FROM t WHERE COUNT(*) > 0")

    def test_errors_do_not_kill_the_connection(self, conn: nanosql.Connection) -> None:
        for bad in (
            "SELECT FROM t",
            "SELECT * FROM no_existe",
            "INSERT INTO t (id, name) VALUES (1, 'dup')",
        ):
            with pytest.raises(NanosqlError):
                conn.execute(bad)
        assert conn.execute("SELECT COUNT(*) FROM t").fetchall() == [(1,)]


class TestSqlstate:
    """Cada error lleva un SQLSTATE: el de su clase o uno específico."""

    def test_class_defaults(self) -> None:
        assert nanosql.ProgrammingError("x").sqlstate == "42000"
        assert nanosql.NotSupportedError("x").sqlstate == "0A000"
        assert nanosql.TransactionConflictError("x").sqlstate == "40001"
        assert nanosql.ProgrammingError("x", sqlstate="42P01").sqlstate == "42P01"

    def test_specific_codes_from_the_engine(self) -> None:
        conn = nanosql.connect(":memory:", autocommit=True)
        try:
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            cases = {
                "SELECT * FROM nope": "42P01",
                "SELECT zz FROM t": "42703",
                "SELEC 1": "42601",
                "CREATE TABLE t (id INTEGER)": "42P07",
                "INSERT INTO t (id, v) VALUES (1, 'x')": "42804",
                "INSERT INTO t (v) VALUES (1)": "23502",
            }
            for sql, code in cases.items():
                with pytest.raises(nanosql.DatabaseError) as info:
                    conn.execute(sql)
                assert info.value.sqlstate == code, sql
            conn.execute("INSERT INTO t (id, v) VALUES (1, 0)")
            with pytest.raises(nanosql.IntegrityError) as dup:
                conn.execute("INSERT INTO t (id, v) VALUES (1, 0)")
            assert dup.value.sqlstate == "23505"
            with pytest.raises(nanosql.DataError) as div:
                conn.execute("SELECT id / v FROM t")
            assert div.value.sqlstate == "22012"
        finally:
            conn.close()
