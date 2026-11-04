"""Piezas del servidor Postgres sin red: SCRAM, reescritura de SQL, tipos y
emulación del catálogo."""

from __future__ import annotations

import base64

import pytest

import nanosql
from nanosql.pgwire.auth import AuthenticationFailedError, ScramServerExchange, ScramVerifier
from nanosql.pgwire.catalog import CatalogEmulator, SessionInfo
from nanosql.pgwire.session import pg_column_name
from nanosql.pgwire.sqltext import prepare_text, split_statements, substitute_params
from nanosql.pgwire.types import (
    OID_BOOL,
    OID_FLOAT8,
    OID_INT4,
    OID_INT8,
    OID_NUMERIC,
    OID_TEXT,
    decode_binary_value,
    decode_text_value,
    encode_value,
    format_float,
)


class TestScram:
    # Vector de prueba del RFC 7677, sección 3 (usuario "user", "pencil").
    SALT = base64.b64decode("W22ZaJ0SNY7soEsUEjb6gQ==")
    CLIENT_FIRST = b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
    SERVER_NONCE = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
    CLIENT_FINAL = (
        b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
        b"p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
    )

    def _exchange(self, password: str) -> ScramServerExchange:
        verifier = ScramVerifier.from_password(password, salt=self.SALT, iterations=4096)
        return ScramServerExchange(verifier, server_nonce=self.SERVER_NONCE)

    def test_rfc7677_vector(self) -> None:
        exchange = self._exchange("pencil")
        first = exchange.handle_client_first(self.CLIENT_FIRST)
        assert first == (
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
            b"s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        )
        final = exchange.handle_client_final(self.CLIENT_FINAL)
        assert final == b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="

    def test_wrong_password_is_rejected(self) -> None:
        exchange = self._exchange("not-pencil")
        exchange.handle_client_first(self.CLIENT_FIRST)
        with pytest.raises(AuthenticationFailedError):
            exchange.handle_client_final(self.CLIENT_FINAL)

    def test_tampered_nonce_and_channel_binding_are_rejected(self) -> None:
        exchange = self._exchange("pencil")
        exchange.handle_client_first(self.CLIENT_FIRST)
        with pytest.raises(AuthenticationFailedError, match="nonce"):
            exchange.handle_client_final(self.CLIENT_FINAL.replace(b"k0,p", b"kX,p"))
        with pytest.raises(AuthenticationFailedError, match="channel binding"):
            self._exchange("pencil").handle_client_first(b"p=tls-server-end-point,,n=,r=abc")

    def test_verifier_string_has_pg_authid_shape(self) -> None:
        text = ScramVerifier.from_password("pencil", salt=self.SALT).to_string()
        assert text.startswith("SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$")


class TestSqlText:
    def test_placeholders_become_qmarks_in_occurrence_order(self) -> None:
        prepared = prepare_text("SELECT * FROM t WHERE b = $2 AND a = $1 OR c = $2;")
        assert prepared.sql == "SELECT * FROM t WHERE b = ? AND a = ? OR c = ?"
        assert prepared.bind(["A", "B"]) == ["B", "A", "B"]

    def test_parameter_casts_fix_the_parameter_type(self) -> None:
        prepared = prepare_text(
            "INSERT INTO t (a, b, c) VALUES ($1::INTEGER, $2::DOUBLE PRECISION, $3::VARCHAR(20))"
        )
        assert prepared.sql == "INSERT INTO t (a, b, c) VALUES (?, ?, ?)"
        assert prepared.bind(["7", "2.5", "x"]) == [7, 2.5, "x"]

    def test_literal_casts_and_escape_strings(self) -> None:
        prepared = prepare_text("SELECT a FROM t WHERE s = '5'::integer AND u = E'a\\'b'")
        assert prepared.sql == "SELECT a FROM t WHERE s = 5 AND u = 'a''b'"

    def test_column_casts_are_left_for_the_parser_to_reject(self) -> None:
        assert "a::text" in prepare_text("SELECT a::text FROM t").sql

    def test_split_respects_quotes_comments_and_dollar_strings(self) -> None:
        sql = "SELECT ';' FROM t; -- ; comentario\n/* ; */ SELECT $$;$$ FROM u;;"
        assert [s.strip() for s in split_statements(sql)] == [
            "SELECT ';' FROM t",
            "-- ; comentario\n/* ; */ SELECT $$;$$ FROM u",
        ]

    def test_substitute_params_quotes_strings(self) -> None:
        assert substitute_params("x = $1 AND y = $2", ["o'k", 3]) == "x = 'o''k' AND y = 3"


class TestTypes:
    def test_text_decoding_by_oid(self) -> None:
        assert decode_text_value("42", OID_INT4) == 42
        assert decode_text_value("2.5", OID_FLOAT8) == 2.5
        assert decode_text_value("10", OID_NUMERIC) == 10
        assert decode_text_value("t", OID_BOOL) is True
        assert decode_text_value("hola", 0) == "hola"
        with pytest.raises(nanosql.DataError) as info:
            decode_text_value("x", OID_INT4)
        assert info.value.sqlstate == "22P02"

    def test_binary_round_trip(self) -> None:
        for value, oid in ((-5, OID_INT8), (2.25, OID_FLOAT8), (True, OID_BOOL), ("ñ", OID_TEXT)):
            encoded = encode_value(value, oid, 1)
            assert encoded is not None
            assert decode_binary_value(encoded, oid) == value

    def test_float_text_matches_postgres(self) -> None:
        assert format_float(2.0) == "2"
        assert format_float(0.1) == "0.1"
        assert format_float(1e20) == "1e+20"
        assert format_float(float("inf")) == "Infinity"
        assert encode_value(False, OID_BOOL, 0) == b"f"

    def test_column_names_follow_postgres(self) -> None:
        assert pg_column_name("t.id") == "id"
        assert pg_column_name("COUNT(*)") == "count"
        assert pg_column_name("(id ADD 1)") == "?column?"
        assert pg_column_name("total") == "total"


class TestCatalogEmulator:
    @pytest.fixture()
    def emulator(self) -> CatalogEmulator:
        conn = nanosql.connect(":memory:", autocommit=True)
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, age INTEGER)")
        conn.execute("CREATE INDEX users_age ON users (age)")
        catalog = conn._require_engine().catalog_snapshot()
        conn.close()
        return CatalogEmulator(catalog, SessionInfo(user="ana", database="db", pid=1))

    def test_constant_selects(self, emulator: CatalogEmulator) -> None:
        result = emulator.handle("SELECT 1")
        assert result is not None and result.rows == [(1,)]
        version = emulator.handle("select pg_catalog.version()")
        assert version is not None
        assert str(version.rows[0][0]).startswith("PostgreSQL 16.0 (nanosql")
        schema = emulator.handle("select current_schema(), current_user AS u")
        assert schema is not None and schema.rows == [("public", "ana")]
        assert [c[0] for c in schema.columns] == ["current_schema", "u"]

    def test_user_queries_are_not_intercepted(self, emulator: CatalogEmulator) -> None:
        assert emulator.handle("SELECT id FROM users") is None

    def test_has_table_shape(self, emulator: CatalogEmulator) -> None:
        sql = (
            "SELECT pg_catalog.pg_class.relname FROM pg_catalog.pg_class "
            "WHERE pg_catalog.pg_class.relname IN ('{}'::VARCHAR) AND "
            "pg_catalog.pg_class.relkind = ANY (ARRAY['r'::VARCHAR, 'p'::VARCHAR])"
        )
        found = emulator.handle(sql.format("users"))
        missing = emulator.handle(sql.format("nope"))
        assert found is not None and found.rows == [("users",)]
        assert missing is not None and missing.rows == []

    def test_psql_describe_indexes(self, emulator: CatalogEmulator) -> None:
        sql = (
            "SELECT c2.relname, i.indisprimary, i.indisunique, "
            "pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), "
            "pg_catalog.pg_get_constraintdef(con.oid, true), contype "
            "FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i "
            "LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid) "
            "WHERE c.oid = '16384' AND c.oid = i.indrelid ORDER BY i.indisprimary DESC"
        )
        result = emulator.handle(sql)
        assert result is not None
        assert [row[0] for row in result.rows] == ["users_pkey", "users_age", "users_email_key"]
        assert result.rows[0][4] == "PRIMARY KEY (id)"
        assert result.rows[1][3] == "CREATE INDEX users_age ON public.users USING btree (age)"

    def test_unknown_catalog_relations_are_empty_with_columns(
        self, emulator: CatalogEmulator
    ) -> None:
        result = emulator.handle(
            "SELECT pol.polname, pol.polpermissive FROM pg_catalog.pg_policy pol "
            "WHERE pol.polrelid = '16384'"
        )
        assert result is not None
        assert result.rows == [] and [c[0] for c in result.columns] == [
            "polname",
            "polpermissive",
        ]
