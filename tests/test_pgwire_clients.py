"""El servidor de protocolo Postgres frente a clientes reales: psql,
psycopg 3 y SQLAlchemy Core (postgresql+psycopg).

Cada test arranca un `PgServer` de verdad en un puerto efímero y habla con
él por TCP. psycopg/SQLAlchemy son dependencias opcionales de desarrollo y
psql un binario del sistema: si faltan, sus tests se saltan (en CI se
instalan los tres).
"""

from __future__ import annotations

import shutil
import subprocess
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from nanosql.pgwire import PgServer

psycopg: Any = pytest.importorskip("psycopg")

PSQL = shutil.which("psql")


@pytest.fixture()
def server(tmp_path: Path) -> Iterator[PgServer]:
    with PgServer(tmp_path / "pg.db", port=0) as srv:
        yield srv


@pytest.fixture()
def scram_server(tmp_path: Path) -> Iterator[PgServer]:
    users = {"ana": "correct horse", "bob": "battery"}
    with PgServer(tmp_path / "pg.db", port=0, auth="scram-sha-256", users=users) as srv:
        yield srv


def dsn(server: PgServer, user: str = "tester", password: str | None = None) -> str:
    host, port = server.address
    text = f"host={host} port={port} user={user} dbname=nanosql connect_timeout=5"
    return text + (f" password='{password}'" if password is not None else "")


def run_psql(
    server: PgServer, *commands: str, password: str | None = None, user: str = "tester"
) -> subprocess.CompletedProcess[str]:
    assert PSQL is not None
    host, port = server.address
    args = [PSQL, "-X", "-h", host, "-p", str(port), "-U", user, "-d", "nanosql"]
    args += ["-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose"]
    for command in commands:
        args += ["-c", command]
    env = {"PGCONNECT_TIMEOUT": "5", "PATH": "/usr/bin:/bin"}
    if password is not None:
        env["PGPASSWORD"] = password
    return subprocess.run(args, capture_output=True, text=True, timeout=60, env=env)


# ---------------------------------------------------------------------------
# psql
# ---------------------------------------------------------------------------


@pytest.mark.skipif(PSQL is None, reason="psql no está instalado")
class TestPsql:
    def test_create_insert_select(self, server: PgServer) -> None:
        result = run_psql(
            server,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(40) NOT NULL, "
            "age INTEGER DEFAULT 18, score FLOAT)",
            "INSERT INTO users (id, name, age, score) VALUES (1, 'ada', 36, 9.5), "
            "(2, 'grace', 45, 7.25)",
            "SELECT id, name, score FROM users ORDER BY id",
        )
        assert result.returncode == 0, result.stderr
        assert "CREATE TABLE" in result.stdout
        assert "INSERT 0 2" in result.stdout
        assert "  1 | ada   |   9.5" in result.stdout
        assert "(2 rows)" in result.stdout

    def test_meta_commands_dt_and_d(self, server: PgServer) -> None:
        setup = run_psql(
            server,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, "
            "age INTEGER DEFAULT 18)",
            "CREATE INDEX users_age ON users (age)",
        )
        assert setup.returncode == 0, setup.stderr
        listing = run_psql(server, r"\dt")
        assert listing.returncode == 0, listing.stderr
        assert "public | users | table | tester" in listing.stdout
        describe = run_psql(server, r"\d users")
        assert describe.returncode == 0, describe.stderr
        out = describe.stdout
        assert 'Table "public.users"' in out
        assert "id     | bigint |           | not null |" in out
        assert "| 18" in out
        assert '"users_pkey" PRIMARY KEY, btree (id)' in out
        assert '"users_email_key" UNIQUE CONSTRAINT, btree (email)' in out
        assert '"users_age" btree (age)' in out
        missing = run_psql(server, r"\d nope")
        assert "Did not find any relation named" in missing.stderr

    def test_version_show_and_sqlstate_errors(self, server: PgServer) -> None:
        result = run_psql(server, "SELECT version()", "SHOW server_version", "SHOW TimeZone")
        assert result.returncode == 0, result.stderr
        assert "PostgreSQL 16.0 (nanosql" in result.stdout
        assert "16.0" in result.stdout and "UTC" in result.stdout
        error = run_psql(server, "SELECT * FROM missing")
        assert error.returncode != 0
        assert "ERROR:  42P01:" in error.stderr

    def test_transaction_block(self, server: PgServer) -> None:
        result = run_psql(
            server,
            "CREATE TABLE t (id INTEGER PRIMARY KEY)",
            "BEGIN; INSERT INTO t (id) VALUES (1); ROLLBACK;",
            "BEGIN; INSERT INTO t (id) VALUES (2); COMMIT;",
            "SELECT count(*) FROM t",
        )
        assert result.returncode == 0, result.stderr
        assert " count \n-------\n     1\n" in result.stdout

    def test_scram_password(self, scram_server: PgServer) -> None:
        ok = run_psql(scram_server, "SELECT current_user", user="ana", password="correct horse")
        assert ok.returncode == 0, ok.stderr
        assert "ana" in ok.stdout
        bad = run_psql(scram_server, "SELECT 1", user="ana", password="wrong")
        assert bad.returncode != 0
        assert "FATAL" in bad.stderr and '"ana"' in bad.stderr


# ---------------------------------------------------------------------------
# psycopg 3
# ---------------------------------------------------------------------------


class TestPsycopg:
    def test_parameters_and_types_round_trip(self, server: PgServer) -> None:
        with psycopg.connect(dsn(server)) as conn:
            conn.execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, ok BOOLEAN)"
            )
            rows = [
                (1, "pan", 1.25, True),
                (2, "sal 'fina'; --", 0.5, False),
                (3, None, None, None),
            ]
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO items (id, name, price, ok) VALUES (%s, %s, %s, %s)", rows
                )
            conn.commit()
            got = conn.execute("SELECT id, name, price, ok FROM items ORDER BY id").fetchall()
            assert got == rows
            cur = conn.execute("SELECT id, price FROM items WHERE price > %s", (1,))
            assert [(d.name, d.type_code) for d in cur.description] == [("id", 20), ("price", 701)]
            assert cur.fetchall() == [(1, 1.25)]

    def test_binary_results_and_prepared_statements(self, server: PgServer) -> None:
        with psycopg.connect(dsn(server), autocommit=True) as conn:
            conn.execute("CREATE TABLE n (k INTEGER PRIMARY KEY, v FLOAT, s TEXT, b BOOLEAN)")
            conn.execute("INSERT INTO n (k, v, s, b) VALUES (1, -2.5, 'x', TRUE)")
            with conn.cursor(binary=True) as cur:
                cur.execute("SELECT k, v, s, b FROM n WHERE k = %s", (1,))
                assert cur.fetchall() == [(1, -2.5, "x", True)]
            # por encima del umbral psycopg pasa a sentencias preparadas con nombre
            for _ in range(8):
                assert conn.execute("SELECT s FROM n WHERE k = %s", (1,)).fetchone() == ("x",)
            prepared = conn.execute("SELECT s FROM n WHERE k = %s", (1,), prepare=True)
            assert prepared.fetchall() == [("x",)]

    def test_errors_carry_sqlstate_and_abort_the_block(self, server: PgServer) -> None:
        with psycopg.connect(dsn(server)) as conn:
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            conn.commit()
            with pytest.raises(psycopg.errors.UndefinedTable):
                conn.execute("SELECT * FROM missing")
            with pytest.raises(psycopg.errors.InFailedSqlTransaction):
                conn.execute("SELECT * FROM t")
            conn.rollback()
            conn.execute("INSERT INTO t (id) VALUES (1)")
            with pytest.raises(psycopg.errors.UniqueViolation):
                conn.execute("INSERT INTO t (id) VALUES (1)")
            conn.rollback()
            with pytest.raises(psycopg.errors.SyntaxError):
                conn.execute("SELEC 1")
            conn.rollback()
            assert conn.execute("SELECT count(*) FROM t").fetchone() == (0,)

    def test_commit_rollback_and_isolation_between_connections(self, server: PgServer) -> None:
        with psycopg.connect(dsn(server), autocommit=True) as setup:
            setup.execute("CREATE TABLE acct (id INTEGER PRIMARY KEY, bal INTEGER)")
            setup.execute("INSERT INTO acct (id, bal) VALUES (1, 100)")
        a = psycopg.connect(dsn(server))
        b = psycopg.connect(dsn(server))
        try:
            a.execute("UPDATE acct SET bal = bal - 10 WHERE id = 1")
            # b no ve la escritura sin confirmar de a
            assert b.execute("SELECT bal FROM acct WHERE id = 1").fetchone() == (100,)
            b.rollback()
            a.commit()
            assert b.execute("SELECT bal FROM acct WHERE id = 1").fetchone() == (90,)
            b.rollback()
            # first-committer-wins bajo REPEATABLE READ → 40001
            a.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            b.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            a.execute("UPDATE acct SET bal = bal - 1 WHERE id = 1")
            b.execute("UPDATE acct SET bal = bal - 2 WHERE id = 1")
            a.commit()
            with pytest.raises(psycopg.errors.SerializationFailure):
                b.commit()
            assert a.execute("SELECT bal FROM acct WHERE id = 1").fetchone() == (89,)
        finally:
            a.close()
            b.close()

    def test_concurrent_connections(self, server: PgServer) -> None:
        with psycopg.connect(dsn(server), autocommit=True) as setup:
            setup.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, worker INTEGER)")
        errors: list[BaseException] = []

        def worker(n: int) -> None:
            try:
                with psycopg.connect(dsn(server)) as conn:
                    for i in range(20):
                        conn.execute(
                            "INSERT INTO log (id, worker) VALUES (%s, %s)", (n * 1000 + i, n)
                        )
                        if i % 5 == 4:
                            conn.commit()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)
        assert not errors
        with psycopg.connect(dsn(server)) as conn:
            rows = conn.execute("SELECT worker, count(*) FROM log GROUP BY worker").fetchall()
            assert sorted(rows) == [(n, 20) for n in range(8)]

    def test_session_commands(self, server: PgServer) -> None:
        with psycopg.connect(dsn(server), autocommit=True) as conn:
            conn.execute("SET application_name = 'tests'")
            assert conn.execute("SHOW application_name").fetchone() == ("tests",)
            assert conn.execute("SELECT current_setting('server_version_num')").fetchone() == (
                "160000",
            )
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            conn.execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY")
            assert conn.execute("SHOW transaction_isolation").fetchone() == ("serializable",)
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                conn.execute("INSERT INTO t (id) VALUES (1)")
            conn.execute("ROLLBACK")
            # savepoint sin escrituras intermedias: ROLLBACK TO es exacto
            conn.execute("BEGIN")
            conn.execute("INSERT INTO t (id) VALUES (1)")
            conn.execute("SAVEPOINT sp")
            with pytest.raises(psycopg.errors.UndefinedTable):
                conn.execute("SELECT * FROM missing")
            conn.execute("ROLLBACK TO SAVEPOINT sp")
            conn.execute("COMMIT")
            assert conn.execute("SELECT count(*) FROM t").fetchone() == (1,)
            # con escrituras tras el savepoint se rechaza (nunca a medias)
            conn.execute("BEGIN")
            conn.execute("SAVEPOINT sp")
            conn.execute("INSERT INTO t (id) VALUES (2)")
            with pytest.raises(psycopg.errors.FeatureNotSupported):
                conn.execute("ROLLBACK TO SAVEPOINT sp")
            conn.execute("ROLLBACK")
            assert conn.execute("SELECT count(*) FROM t").fetchone() == (1,)

    def test_ddl_inside_block_warns_and_keeps_block_open(self, server: PgServer) -> None:
        notices: list[str] = []
        with psycopg.connect(dsn(server)) as conn:
            conn.add_notice_handler(lambda diag: notices.append(diag.message_primary or ""))
            conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
            conn.execute("INSERT INTO a (id) VALUES (1)")
            conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
            conn.execute("INSERT INTO a (id) VALUES (2)")
            conn.rollback()
            assert conn.execute("SELECT id FROM a").fetchall() == [(1,)]
        assert any("DDL" in n for n in notices)

    def test_scram_authentication(self, scram_server: PgServer) -> None:
        with psycopg.connect(dsn(scram_server, "ana", "correct horse")) as conn:
            assert conn.execute("SELECT current_user").fetchone() == ("ana",)
        with pytest.raises(psycopg.OperationalError, match="ana"):
            psycopg.connect(dsn(scram_server, "ana", "battery"))
        with pytest.raises(psycopg.OperationalError):
            psycopg.connect(dsn(scram_server, "mallory", "x"))


# ---------------------------------------------------------------------------
# SQLAlchemy Core (postgresql+psycopg)
# ---------------------------------------------------------------------------


class TestSqlAlchemyCore:
    def test_core_round_trip(self, server: PgServer) -> None:
        sa = pytest.importorskip("sqlalchemy")
        host, port = server.address
        engine = sa.create_engine(f"postgresql+psycopg://tester@{host}:{port}/nanosql")
        md = sa.MetaData()
        accounts = sa.Table(
            "accounts",
            md,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
            sa.Column("owner", sa.String(40), nullable=False),
            sa.Column("balance", sa.Float, nullable=False),
            sa.Column("active", sa.Boolean, nullable=False, default=True),
            sa.UniqueConstraint("owner"),
        )
        md.create_all(engine)
        md.create_all(engine)  # idempotente: has_table ve la tabla creada
        inspector = sa.inspect(engine)
        assert inspector.has_table("accounts") and not inspector.has_table("nope")
        with engine.begin() as conn:
            conn.execute(
                sa.insert(accounts),
                [{"id": i, "owner": f"o{i}", "balance": i * 10.0} for i in range(1, 6)],
            )
        with engine.connect() as conn:
            rich = conn.execute(
                sa.select(accounts.c.owner).where(accounts.c.balance >= 30).order_by(accounts.c.id)
            ).scalars()
            assert list(rich) == ["o3", "o4", "o5"]
            assert conn.execute(sa.select(sa.func.count()).select_from(accounts)).scalar() == 5
            conn.execute(
                sa.update(accounts).where(accounts.c.id == 1).values(balance=accounts.c.balance + 5)
            )
            conn.execute(sa.delete(accounts).where(accounts.c.id == 5))
            conn.commit()
            rows = conn.execute(
                sa.select(accounts.c.id, accounts.c.balance, accounts.c.active).order_by(
                    accounts.c.id
                )
            ).all()
            assert [tuple(r) for r in rows] == [
                (1, 15.0, True),
                (2, 20.0, True),
                (3, 30.0, True),
                (4, 40.0, True),
            ]
            text = conn.execute(sa.text("SELECT owner FROM accounts WHERE id = :i"), {"i": 2})
            assert text.scalar() == "o2"
            with pytest.raises(sa.exc.IntegrityError):
                conn.execute(sa.insert(accounts).values(id=9, owner="o2", balance=1.0))
            conn.rollback()
        engine.dispose()
