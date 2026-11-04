# nanosql

**An embeddable SQL engine with a CLI/REPL and a Python driver, assembled
entirely from the ten from-scratch systems components of the
[strata-database-engine](https://github.com/juanmmm21) ecosystem.**

Repository: <https://github.com/juanmmm21/nanosql>

`nanosql` is the flagship application of the ecosystem — the eleventh and
final project. The other ten repositories each build one database-systems
component in isolation (a B+Tree, an LSM tree, a WAL, an MVCC manager, a
lock manager, a SQL parser, a cost-based optimizer, a vectorized executor,
Raft consensus, a shard router), with a strict rule: **no cross-imports
between them**. `nanosql` is the single, explicit exception to that rule.
It imports all ten as real package dependencies and wires them into a
working relational database — no SQLite, DuckDB or Postgres anywhere in the
stack, not even for tests.

```python
import nanosql

conn = nanosql.connect("mydata.db")
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
conn.execute("INSERT INTO users (id, name, age) VALUES (1, 'ada', 36), (2, 'grace', 45)")
conn.commit()

cur = conn.execute("SELECT name FROM users WHERE age > 40 ORDER BY name")
print(cur.fetchall())  # [('grace',)]
conn.close()
```

## What it is

A single-node, embeddable, transactional SQL engine:

* **DB-API 2.0 style Python driver** — `connect()`, `Cursor.execute()` with
  qmark (`?`) query parameters, `executemany()`,
  `fetchone/fetchmany/fetchall`, `description`, `rowcount`,
  `commit()`/`rollback()`, context-manager support.
* **Interactive CLI/REPL** (`nanosql` in a terminal) with table-formatted
  output, multi-line statements, `EXPLAIN` for inspecting physical plans,
  `.tables`/`.schema` meta commands and a built-in benchmark mode.
* **Real transactions** — `BEGIN`/`COMMIT`/`ROLLBACK` backed by MVCC
  snapshots with three isolation levels (`read_committed`,
  `repeatable_read`, `serializable`, including write-skew detection).
* **Two swappable storage backends** — a disk-backed B+Tree (default) or an
  LSM tree, chosen at creation time and recorded in the database catalog.
* **Crash safety** — every commit is written ahead to a binary WAL with
  per-record `fsync`; opening a database after a crash replays committed
  work (redo) and rolls back half-written commits (undo). The test suite
  includes a scenario where the entire B+Tree data file is deleted and the
  database is rebuilt from the WAL alone.
* **PostgreSQL wire protocol** — `nanosql serve` speaks protocol v3
  (simple and extended query, SCRAM-SHA-256, SQLSTATE errors), so psql,
  psycopg and SQLAlchemy connect to a nanosql database unchanged.
* **Optional distributed mode** (`nanosql[distributed]`) — a Raft-replicated
  cluster of nanosql nodes and a hash-sharded deployment with distributed
  aggregate merging, both as in-process simulations.

## How the ten repositories fit together

```text
                              SQL text
                                 │
                     ┌───────────▼───────────┐
                     │    sql-query-parser    │  lexer + parser → typed AST
                     └───────────┬───────────┘
                                 │  (structural AST translation)
                     ┌───────────▼───────────┐
                     │ cost-based-query-      │  logical → physical plan,
                     │ optimizer              │  join reordering, index choice
                     └───────────┬───────────┘
                                 │  (structural plan translation)
                     ┌───────────▼───────────┐
                     │ vectorized-execution-  │  columnar batches, NumPy
                     │ engine                 │  operators, memory budget
                     └───────────┬───────────┘
                                 │  TableSource protocol
                     ┌───────────▼───────────┐      ┌──────────────────────┐
                     │ mvcc-transaction-      │◄────►│ lock-manager-        │
                     │ manager                │      │ deadlock-detector    │
                     │ snapshots, conflicts,  │      │ table-level 2PL      │
                     │ isolation levels       │      │ fencing DDL          │
                     └───────────┬───────────┘      └──────────────────────┘
                                 │  RowStore protocol
                     ┌───────────▼───────────┐
                     │  DurableRowStore       │  version chains as ordered keys
                     └─────┬───────────┬─────┘
                           │           │ write-ahead of every mutation
              ┌────────────▼─┐   ┌─────▼──────────────────┐
              │ storage      │   │ write-ahead-log-        │
              │ backend      │   │ recovery                │
              │              │   │ fsync'd log, redo/undo  │
              │ bplus-tree-  │   │ recovery, checkpoints   │
              │ storage-     │   └────────────────────────┘
              │ engine  OR   │
              │ lsm-tree-    │        optional distributed mode
              │ engine       │   ┌────────────────────────────────┐
              └──────────────┘   │ raft-replication-log            │
                                 │   → ReplicatedNanosql           │
                                 │ distributed-query-router        │
                                 │   → ShardedNanosql              │
                                 └────────────────────────────────┘
```

| Repository | Role inside nanosql |
|---|---|
| [sql-query-parser](https://github.com/juanmmm21/sql-query-parser) | Parses every statement into a typed AST; its lexer also powers REPL statement splitting and `BEGIN/COMMIT/ROLLBACK` detection; its `CaretErrorFormatter` renders syntax errors with exact line/column carets. |
| [cost-based-query-optimizer](https://github.com/juanmmm21/cost-based-query-optimizer) | Turns every `SELECT` (including the synthetic selects that drive `UPDATE`/`DELETE`) into a cost-optimal physical plan, using live row counts and the indexes registered by `CREATE INDEX`. |
| [vectorized-execution-engine](https://github.com/juanmmm21/vectorized-execution-engine) | Executes every plan over columnar NumPy batches: scans, filters, projections, all four join types, hash aggregates, sorts, limits — under a shared memory budget. |
| [mvcc-transaction-manager](https://github.com/juanmmm21/mvcc-transaction-manager) | Owns transaction lifecycle and visibility: snapshots, first-committer-wins write conflicts, serializable write-skew detection, version garbage collection. |
| [lock-manager-deadlock-detector](https://github.com/juanmmm21/lock-manager-deadlock-detector) | Table-level two-phase locking that fences DDL: every statement holds SHARED locks on the tables it touches until commit; `CREATE TABLE`/`CREATE INDEX` require EXCLUSIVE. |
| [write-ahead-log-recovery](https://github.com/juanmmm21/write-ahead-log-recovery) | Durability: every storage mutation of a commit is logged (fsync per record) before it touches the backend; recovery on open performs ARIES-style redo/undo; periodic checkpoints truncate the log. |
| [bplus-tree-storage-engine](https://github.com/juanmmm21/bplus-tree-storage-engine) | Default storage backend: MVCC version chains persisted as ordered keys in a disk-backed B+Tree behind a bounded LRU buffer pool. |
| [lsm-tree-engine](https://github.com/juanmmm21/lsm-tree-engine) | Alternative storage backend (`backend="lsm"`): the same key space over a memtable + SSTables + compaction engine. |
| [raft-replication-log](https://github.com/juanmmm21/raft-replication-log) | Optional: `ReplicatedNanosql` replicates SQL commands through quorum consensus; each Raft state machine *is* a nanosql database, and Raft snapshots serialize the whole database. |
| [distributed-query-router](https://github.com/juanmmm21/distributed-query-router) | Optional: `ShardedNanosql` hash-partitions rows across independent nanosql databases and fans out reads, with correct distributed aggregate merging (weighted `AVG`). |

The parser, optimizer and executor deliberately never import each other, so
each redefines the AST/plan node subset it needs. `nanosql/translate.py` is
the only place where the three hierarchies coexist: it converts parser AST →
optimizer AST and optimizer physical plan → executor plan mechanically, and
verifies at import time that the mirrored enums have not diverged.

## Architecture

A database is a **directory** containing:

```text
mydata.db/
├── catalog.json    # schema: tables, columns, indexes, backend name (atomic writes)
├── wal.log         # write-ahead log (binary, CRC32 per record, fsync per append)
└── data.btree      # B+Tree page file        — or lsm/ directory for the LSM backend
```

* **Row storage.** Each MVCC row version is one key/value pair:
  `<table>\x1f<zero-padded rowid> \x00 <commit_seq big-endian>` →
  `[format version][flags][created_by][JSON row payload]`. Lexicographic key
  order therefore equals logical order, which both backends exploit for
  range scans. Corruption detection is delegated to the storage engines'
  checksummed pages/blocks.
* **Write path.** Writes buffer inside the MVCC transaction. On commit, the
  manager validates conflicts, then materializes each version through
  `DurableRowStore`, which logs to the WAL *before* every backend write —
  the write-ahead invariant holds even if the buffer pool evicts a page
  mid-transaction, because the WAL fsyncs per record.
* **Read path.** A `SELECT` builds statistics from live row counts,
  optimizes, translates the plan, and executes it against
  `MvccTableSource`, which materializes the transaction's visible snapshot
  into columnar batches.
* **UPDATE/DELETE.** Row identification reuses the full read pipeline: the
  engine plans a synthetic `SELECT __rowid__ [, assignment expressions]`
  with the same `WHERE`, so predicate evaluation (vectorized) and SET
  expressions run through the optimizer and executor, never through a
  second interpreter.
* **Subqueries.** Uncorrelated subqueries are evaluated bottom-up by
  materialization *before* translation to the optimizer (the same pipeline
  stage where `?` parameters are bound): each `(SELECT ...)` is an
  independent statement executed recursively under the *same*
  transaction/snapshot as its outer query, and its result is substituted
  into the AST — a list of literals for `IN (SELECT ...)`, a single literal
  for the scalar position. The optimizer and executor then run unchanged,
  with no notion that subqueries ever existed; nesting falls out of the
  recursion. The honest cost: an `IN` subquery's result is materialized
  in memory as a literal list, proportional to the rows it projects — fine
  for the selective subqueries this engine targets, and the reason the
  planned-semi-join alternative is documented as the natural evolution
  rather than implemented. Table locks cover subquery tables too, at any
  nesting depth.
* **Concurrency model.** MVCC resolves row conflicts optimistically
  (first-committer-wins); the lock manager is used only at table
  granularity to fence DDL against in-flight transactions. Statement
  execution within one process is serialized by an engine lock; connections
  from multiple threads are safe, one transaction per connection.
* **Dependency strategy.** The ten components are declared in
  `pyproject.toml` as PEP 508 direct references
  (`name @ git+https://github.com/juanmmm21/<name>.git`), so
  `pip install git+https://github.com/juanmmm21/nanosql.git` resolves the
  whole ecosystem from source. The two distribution components live behind
  the `[distributed]` extra. None of the ten is vendored or re-implemented.

## Installation

Requires Python ≥ 3.11.

```bash
pip install "git+https://github.com/juanmmm21/nanosql.git"

# with the optional distributed mode (raft + shard router):
pip install "nanosql[distributed] @ git+https://github.com/juanmmm21/nanosql.git"
```

From a source checkout:

```bash
pip install -e ".[distributed,dev]"
```

## CLI / REPL usage

```text
$ nanosql mydata.db
nanosql 0.1.0 — motor: btree
Escribe SQL terminado en ";", ".help" para ayuda, ".quit" para salir.
nanosql> CREATE TABLE t (id INTEGER PRIMARY KEY, nombre VARCHAR(20));
OK (0 filas afectadas, 0.3 ms)
nanosql> INSERT INTO t VALUES (1, 'ada'), (2, 'grace');
OK (2 filas afectadas, 0.3 ms)
nanosql> SELECT * FROM t ORDER BY id;
t.id | t.nombre
-----+---------
1    | ada
2    | grace
(2 filas, 0.3 ms)
nanosql> BEGIN;
OK (0.0 ms)
nanosql*> DELETE FROM t;
OK (2 filas afectadas, 0.1 ms)
nanosql*> ROLLBACK;
OK (0.0 ms)
nanosql> .quit
```

The REPL runs in autocommit until `BEGIN` (the `*` prompt marks an open
transaction). Non-interactive usage:

```bash
nanosql mydata.db -c "SELECT COUNT(*) FROM t;"   # run statements and exit
nanosql --benchmark                              # full benchmark, both backends
nanosql mydata.db --backend lsm                  # create with the LSM backend
nanosql mydata.db --isolation serializable
```

## Programmatic usage

```python
import nanosql

# One engine per path per process: multiple connections share it and get
# real transaction isolation between each other.
a = nanosql.connect("shop.db")  # repeatable_read (default)
b = nanosql.connect("shop.db", isolation_level="serializable")

a.execute("CREATE TABLE stock (sku INTEGER PRIMARY KEY, qty INTEGER NOT NULL)")
a.execute("INSERT INTO stock (sku, qty) VALUES (1, 10)")
a.commit()

a.execute("UPDATE stock SET qty = qty - 1 WHERE sku = 1")
b.execute("UPDATE stock SET qty = qty - 2 WHERE sku = 1")
a.commit()
try:
    b.commit()  # first committer wins
except nanosql.TransactionConflictError:
    ...  # b was rolled back; retry the transaction

with nanosql.connect(":memory:") as conn:  # commits on clean exit
    conn.execute("CREATE TABLE t (x INTEGER)")
    conn.execute("INSERT INTO t (x) VALUES (?)", (1,))  # qmark parameters
    conn.cursor().executemany("INSERT INTO t (x) VALUES (?)", [(2,), (3,), (4,)])
    print(conn.execute("EXPLAIN SELECT x FROM t WHERE x > ?", (2,)).fetchall())
```

Parameters are bound onto the typed AST (never re-interpolated as SQL
text), so quotes and semicolons inside a string parameter are data, not
syntax — injection-safe by construction.

Exceptions follow the DB-API hierarchy (`ProgrammingError`, `DataError`,
`IntegrityError`, `OperationalError`, `NotSupportedError`,
`TransactionConflictError`, …), all rooted at `nanosql.NanosqlError`.
Syntax errors carry the parser's caret-formatted context:

```text
Syntax error at line 1, column 8: se esperaba una expresión
SELECT FROM t
       ^
```

### Supported SQL subset

`CREATE TABLE` (INTEGER/FLOAT/VARCHAR/TEXT/BOOLEAN families, `PRIMARY KEY`,
`NOT NULL`, `UNIQUE`, `DEFAULT`, `IF NOT EXISTS`), `CREATE [UNIQUE] INDEX`,
`INSERT` (multi-row, with or without a column list), `SELECT [DISTINCT]`
with `INNER/LEFT/RIGHT/FULL JOIN`, `WHERE`, `GROUP BY`, `HAVING`,
`COUNT/SUM/AVG/MIN/MAX` (plus `DISTINCT` inside aggregates), `ORDER BY`,
`LIMIT/OFFSET`, `UPDATE ... SET` with expressions, `DELETE`, qmark (`?`)
query parameters, **uncorrelated subqueries** — both scalar
(`WHERE v > (SELECT AVG(v) FROM t)`, zero rows ⇒ `NULL`, more than one row ⇒
`DataError`) and membership (`[NOT] IN (SELECT one_column FROM ...)`, with
correct three-valued `NOT IN` semantics when the subquery result contains
`NULL`s), nestable and parameterizable — and driver-level
`BEGIN/COMMIT/ROLLBACK` plus `EXPLAIN <select>`.

Documented limitations (typed `NotSupportedError`/`ProgrammingError`, never
silent wrong answers): no **correlated** subqueries (a subquery column that
only resolves against the outer query is detected statically and rejected
with a clear `NotSupportedError`, never a cryptic downstream error), no
derived tables (`FROM (SELECT ...)`), no `EXISTS`, no `EXPLAIN` over queries
containing subqueries (materialization would have to execute them to plan),
no composite primary keys (use a multi-column `UNIQUE` index), no
`DATE/TIME/TIMESTAMP` column types. With
`SELECT DISTINCT`, `ORDER BY` may only reference columns of the select list
(standard SQL: a hidden sort key would change the deduplication).
Identifiers are case-sensitive. DDL implicitly commits the open transaction
(MySQL-style) — that is what lets it take the EXCLUSIVE table lock without
self-deadlocking. `UNIQUE` constraints are enforced twice: per statement
against the transaction's snapshot, and again at commit against the latest
committed state (commits are serialized by the engine lock), so two
concurrent transactions inserting the same "unique" value cannot both
succeed — the second commit fails with `IntegrityError`.

## Backend configuration

```python
nanosql.connect("a.db")  # B+Tree (default)
nanosql.connect("b.db", backend="lsm")  # LSM tree
nanosql.connect("a.db")  # reopen: backend read from catalog
nanosql.connect(":memory:")  # in-memory B+Tree, no WAL, private
```

The backend is chosen when the database is created and recorded in
`catalog.json`; reopening with a different explicit backend fails with a
typed error. `:memory:` databases support only the B+Tree backend (the LSM
engine needs a directory for its SSTables) and are never shared between
connections.

With the LSM backend every commit pays two fsyncs — the ecosystem WAL plus
the LSM's own internal WAL. That redundancy is deliberate: both backends
share a single recovery path, and it is never incorrect.

### Optional distributed mode

```python
from nanosql.distributed import ReplicatedNanosql, ShardedNanosql
from distributed_query_router.models import AggregateFunction

# Raft-replicated cluster: every committed SQL command is applied by all replicas.
cluster = ReplicatedNanosql(num_nodes=3, seed=7)
cluster.execute("CREATE TABLE m (id INTEGER PRIMARY KEY, v FLOAT NOT NULL)")
cluster.execute("INSERT INTO m (id, v) VALUES (1, 1.5)")
cluster.query("SELECT COUNT(*) FROM m")  # strong read (leader)
cluster.crash_node(cluster.leader())  # quorum survives
cluster.execute("INSERT INTO m (id, v) VALUES (2, 2.5)")

# Hash-sharded deployment with distributed aggregate merging.
sharded = ShardedNanosql(num_shards=4, seed=7)
sharded.create_table("CREATE TABLE m (id INTEGER PRIMARY KEY, v FLOAT NOT NULL)")
sharded.insert("m", {"id": 1, "v": 9.5}, shard_key="id")
result = sharded.aggregate(
    "m", aggregates=[(AggregateFunction.AVG, "v", "media")]
)  # AVG merged from per-shard (sum, count) partials — exact, never avg-of-avgs
```

Sharded `select()` rejects subqueries with `NotSupportedError`: each shard
would evaluate the subquery against its local rows only, silently producing
a wrong global membership set (replicated mode has no such restriction —
every node holds the full database).

Both are in-process simulations (no sockets), matching the scope of their
underlying repositories: the goal is real protocol integration —
quorum commits, leader failover, database-as-state-machine snapshots,
partial results on shard failure — not multi-machine deployment.

## PostgreSQL wire protocol (`nanosql serve`)

`nanosql serve` exposes a database over the PostgreSQL frontend/backend
protocol v3, so stock PostgreSQL clients connect to it directly:

```bash
nanosql serve mydata.db --port 5432                        # trust auth, 127.0.0.1
NANOSQL_PASSWORD=s3cret nanosql serve mydata.db --auth scram-sha-256 --user ana
nanosql serve mydata.db --auth scram-sha-256 --user ana:s3cret --user bob:hunter2

psql -h 127.0.0.1 -p 5432 -U ana -d mydata
```

```python
import psycopg

with psycopg.connect("host=127.0.0.1 port=5432 user=ana dbname=mydata") as conn:
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO t (id, name) VALUES (%s, %s)", (1, "ada"))
    conn.commit()

import sqlalchemy as sa

engine = sa.create_engine("postgresql+psycopg://ana@127.0.0.1:5432/mydata")
```

The server is pure Python (`nanosql.pgwire`, standard library only): one
thread per connection, each session with its own nanosql `Connection` over
the shared engine, so concurrent sessions get real MVCC isolation from each
other exactly like embedded connections do.

**Verified clients** (automated tests start a real server on an ephemeral
port and drive it with each client — `tests/test_pgwire_clients.py`):
psql 16.15, psycopg 3.3.7 (binary wheel, bundled libpq 18, `pq.version()` = 180006) and SQLAlchemy
2.1.7 Core with `postgresql+psycopg`: create tables (including
`metadata.create_all()`), insert with parameters (single and `executemany`),
query, update, delete, `\dt` and `\d`.

### What is supported

| Area | Support |
|---|---|
| Startup | Protocol 3.0 (`NegotiateProtocolVersion` for newer minor versions); `SSLRequest`/`GSSENCRequest` answered `N` (plaintext); `ParameterStatus` (`server_version` = `16.0`, UTF8, ISO dates, UTC); `BackendKeyData`. |
| Authentication | `trust`, and `SCRAM-SHA-256` (RFC 5802/7677; verified against the RFC 7677 test vector). Passwords are kept only as SCRAM verifiers. Unknown users go through a full fake exchange, so user names cannot be probed. |
| Simple query | Multiple `;`-separated statements per message; `EmptyQueryResponse`. |
| Extended query | `Parse`/`Bind`/`Describe` (statement and portal)/`Execute` (with row limits and `PortalSuspended`)/`Sync`/`Flush`/`Close`, named and unnamed statements and portals, pipelining, error → skip-until-`Sync`. `$n` parameters are bound onto nanosql's typed AST (never interpolated); a `$n::type` cast fixes the parameter type, as in Postgres. |
| Types | `INTEGER` ↔ `int8`, `FLOAT` ↔ `float8`, `TEXT`/`VARCHAR` ↔ `text`, `BOOLEAN` ↔ `bool`, in text format; binary format for those types too. Parameters arriving as `int2/int4/int8/oid/float4/float8/numeric/bool/text/varchar/unknown` are converted. |
| Errors | `ErrorResponse` with a real SQLSTATE: every nanosql error carries one (`42P01` undefined table, `42703` undefined column, `42601` syntax, `23505`/`23502` unique/not-null, `22012` division by zero, `40001` serialization failure, `40P01` deadlock, `25P02` failed transaction block, ...), so psycopg raises `UndefinedTable`, `UniqueViolation`, `SerializationFailure`, etc. |
| Transactions | `BEGIN`/`START TRANSACTION` with `ISOLATION LEVEL` and `READ ONLY` (enforced), `COMMIT`/`END`, `ROLLBACK`/`ABORT`, `AND CHAIN`, `SET TRANSACTION`, `SET SESSION CHARACTERISTICS`, the `I`/`T`/`E` transaction states (after an error only `ROLLBACK` is accepted). Default isolation is `read committed`, like Postgres (`--isolation` changes it). |
| Session | `SET`/`RESET`/`SHOW` (including `SHOW ALL`, `SHOW transaction isolation level`), `DISCARD ALL`, `DEALLOCATE`, `current_setting()`/`set_config()`. |
| Introspection | `SELECT version()`, `current_schema()`, `current_database()`, `current_user`, `SELECT 1`; the `pg_catalog` queries behind psql `\dt`, `\d <table>` (columns, types, nullability, defaults, primary key/unique/plain indexes), `\dn`, `\l`, `\du`; SQLAlchemy `has_table`/`create_all`; psycopg `TypeInfo` lookups; basic `information_schema.tables/columns`. |

### What is not supported (and how it fails)

* **pg_catalog is emulated, not queryable.** Introspection queries are
  recognised by shape (which system relations they touch, which
  `relname`/`oid`/`relkind` filters they apply) and answered from nanosql's
  own catalog. Unrecognised catalog queries return an empty result with
  the right columns (that is how psql shows "no policies/triggers/foreign
  keys"). Arbitrary SQL over `pg_catalog` is not possible.
* **SQL dialect = nanosql's subset.** Anything outside it fails with
  `42601`/`0A000` exactly as in the embedded driver: no `RETURNING`
  (so SQLAlchemy `SERIAL`/autoincrement primary keys do not work — use
  `autoincrement=False` and explicit ids), no `CAST`/`::` on columns (casts
  on parameters and literals are applied), no date/time, JSON, arrays or
  `bytea` columns, identifiers are case-sensitive (no lower-case folding).
* **`COPY`, `LISTEN/NOTIFY`, TLS, cancellation.** `COPY` messages are
  rejected with `0A000`; `CancelRequest` is accepted and ignored (the
  executor has no cancellation points); no TLS.
* **Savepoints are markers.** `SAVEPOINT`/`RELEASE` work; `ROLLBACK TO` is
  accepted only when the transaction wrote nothing after the savepoint
  (then rolling back to it is exactly a no-op, and it recovers a block
  after a failed read — what psycopg and SQLAlchemy use it for). Otherwise
  it fails with `0A000`; nanosql has no nested transactions.
* **DDL commits.** As in the embedded driver, `CREATE TABLE`/`CREATE INDEX`
  inside a transaction block commits the work before it; the server opens
  a new transaction so the rest of the block stays atomic, and sends a
  `NOTICE` saying so.
* **Implicit transactions.** Outside `BEGIN`, every statement commits on
  its own — including each statement of a multi-statement simple query and
  each `Execute` of an extended-protocol batch (Postgres would group them
  until `Sync`).
* **One database per server.** Any database name in the startup packet
  connects to the database being served. SASLprep is reduced to NFKC
  normalisation (irrelevant for ASCII passwords).

## Development

```bash
python -m venv .venv && .venv/bin/pip install -e ".[distributed,dev]"
.venv/bin/pytest              # 232 tests (psql/psycopg/SQLAlchemy ones skip if absent)
.venv/bin/ruff check . && .venv/bin/ruff format --check .
.venv/bin/mypy --strict src/
```

The test suite covers the DB-API surface, end-to-end SQL, isolation levels
and conflicts (including write skew under `serializable`), DDL locking,
typed errors, clean reopens, crash recovery (including rebuilding the
database from the WAL alone after deleting the data file), backend parity,
subquery semantics (three-valued `NOT IN` with `NULL`s, empty-set membership,
scalar cardinality errors, correlation detection, shared MVCC snapshot),
the distributed mode, and seeded property-based random workloads verified
against an independent reference model on both backends.

All ten dependencies ship a `py.typed` marker, so `mypy --strict` checks
the integration layer against their real signatures with no overrides.

## Benchmarks

`nanosql --benchmark`, Apple Silicon, Python 3.14, 2 000-row table, WAL
fsync per commit. Full pipeline (parse → optimize → execute → MVCC →
WAL → storage) on every operation:

| backend | operation | ops | seconds | ops/s |
|---|---|---:|---:|---:|
| btree | insert (commit every 200 rows) | 2000 | 0.339 | 5900.7 |
| btree | point select by pk | 100 | 0.594 | 168.3 |
| btree | aggregate group-by (full table) | 20 | 0.138 | 145.1 |
| btree | join + aggregate + top-10 | 10 | 0.112 | 89.6 |
| btree | bulk update (with expression) | 210 | 0.069 | 3051.0 |
| lsm | insert (commit every 200 rows) | 2000 | 0.326 | 6137.8 |
| lsm | point select by pk | 100 | 0.509 | 196.6 |
| lsm | aggregate group-by (full table) | 20 | 0.120 | 166.2 |
| lsm | join + aggregate + top-10 | 10 | 0.092 | 108.3 |
| lsm | bulk update (with expression) | 210 | 0.047 | 4515.3 |

Honest reading of these numbers:

* **Writes are fsync-bound and healthy** (~6 000 rows/s with per-commit
  durability; the LSM backend wins as expected from its append-only design).
* **Reads improved ~20x over the first release.** Snapshot materialization
  originally performed one version-chain lookup per row through the
  `RowStore` protocol; the `BulkRowStore` extension added to
  `mvcc-transaction-manager` (one ordered range scan over the whole store
  per snapshot, contributed back upstream from this project's benchmark
  findings) removed that cost. What remains per query is decoding the
  visible rows' JSON payloads into columnar batches — a point select on a
  2 000-row table now costs ~6 ms instead of ~120 ms.
* Secondary indexes influence *plan* selection (visible via `EXPLAIN`) but
  execute as filtered scans — the executor's documented
  `IndexScanOperator` contract.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `OperationalError: timeout esperando el lock de la tabla ...` | DDL waiting on a table that an open transaction has read/written. Commit or roll back that transaction; tune with `connect(..., lock_timeout=...)`. |
| `TransactionConflictError` on commit | Another transaction committed first on the same rows (or a serializable write-skew was detected). The transaction is already rolled back — retry it whole. |
| `ProgrammingError: ya hay una transacción activa` on `BEGIN` | A previous statement opened the implicit transaction. `commit()`/`rollback()` first, or use `autocommit=True` and explicit `BEGIN` blocks. |
| `la base de datos se creó con el backend 'X'` | The backend is fixed at creation. Omit `backend=` when reopening. |
| `:memory:` with `backend="lsm"` fails | The LSM engine needs a directory for SSTables; in-memory databases are B+Tree only. |
| Database directory left after a crash | Nothing to do — the next `connect()` runs WAL recovery (redo committed, undo incomplete) and truncates the log. |
| Two OS processes on the same directory | Unsupported (no file locking): a single process may share the database across many connections/threads, but cross-process access corrupts the buffer pool's view. |

## License

MIT — see [LICENSE](LICENSE). © 2026 Juan Martos Cuevas (juanmmm21).
