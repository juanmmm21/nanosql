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
nanosql 0.2.0 — motor: btree
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
`NOT NULL`, `UNIQUE`, `DEFAULT`, `IF NOT EXISTS`, and table-level
`[CONSTRAINT name] PRIMARY KEY (..)`/`UNIQUE (..)` — a composite primary key
becomes NOT NULL columns plus a unique index `<table>_pkey`),
`CREATE [UNIQUE] INDEX`, `DROP TABLE [IF EXISTS]`, `INSERT` (multi-row, with
or without a column list, or `INSERT ... SELECT`), `SELECT [DISTINCT | ALL]`
with `INNER/LEFT/RIGHT/FULL JOIN`, comma joins and `CROSS JOIN`, `WHERE`,
`GROUP BY`, `HAVING`, `COUNT/SUM/AVG/MIN/MAX` (plus `DISTINCT`/`ALL` inside
aggregates), `ORDER BY` (expressions, names or output positions),
`LIMIT/OFFSET`, `UPDATE ... SET` with expressions, `DELETE`, qmark (`?`)
query parameters, **uncorrelated subqueries** — both scalar
(`WHERE v > (SELECT AVG(v) FROM t)`, zero rows ⇒ `NULL`, more than one row ⇒
`DataError`) and membership (`[NOT] IN (SELECT one_column FROM ...)`, with
correct three-valued `NOT IN` semantics when the subquery result contains
`NULL`s), nestable and parameterizable — `ANALYZE [table]`, and driver-level
`BEGIN/COMMIT/ROLLBACK` plus `EXPLAIN <select>`.

**Optimizer statistics.** The optimizer gets real per-column statistics:
exact distinct and null counts, plus 32-bucket equi-depth histograms for
numeric columns, computed over the latest committed version of every live
row (`src/nanosql/statistics.py`). They are cached per table and recomputed
automatically when a table has changed by more than 50 rows + 10 % since
its last analysis (PostgreSQL's autovacuum threshold); `ANALYZE [table]`
refreshes them on demand without touching the session's transaction.
Before this, every column was reported as unique (`distinct_count =
row_count`), which made equality filters on low-cardinality columns look
like one-row results and the cost model pick nested-loop joins.

Documented limitations (typed `NotSupportedError`/`ProgrammingError`, never
silent wrong answers): no **correlated** subqueries (a subquery column that
only resolves against the outer query is detected statically and rejected
with a clear `NotSupportedError`, never a cryptic downstream error), no
derived tables (`FROM (SELECT ...)`), no `EXISTS`, no `EXPLAIN` over queries
containing subqueries (materialization would have to execute them to plan),
no `SELECT` without `FROM`, no `CASE`/`CAST`/scalar functions, no
`UNION`/`INTERSECT`/`EXCEPT`, no views, no `DATE/TIME/TIMESTAMP` column
types. Arithmetic follows SQL: `INTEGER / INTEGER` truncates toward zero
(`-7 / 2 = -3`), `%` takes the dividend's sign, and division by zero raises
`DataError` (SQLSTATE `22012`). With
`SELECT DISTINCT`, `ORDER BY` may only reference columns of the select list
(standard SQL: a hidden sort key would change the deduplication).
Identifiers are case-sensitive. DDL implicitly commits the open transaction
(MySQL-style) — that is what lets it take the EXCLUSIVE table lock without
self-deadlocking. `UNIQUE` constraints are enforced twice: per statement
against the transaction's snapshot, and again at commit against the latest
committed state (commits are serialized by the engine lock), so two
concurrent transactions inserting the same "unique" value cannot both
succeed — the second commit fails with `IntegrityError`. Both checks go
through in-memory key indexes (`src/nanosql/uniqueness.py`) — one per
transaction, built from its snapshot and kept current with its own writes,
and one per table for the committed state — so their cost is proportional
to the rows written, not to the size of the table.

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

Both are in-process simulations (no sockets): the goal there is protocol
integration — quorum commits, leader failover, database-as-state-machine
snapshots, partial results on shard failure. The real multi-process
cluster is described next.

## Real cluster and chaos testing (`nanosql cluster`)

`nanosql cluster start --nodes 3` launches a real cluster: one process per
node (3–5, static membership), Raft over TCP with a persistent, fsynced
log per node (`raft-replication-log`'s `NetworkNode`/`DiskStorage`), and a
replicated nanosql database as the state machine, rebuilt on restart from
the node's snapshot and log.

```python
from nanosql.cluster import ClusterClient

# endpoints as printed by `nanosql cluster start`
client = ClusterClient([("127.0.0.1", 7201), ("127.0.0.1", 7202), ("127.0.0.1", 7203)])
client.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)")
client.execute("INSERT INTO kv (k, v) VALUES (?, ?)", (1, "a"))
client.query("SELECT v FROM kv WHERE k = ?", (1,))  # linearizable read
```

* **Finding the leader.** Any node answers a follower's
  `{"redirect": leader}`; the client follows it, rotates on connection
  errors or `retry`, and raises `IndeterminateError` when its deadline
  passes without a definitive answer (the operation may or may not have
  happened). Deterministic SQL errors (e.g. a unique violation) come back
  as `ClusterSqlError` with their SQLSTATE.
* **Exactly-once retries.** Every operation carries `(client id,
  sequence)`; retries reuse it, and the replicated state machine keeps a
  session table (Raft paper §8) that returns the stored result of an
  already-applied sequence instead of applying it again — otherwise a
  retried `UPDATE ... SET n = n + 1` after a lost response would count
  twice.
* **Linearizable reads.** Reads go through the log (one quorum round
  trip); a deposed leader cannot serve stale data. `--unsafe-local-reads`
  deliberately breaks this (reads served from local state) and exists only
  to prove that the checker catches the resulting violations.

### Chaos harness and linearizability checker

`nanosql cluster chaos --nodes 3 --duration 600` starts the cluster behind
**userspace TCP proxies** — one per directed link, so node *i* reaches peer
*j* through proxy *i→j* — and runs concurrent clients issuing a SQL
workload of **registers** (`UPDATE registers SET v = ? WHERE k = ?` /
`SELECT v ...`) and **counters** (`UPDATE counters SET n = n + 1 WHERE k =
?` / `SELECT n ...`) while a nemesis injects one fault every 2–5 s, held
for 1.5–4 s:

* `kill-leader` — `SIGKILL` of the current leader, restarted later from its
  data directory;
* `partition` — random two-group split by blocking proxies;
* `partition-leader` — the leader isolated from everyone;
* `pause` — `SIGSTOP`/`SIGCONT` of a random node.

Every operation is recorded with its invocation and response instants (a
single monotonic clock in the harness). The history is then checked by
`nanosql.cluster.linearizability`, a self-contained implementation of the
**Wing & Gong** algorithm with **Lowe's (linearized-set, state) cache** —
the variant Porcupine uses — per key (P-compositionality), with register
and counter models; operations whose outcome is unknown get a return time
of +∞ and unanswered reads are dropped.

The checker is tested on synthetic histories — concurrent ones, pending
writes, and **injected violations** (a stale read after a completed
write, a lost increment, broken real-time order, 150 random linearizable
histories that must pass until a never-written value is injected) — and on
a real cluster: `test_cluster.py` isolates the leader of a cluster running
with `--unsafe-local-reads`, writes on the majority and reads from the old
leader, and the checker **flags the stale read**; the same scenario with
reads through the log is linearizable.

### Results

Measured in this session (Intel Xeon @ 2.10 GHz, 4 vCPU, 15 GiB, Linux
6.18, Python 3.13.18), 6 clients over 4 register and 4 counter keys,
faults as above; full reports in `docs/results/`:

| cluster | duration | operations (all definite) | faults injected | linearizable | checker time |
|---|---:|---:|---|---|---:|
| 3 nodes, seed 101 | 630 s | 188,686 | 22 kill -9 of the leader, 27 partitions, 26 leader isolations, 27 SIGSTOP pauses | **yes**, 8/8 keys | 1.3 s |
| 5 nodes, seed 202 | 630 s | 155,431 | 19 kill -9 of the leader, 30 partitions, 23 leader isolations, 26 SIGSTOP pauses | **yes**, 8/8 keys | 0.9 s |

No operation ended indeterminate in either run (every client retry
eventually got a definite answer within its 10 s deadline); p50/p99 latency
was 13.9/44.1 ms (3 nodes) and 18.2/54.4 ms (5 nodes), with maxima of ~4 s
during fail-overs. Throughput (300 and 247 ops/s) is that of a pure-Python
stack with an fsync per Raft append on every node; the 5-node run shared
the machine with a `kill -9` campaign for part of its duration. CI runs a
90 s chaos job on every push and fails if the history is not
linearizable.

Limits, stated plainly: static membership only (no joint consensus), no
leases or read-index (reads pay a log round trip), the userspace proxy
models partitions and connection resets but not packet delay or
reordering, and the harness and all nodes share one machine and one
clock.

## Crash safety under faults

Two layers of evidence, both measured in this session:

* **Disk-level faults, in `write-ahead-log-recovery`.** The WAL now does all
  its I/O through a pluggable file layer, and its test suite crashes it
  before *every* I/O operation of small workloads against a disk model with
  sector-granular torn writes, reordering of unsynchronised writes, a lying
  `fsync` and non-durable directory metadata, checking every possible
  surviving subset (183 crash points / 404 disk images exhaustively, plus
  random crashes on 60-transaction workloads and crash-recover-continue
  restarts). It found and fixed four real bugs: no directory `fsync` after
  creating or compacting the log, appending behind an unreadable tail (and
  failing to reopen a log whose last record had a bad CRC), and undoing
  aborted or interrupted transactions on top of later commits (now handled
  with compensation records). See that repository's README.
* **Process-level `kill -9`, here.** `python -m nanosql.crashtest campaign`
  (and `tests/test_kill9.py`, 150 kills per backend on every test run)
  starts a real writer process that commits multi-row transactions — batch
  *b* inserts `1 + b % 3` rows into `ledger` and adds the same number to
  the single row of `totals`, in one transaction, printing `ack b` once
  `commit()` returns — with checkpoints and version GC forced every few
  commits, kills it with `SIGKILL` after a random delay (0–400 ms), reopens
  the database (real recovery), checks the invariants and restarts the
  writer where the committed data ends. Invariants: every acknowledged
  batch is present; every present batch has exactly its rows and
  `totals.n` equals the row count (nothing half-applied); batches are
  exactly `1..m` (no holes); the database always opens.

| backend | kills | batches acknowledged | batches recovered | violations | time |
|---|---:|---:|---:|---:|---:|
| btree | 300 | 1,430 | 1,430 | 0 | 146 s |
| lsm | 300 | 997 | 997 | 0 | 151 s |

(A run can recover one more batch than it acknowledged: a commit that
reached disk but was killed before printing its `ack` — allowed.) The same
campaign against the previous `write-ahead-log-recovery` release (100
kills per backend) also passed: `SIGKILL` does not lose the operating
system's page cache, so it cannot tear writes; the WAL bugs above need a
power-loss-style fault, which is what the fault-injection layer models.
Reports in `docs/results/kill9-*.json`.

* **A bug the random campaign missed, and CI found.** Random kills almost
  never land inside a checkpoint, the only moment the B+Tree writes its
  dirty pages, one after another. CI's run of `test_kill9.py` did, and the
  database no longer opened (`page_type desconocido: 0`): a parent already
  on disk pointed to a page created by a split that was still the zeros
  `allocate_page` writes. A logical WAL cannot replay `put`s through a tree
  like that. `bplus-tree-storage-engine` now has a crash-consistent mode —
  eviction never writes dirty pages, and `flush()` is atomic through a
  double-write journal — and nanosql opens its on-disk B+Tree with it.
  `tests/test_kill_during_flush.py` now kills the writer deterministically
  before *every* in-place page write of a checkpoint (31 points) and
  requires exactly the committed rows after reopening: without the fix 30
  of the 31 left a database that did not open; with it, all 31 pass. The
  campaign above was re-run after the fix.

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

## SQL conformance (sqllogictest)

`python -m nanosql.sqllogictest` runs files in the
[sqllogictest](https://www.sqlite.org/sqllogictest/) format of SQLite
(statement ok/error, `query` with `I`/`R`/`T` types, `nosort`/`rowsort`/
`valuesort`, `hash-threshold` with MD5 result hashing, `skipif`/`onlyif`,
`halt`) against an in-memory nanosql database, formatting values exactly as
the reference `sqllogictest.c` does for SQLite. The runner identifies itself
as the database `nanosql`, so `onlyif sqlite`/`onlyif mysql` records are
skipped and the portable ones run.

The corpus is vendored under `tests/sqllogictest/corpus/` (public domain,
gzip, byte-identical): **all of `select1`–`select5`** plus a deterministic
sample of the rest — the first file of every `index/<kind>/10/` and
`random/<kind>/` directory and four `evidence/` files. Origin, mirror commit
and per-file SHA-256 are in
[`tests/sqllogictest/SOURCE.md`](tests/sqllogictest/SOURCE.md)
(`www.sqlite.org` was blocked from the vendoring session, so the files come
from the GitHub mirror `gregrahn/sqllogictest` at `c93ac41`).

Every failing record is classified:

* **unsupported** — nanosql rejects it with a typed error for SQL outside
  the documented subset (`NotSupportedError`, or SQLSTATE `42601`/`0A000`),
  division by zero (`22012`: nanosql raises like PostgreSQL, SQLite returns
  `NULL`), or the record fails only because an earlier unsupported statement
  left a table missing or unpopulated (a *cascade*, attributed to its cause);
* **bug** — a wrong result: different values, a different column count,
  success where an error was expected, or another error on in-subset SQL;
* **resource** — over the time (`--timeout`) or memory (`--memory-limit-mb`)
  limit.

### Results, before and after

"Before" is nanosql at the end of the wire-protocol work (commit `5601f64`,
components at their `main` heads); "after" is this branch, with every bug
the run surfaced fixed in the component that owned it. Measured in this
session on an Intel Xeon @ 2.10 GHz (4 vCPU, 15 GiB, Linux 6.18), Python
3.13.18, NumPy 2.5.4; components at `sql-query-parser@bd97148`,
`cost-based-query-optimizer@e46ff59`, `vectorized-execution-engine@63db698`.
Raw numbers: `tests/sqllogictest/before.json` and
`tests/sqllogictest/results.json`.

| file | records run | before: passed | before % | after: passed | after % | unsupported (after) | bug (after) |
|---|---:|---:|---:|---:|---:|---:|---:|
| select1 | 1031 | 196 | 19.01 | 239 | 23.18 | 792 | 0 |
| select2 | 1031 | 217 | 21.05 | 217 | 21.05 | 814 | 0 |
| select3 | 3351 | 723 | 21.58 | 723 | 21.58 | 2628 | 0 |
| select4 | 3857 | 1042 | 27.02 | 2856 | 74.05 | 1001 | 0 |
| select5 | 1436 | 704 | 49.03 | 1436 | 100.00 | 0 | 0 |
| evidence_in1 | 132 | 20 | 15.15 | 27 | 20.45 | 105 | 0 |
| evidence_in2 | 53 | 44 | 83.02 | 44 | 83.02 | 9 | 0 |
| evidence_slt_lang_aggfunc | 5 | 5 | 100.00 | 5 | 100.00 | 0 | 0 |
| evidence_slt_lang_update | 27 | 24 | 88.89 | 24 | 88.89 | 3 | 0 |
| index_between_10_slt_good_0 | 10033 | 4762 | 47.46 | 10030 | 99.97 | 3 | 0 |
| index_commute_10_slt_good_0 | 10034 | 3895 | 38.82 | 10027 | 99.93 | 7 | 0 |
| index_delete_10_slt_good_0 | 10730 | 5994 | 55.86 | 10071 | 93.86 | 659 | 0 |
| index_in_10_slt_good_0 | 10035 | 5300 | 52.82 | 10032 | 99.97 | 3 | 0 |
| index_orderby_10_slt_good_0 | 10053 | 3899 | 38.78 | 10047 | 99.94 | 6 | 0 |
| index_orderby_nosort_10_slt_good_0 | 10053 | 3343 | 33.25 | 10043 | 99.90 | 10 | 0 |
| index_random_10_slt_good_0 | 10032 | 5074 | 50.58 | 8651 | 86.23 | 1381 | 0 |
| index_view_10_slt_good_1 | 7135 | 402 | 5.63 | 1094 | 15.33 | 6041 | 0 |
| random_aggregates_slt_good_0 | 10012 | 5725 | 57.18 | 8705 | 86.95 | 1307 | 0 |
| random_expr_slt_good_0 | 10012 | 12 | 0.12 | 12 | 0.12 | 10000 | 0 |
| random_groupby_slt_good_0 | 10012 | 5596 | 55.89 | 9622 | 96.10 | 390 | 0 |
| random_select_slt_good_0 | 10012 | 5900 | 58.93 | 9587 | 95.76 | 425 | 0 |
| select1–5 | 10706 | 2882 | 26.92 | 5471 | 51.10 | 5235 | 0 |
| sample of the rest | 118370 | 49995 | 42.24 | 98021 | 82.81 | 20349 | 0 |
| **total** | 129076 | 52877 | 40.97 | 103492 | 80.18 | 25584 | 0 |

Bugs found and fixed (all were silent wrong answers or spurious errors on
valid SQL):

| Component | Bug |
|---|---|
| cost-based-query-optimizer | `IN`, `BETWEEN`, `LIKE` and `IS NULL` conjuncts of a multi-table query touched no table and were **dropped**: `SELECT x FROM a JOIN b ON a.x = b.y WHERE a.z IN (1, 2)` returned every joined row. |
| cost-based-query-optimizer | Constant join conjuncts (`ON 1 = 0`, `ON NULL IS NOT NULL`) were dropped, turning the join into a cartesian product. |
| cost-based-query-optimizer | Repeated `GROUP BY` keys duplicated an output column and the query failed. |
| vectorized-execution-engine | `INTEGER / INTEGER` returned a real (`7 / 2 = 3.5`); it now truncates toward zero like the SQL standard, PostgreSQL and SQLite, and `%` takes the dividend's sign. |
| vectorized-execution-engine | `GROUP BY col, t.col` (one column named twice) failed with a schema conflict. |
| vectorized-execution-engine | The nested-loop join materialised every row pair of a batch at once (a `select4` cross join reached 14 GB and was OOM-killed); pairs are now generated in bounded steps. |
| sql-query-parser | `ALL` was an identifier, so `SELECT ALL - x FROM t` parsed as the column `"ALL"` minus `x` and `SELECT ALL (x)` as a call to a function `ALL`. |
| nanosql | `ORDER BY 1` sorted by the constant 1 instead of by the first output column. |
| nanosql | `SELECT *` followed the plan's column order (after join reordering, or the `GROUP BY` key order) instead of the `FROM` order. |
| nanosql | Repeated output names (`SELECT 1 AS c, x AS c`) failed instead of being returned. |

Features added because the corpus depends on them, each in its owning
component: `INSERT INTO t SELECT ...` and `DROP TABLE [IF EXISTS]` (parser +
nanosql), comma joins and `CROSS JOIN` (parser: they are `INNER JOIN ... ON
TRUE`).

What remains is **unsupported**, not wrong: `SELECT` without `FROM`
(`SELECT 1 IN (2)`, nearly all of `random/expr`), `CASE`, correlated and
`EXISTS` subqueries and scalar functions such as `abs`/`coalesce` (most of
`select1`–`select3`), compound selects (`UNION`/`INTERSECT`/`EXCEPT`, most of
the rest of `select4`), views and derived tables (`index/view`), `CAST`.

CI (`sqllogictest` job) runs the whole corpus and fails if any file passes
fewer records than `tests/sqllogictest/results.json`:

```bash
python -m nanosql.sqllogictest tests/sqllogictest/corpus/*.test.gz \
    --json out.json --check tests/sqllogictest/results.json
```

## Development

```bash
python -m venv .venv && .venv/bin/pip install -e ".[distributed,dev]"
.venv/bin/pytest              # 277 tests (psql/psycopg/SQLAlchemy ones skip if absent)
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

### TPC-H-like comparison with SQLite and DuckDB (`bench/`)

`bench/` compares nanosql — pure Python, and with the optional Rust page
codec of `bplus-tree-storage-engine` — against SQLite and DuckDB on the
same generated data. **This is not TPC-H**: `bench/tpch_like.py` is a
deterministic generator (fixed seed, byte-identical output for a given
scale) that reproduces the TPC-H schema's main columns, the per-scale row
ratios and the shape of the queries, without `dbgen`, its text
distributions or its audit rules. Dates are ISO text so the same SQL runs
on all four engines. The queries are the TPC-H ones that fit nanosql's
SQL subset, with the specification's default parameters: Q1, Q3, Q5, Q6,
Q10, Q18 (its `IN (SELECT … GROUP BY … HAVING …)`), Q12 rewritten without
`CASE` (`Q12*`), and a primary-key point lookup.

```bash
pip install -e ".[bench]"            # duckdb; sqlite3 ships with Python
pip install -e ".[native]"           # optional: the Rust codec (needs a Rust toolchain)
python bench/run.py --scale 0.01 --repeat 3 --out bench/results.json
python bench/profile_components.py --scale 0.01   # per-component cProfile shares
```

Every engine runs in its own subprocess on an in-memory database (for
nanosql, the B+Tree backend over an in-memory page device). After loading,
each engine runs `ANALYZE` (all three support it; it counts as part of the
load). Per query: one warm-up run and the median of three timed runs. The
rows of every query are compared across the four engines (floats with a
1e-6 relative tolerance): **all eight queries returned the same rows on
all four engines**. Raw timings, every run and the result rows are in
`bench/results.json`; `bench/results-reversed.json` is a second run of the
two nanosql engines in the opposite order, and
`bench/results-before-statistics.json` is the code before the statistics
and load work below (commit `ba65361`, its own bench script, no `ANALYZE`),
measured on the same machine for the before/after comparison.

Measured in this session at scale factor 0.01 (lineitem 60 278 rows,
orders 15 000, partsupp 8 000, part 2 000, customer 1 500, supplier 100):
cloud VM with an Intel Xeon @ 2.10 GHz, 4 vCPUs, 15 GiB RAM, Linux 6.18;
CPython 3.13.18, NumPy 2.5.4, SQLite 3.53.1 (Python's `sqlite3`),
DuckDB 2.0.0, rustc 1.97.0 + PyO3 0.23.5 + maturin 1.16.0 (release build).
The VM was restarted during the session; every number compared below comes
from the same instance.

| query (SF 0.01) | nanosql, pure Python (ms) | nanosql + Rust codec (ms) | SQLite (ms) | DuckDB (ms) |
|---|---:|---:|---:|---:|
| load + ANALYZE (s) | 50.9 | 27.3 | 0.8 | 1.4 |
| Q1 | 988.1 | 800.8 | 60.0 | 7.0 |
| Q3 | 964.5 | 930.8 | 3.3 | 5.4 |
| Q5 | 1632.1 | 1413.6 | 8.2 | 5.6 |
| Q6 | 841.5 | 621.2 | 7.9 | 2.3 |
| Q10 | 1118.5 | 928.1 | 7.5 | 5.6 |
| Q12* | 859.3 | 752.7 | 10.0 | 7.6 |
| Q18 | 2130.3 | 2031.5 | 14.1 | 5.8 |
| point | 182.3 | 102.3 | < 0.01 | 0.9 |

Before and after the optimizer statistics and the load fixes, same
machine (medians in ms; "after" shows the two runs):

| | pure Python before | pure Python after | + Rust before | + Rust after |
|---|---:|---:|---:|---:|
| load (s) | 80.7 | 50.9 / 43.3 | 60.6 | 27.3 / 28.9 |
| Q3 | 8189.4 | 964.5 / 1013.5 | 8093.6 | 930.8 / 831.9 |
| Q5 | 17182.9 | 1632.1 / 1339.3 | 18089.1 | 1413.6 / 1349.6 |
| Q10 | 1967.5 | 1118.5 / 990.1 | 2024.4 | 928.1 / 892.8 |

How to read it, honestly:

* **nanosql is still one to three orders of magnitude slower** than both
  baselines: from 13–16x behind SQLite on Q1 to 285–295x on Q3 and up to
  ~365x behind DuckDB on Q6 and Q18; SQLite answers the point lookup in
  3 µs. That is expected for an interpreted engine whose every row goes
  through MVCC version resolution, a JSON row payload and a B+Tree page
  codec in Python; the table shows where it stands, it does not compete.
* **Q3, Q5 and Q10 were slow because of the plan, and now are not.** With
  `distinct_count = row_count` for every column, `c_mktsegment =
  'BUILDING'` (5 values in 1 500 rows) was estimated at one row, so every
  join in the `EXPLAIN` of Q3, Q5 and Q10 was a `NestedLoopJoin`. With real
  statistics every one of them is a `HashJoin`: Q3 is 8.1–9.7x faster, Q5
  10.5–13.4x and Q10 1.8–2.3x, on the same machine.
* **Loading is 1.6–1.9x faster in pure Python and 2.1–2.2x with Rust.**
  Two things were wasted per `INSERT`: the unique-constraint check decoded
  every visible row of the table (quadratic over a bulk load), and the
  driver lexed each statement three times. With those gone the B+Tree
  codec is a larger share of what remains, so the Rust codec's load gain
  grew from 16–21 % to 33–46 % (pure Python → Rust: 50.9 → 27.3 s, then
  43.3 → 28.9 s).
* **The single-table queries did not change** (Q1, Q6, Q12*, Q18, the
  point lookup: same plans). Their run-to-run spread on this VM is about
  ±20 %, which is why a single run can look slower or faster; an
  interleaved A/B in separate processes at SF 0.005 (pure Python, two
  rounds each) gave Q6 377 / 374 ms before vs 384 / 347 ms after, and Q1
  410 / 476 vs 410 / 442.
* **The Rust codec, query by query:** faster in both runs on Q3, Q6, Q10,
  Q18 and the point lookup, by very different amounts (Q6 −26 % / −22 %,
  Q10 −17 % / −10 %, point −44 % / −6 %, Q3 −3 % / −18 %); on Q1, Q5 and
  Q12* the sign changes between runs. It helps where pages are decoded and
  nowhere else.
* SQLite's numbers now include its `ANALYZE` too. Compared with the run
  without it on the same machine, it got faster on Q3 (7.9 → 3.3 ms) and
  slower on Q1 (43.6 → 60.0), Q6 (4.3 → 7.9) and Q18 (7.6 → 14.1); its
  plans were not inspected, so this is reported, not explained. DuckDB
  barely moved.
* SQLite and DuckDB load through their own bulk paths (`executemany`,
  `COPY` from CSV); nanosql through 500-row parameterised `INSERT`s.

#### What was profiled, and what changed because of it

`bench/profile_components.py` attributes cProfile's self time to the
repository that owns each function (absolute times include the
profiler's 2–4x overhead; the shares are what matter). Four findings
drove the work, each measured in this session:

1. **Every table scan read the whole database.** Snapshot
   materialization asked `mvcc-transaction-manager` for every visible row
   of every table and filtered by row-id prefix in nanosql. The manager now
   has `scan_range(txn, start, end)` over an optional `RangeRowStore`
   protocol (`versions_in_range`), which nanosql's durable row store
   implements as one ordered range scan of the backend (row ids sort by
   table). Under the profiler, Q5 went from 24.8 s to 18.2 s, Q10 from
   8.3 s to 3.6 s, Q18 from 9.5 s to 5.7 s and the point lookup from
   1.57 s to 0.36 s; the storage engine's share in Q5 fell from 9% to 2%.
2. **The B+Tree page codec was the hottest component code.** During a
   bulk load (SF 0.005, pure Python) `bplus-tree-storage-engine` took 27%
   of the self time: every insert descends the tree and decodes the
   internal pages on its path (`decode_internal_page`, 34.0 s cumulative
   of 92 s profiled). Scan-bound queries spent 13–19% decoding leaves.
   That is a tight, self-contained, byte-level loop — the right shape for
   Rust — so it became the optional PyO3 accelerator described in that
   repository's README (pure-Python fallback, identical results proven
   by Hypothesis properties, abi3 wheels built in CI for Linux x86_64 and
   macOS arm64). With it, the same load profile shows
   `decode_internal_page` at 9.1 s cumulative and the component's share at
   16%; what remains is building the Python entry objects.
3. **Join-heavy queries were a planning problem.** Profiled, Q5 spent 84 %
   of its time in the executor, mostly in `RowBatch.take` building
   nested-loop pairs — not a loop worth porting to Rust, but the result of
   one-row estimates. Real statistics (`ANALYZE`, above) fixed the plans.
4. **The load did avoidable work.** The same SF 0.005 load profile showed
   `_check_unique_constraints` at 14.0 s cumulative of 83 s and the lexer
   at 24.8 s, called three times per statement. With in-memory unique-key
   indexes and a single lexer pass they take 0.13 s and 8.2 s, and the
   profiled load fell to 54 s.

What remains on the load path is lexing and parsing 500-row `INSERT`
statements in Python (22 s of the 54 s profiled) and the B+Tree inserts
themselves (12 s); queries spend their time materialising MVCC snapshots
and JSON payloads into columnar batches.

### Built-in micro-benchmark (`nanosql --benchmark`)

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
