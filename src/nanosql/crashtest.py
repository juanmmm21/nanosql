"""Campaña de `kill -9` contra un proceso que escribe en nanosql.

``python -m nanosql.crashtest campaign --backend btree --kills 300 --dir D``

Un proceso *escritor* abre la base de datos y confirma, en bucle,
transacciones de tamaño variable: el lote ``b`` inserta ``1 + b % 3`` filas
en ``ledger`` y suma ese número en la fila única de ``totals``, todo en la
misma transacción, e imprime ``ack b`` (con flush) en cuanto ``commit()``
devuelve. El proceso padre lo mata con ``SIGKILL`` tras un retardo
aleatorio, abre la base de datos (recuperación ante fallos real), comprueba
los invariantes, la cierra y relanza el escritor, que continúa donde quedó
lo confirmado. Checkpoints y GC se fuerzan cada pocos commits para que el
kill caiga también a mitad de un checkpoint o de un truncado del WAL.

Invariantes tras cada kill:

* **lo confirmado sobrevive** — todo lote con ``ack`` está presente;
* **nada a medias** — cada lote presente tiene exactamente sus
  ``1 + b % 3`` filas, y ``totals.n`` es igual al número de filas;
* **sin huecos** — los lotes presentes son exactamente ``1..m``;
* la base de datos siempre abre (la recuperación nunca falla).
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import connect
from .errors import NanosqlError


def rows_in_batch(batch: int) -> int:
    return 1 + batch % 3


def writer_main(database: Path, backend: str, start: int) -> int:
    conn = connect(database, backend=backend, checkpoint_interval=8, gc_interval=4)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY, batch INTEGER NOT NULL, "
        "val INTEGER NOT NULL)"
    )
    conn.execute("CREATE TABLE IF NOT EXISTS totals (k INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
    conn.commit()
    if not conn.execute("SELECT k FROM totals WHERE k = 0").fetchall():
        conn.execute("INSERT INTO totals (k, n) VALUES (0, 0)")
        conn.commit()
    print("ready", flush=True)
    batch = start
    while True:
        count = rows_in_batch(batch)
        values = ", ".join(f"({batch * 10 + i}, {batch}, {batch * 7 + i})" for i in range(count))
        conn.execute(f"INSERT INTO ledger (id, batch, val) VALUES {values}")
        conn.execute("UPDATE totals SET n = n + ? WHERE k = 0", (count,))
        conn.commit()
        print(f"ack {batch}", flush=True)
        batch += 1


@dataclass(slots=True)
class CampaignResult:
    backend: str
    kills: int = 0
    acknowledged: int = 0  # lotes confirmados al escritor en total
    recovered: int = 0  # lotes presentes al final
    violations: list[str] = field(default_factory=list)
    seconds: float = 0.0

    def to_json(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "kills": self.kills,
            "acknowledged_batches": self.acknowledged,
            "recovered_batches": self.recovered,
            "violations": self.violations,
            "seconds": round(self.seconds, 1),
        }


def check_database(database: Path, last_ack: int, ready_seen: bool) -> tuple[int, list[str]]:
    """Abre la base de datos (recuperación incluida) y devuelve
    `(lotes presentes, violaciones)`."""
    problems: list[str] = []
    try:
        conn = connect(database)
    except NanosqlError as exc:
        return 0, [f"la base de datos no abre tras el kill: {exc}"]
    try:
        tables = conn._require_engine().catalog.tables
        if "ledger" not in tables or "totals" not in tables:
            if ready_seen:
                problems.append("faltan tablas que ya se habían confirmado")
            return 0, problems
        per_batch: dict[int, int] = {}
        for batch_value, count_value in conn.execute(
            "SELECT batch, COUNT(*) FROM ledger GROUP BY batch"
        ).fetchall():
            assert isinstance(batch_value, int) and isinstance(count_value, int)
            per_batch[batch_value] = count_value
        totals = conn.execute("SELECT n FROM totals WHERE k = 0").fetchall()
        present = sorted(per_batch)
        top = present[-1] if present else 0
        if present != list(range(1, top + 1)):
            problems.append(f"huecos en los lotes: {present[:5]}...{present[-5:]}")
        for batch, count in per_batch.items():
            if count != rows_in_batch(batch):
                problems.append(f"lote {batch} a medias: {count} de {rows_in_batch(batch)} filas")
        total_rows = sum(per_batch.values())
        if ready_seen and totals != [(total_rows,)]:
            problems.append(f"totals={totals} pero hay {total_rows} filas")
        if top < last_ack:
            problems.append(f"se perdió lo confirmado: último ack {last_ack}, presente {top}")
        return top, problems
    finally:
        conn.close()


def run_campaign(
    database: Path,
    backend: str,
    kills: int,
    *,
    seed: int = 1,
    max_delay: float = 0.4,
    progress: bool = False,
) -> CampaignResult:
    rng = random.Random(seed)
    result = CampaignResult(backend=backend)
    started = time.monotonic()
    if database.exists():
        shutil.rmtree(database)
    next_batch = 1
    last_ack = 0
    ready_seen = False
    for iteration in range(kills):
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "nanosql.crashtest",
                "writer",
                "--db",
                str(database),
                "--backend",
                backend,
                "--start",
                str(next_batch),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        # esperar a "ready" (o a un fallo de arranque) y dejarle escribir
        line = process.stdout.readline().decode().strip()
        if line != "ready":
            process.kill()
            err = process.stderr.read().decode() if process.stderr else ""
            result.violations.append(
                f"kill {iteration}: el escritor no arrancó: {line} {err[-300:]}"
            )
            break
        ready_seen = True
        time.sleep(rng.uniform(0.0, max_delay))
        process.send_signal(signal.SIGKILL)
        process.wait()
        output = process.stdout.read().decode().split()
        acks = [int(output[i + 1]) for i in range(len(output) - 1) if output[i] == "ack"]
        if acks:
            last_ack = max(last_ack, acks[-1])
            result.acknowledged = last_ack
        result.kills += 1
        present, problems = check_database(database, last_ack, ready_seen)
        result.violations.extend(f"kill {iteration}: {p}" for p in problems)
        next_batch = present + 1
        if progress and iteration % 25 == 0:
            print(
                f"[{backend}] kill {iteration}: confirmado {last_ack}, presente {present}",
                flush=True,
            )
        if problems:
            break
    result.recovered = next_batch - 1
    result.seconds = time.monotonic() - started
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m nanosql.crashtest")
    sub = parser.add_subparsers(dest="command", required=True)
    writer = sub.add_parser("writer")
    writer.add_argument("--db", type=Path, required=True)
    writer.add_argument("--backend", choices=["btree", "lsm"], default="btree")
    writer.add_argument("--start", type=int, default=1)
    campaign = sub.add_parser("campaign")
    campaign.add_argument("--dir", type=Path, required=True)
    campaign.add_argument("--backend", choices=["btree", "lsm"], default="btree")
    campaign.add_argument("--kills", type=int, default=200)
    campaign.add_argument("--seed", type=int, default=1)
    campaign.add_argument("--report", type=Path, default=None)
    args = parser.parse_args(argv)
    if args.command == "writer":
        return writer_main(args.db, args.backend, args.start)
    result = run_campaign(
        args.dir / f"kill9-{args.backend}.db",
        args.backend,
        args.kills,
        seed=args.seed,
        progress=True,
    )
    summary = result.to_json()
    print(json.dumps(summary, indent=2))
    if args.report is not None:
        args.report.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return 0 if not result.violations else 1


if __name__ == "__main__":
    sys.exit(main())
