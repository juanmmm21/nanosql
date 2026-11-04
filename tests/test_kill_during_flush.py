"""kill -9 en cada escritura de página de un checkpoint del B+Tree.

La campaña aleatoria de `test_kill9.py` casi nunca cae dentro de un flush
(sólo ocurre en los checkpoints y al cerrar), y fue ahí donde el CI
encontró un B+Tree que no reabría: un padre ya escrito apuntando a una
página de un split que seguía a cero. Este test lo recorre entero y de
forma determinista: un proceso escritor confirma filas, empieza el flush
de cierre y muere (`os._exit`) justo antes de la escritura en su sitio
número N, para cada N. Tras reabrir, la base tiene que abrir y contener
exactamente todas las filas confirmadas.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import nanosql

ROWS = 600
BATCH = 50

WRITER = r"""
import os, sys
from bplus_tree_storage_engine import pipeline
import nanosql

database, kill_before, rows, batch = sys.argv[1], *map(int, sys.argv[2:5])
conn = nanosql.connect(database)
conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
conn.commit()
for start in range(0, rows, batch):
    values = ", ".join(f"({i}, '{'v' * 120}-{i}')" for i in range(start, start + batch))
    conn.execute("INSERT INTO t (id, v) VALUES " + values)
    conn.commit()
writes = 0
original = pipeline.FilePageDevice.write_page

def write_page(self, page_id, data):
    global writes
    if kill_before >= 0 and writes >= kill_before:
        os._exit(9)
    writes += 1
    return original(self, page_id, data)

pipeline.FilePageDevice.write_page = write_page
conn.close()  # checkpoint: flush of every page the rows touched
print("in-place writes:", writes)
"""


def _run_writer(database: Path, kill_before: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", WRITER, str(database), str(kill_before), str(ROWS), str(BATCH)],
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_kill_before_every_in_place_write_of_a_checkpoint(tmp_path: Path) -> None:
    clean = _run_writer(tmp_path / "clean", -1)
    assert clean.returncode == 0, clean.stderr
    total = int(clean.stdout.split(":")[1])
    assert total > 10, "the checkpoint must write many pages for the test to mean anything"
    expected = [(i, f"{'v' * 120}-{i}") for i in range(ROWS)]

    for kill_before in range(total):
        database = tmp_path / f"kill-{kill_before}"
        result = _run_writer(database, kill_before)
        assert result.returncode == 9, result.stderr
        conn = nanosql.connect(str(database))
        try:
            rows = conn.execute("SELECT id, v FROM t ORDER BY id").fetchall()
        finally:
            conn.close()
        assert rows == expected, f"killed before in-place write #{kill_before + 1}"
