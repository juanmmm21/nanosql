"""`kill -9` de punta a punta: un proceso escritor real muere cientos de
veces a mitad de transacciones, checkpoints y truncados del WAL; tras cada
muerte la base de datos se reabre (recuperación real) y se comprueba que lo
confirmado sobrevive y que nunca aparece nada a medias, con ambos motores
de storage. `NANOSQL_KILL9_KILLS` ajusta el número de kills por motor."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from nanosql.crashtest import run_campaign

KILLS = int(os.environ.get("NANOSQL_KILL9_KILLS", "150"))


@pytest.mark.parametrize("backend", ["btree", "lsm"])
def test_hundreds_of_kill9_keep_committed_and_never_half_applied(
    tmp_path: Path, backend: str
) -> None:
    result = run_campaign(tmp_path / f"{backend}.db", backend, KILLS, seed=7)
    assert result.violations == []
    assert result.kills == KILLS
    assert result.acknowledged > KILLS  # de verdad se confirmó trabajo entre kills
    assert result.recovered >= result.acknowledged
