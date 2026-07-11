"""Test de propiedades: carga SQL aleatoria contra un modelo de referencia.

El modelo es un diccionario Python plano que re-implementa la semántica
esperada (visibilidad transaccional incluida) por un camino de código
totalmente independiente del motor: nunca reutiliza el parser, el
optimizador, el ejecutor ni el storage. Cada secuencia se genera con una
semilla fija (reproducible bit a bit), se ejecuta sobre los dos backends de
storage, intercala COMMIT/ROLLBACK y cierres/reaperturas de la base de
datos, y verifica el contenido visible completo tras cada transacción.

Los valores FLOAT generados son siempre múltiplos de 0.25: exactos en coma
flotante binaria, así la comparación con el modelo puede ser por igualdad
estricta sin tolerancias que puedan enmascarar un bug real.
"""

from __future__ import annotations

import random
from pathlib import Path

import pytest

import nanosql

Row = tuple[str, float | None]  # (cat, val) — id es la clave del dict

SEEDS = [7, 21, 99, 1234]
OPERATIONS_PER_SEED = 250
REOPEN_EVERY = 80
CATEGORIES = ["rojo", "verde", "azul", "gris"]


class ReferenceModel:
    """Estado esperado: committed + working copy transaccional."""

    def __init__(self) -> None:
        self.committed: dict[int, Row] = {}
        self.working: dict[int, Row] = {}

    def commit(self) -> None:
        self.committed = dict(self.working)

    def rollback(self) -> None:
        self.working = dict(self.committed)

    def visible_sorted(self) -> list[tuple[int, str, float | None]]:
        return [(rowid, cat, val) for rowid, (cat, val) in sorted(self.working.items())]


def _random_val(rng: random.Random) -> float | None:
    if rng.random() < 0.15:
        return None
    return rng.randrange(0, 400) / 4.0  # múltiplo exacto de 0.25


def _fmt(val: float | None) -> str:
    return "NULL" if val is None else repr(val)


@pytest.mark.parametrize("backend", ["btree", "lsm"])
@pytest.mark.parametrize("seed", SEEDS)
def test_random_workload_matches_reference_model(backend: str, seed: int, tmp_path: Path) -> None:
    rng = random.Random(seed)
    db_path = tmp_path / f"prop-{backend}-{seed}"
    conn = nanosql.connect(db_path, backend=backend)
    conn.execute("CREATE TABLE datos (id INTEGER PRIMARY KEY, cat VARCHAR(8), val FLOAT)")
    conn.commit()

    model = ReferenceModel()
    next_id = 0

    def verify() -> None:
        got = conn.execute("SELECT id, cat, val FROM datos ORDER BY id").fetchall()
        assert got == model.visible_sorted(), f"divergencia (seed={seed}, backend={backend})"
        count = conn.execute("SELECT COUNT(*) FROM datos").fetchall()
        assert count == [(len(model.working),)]

    for step in range(OPERATIONS_PER_SEED):
        dice = rng.random()
        if dice < 0.40:  # INSERT
            rows = []
            for _ in range(rng.randint(1, 3)):
                cat = rng.choice(CATEGORIES)
                val = _random_val(rng)
                rows.append((next_id, cat, val))
                model.working[next_id] = (cat, val)
                next_id += 1
            values = ", ".join(f"({i}, '{c}', {_fmt(v)})" for i, c, v in rows)
            cur = conn.execute(f"INSERT INTO datos (id, cat, val) VALUES {values}")
            assert cur.rowcount == len(rows)
        elif dice < 0.55 and model.working:  # UPDATE por id
            target = rng.choice(sorted(model.working))
            cat = rng.choice(CATEGORIES)
            val = _random_val(rng)
            cur = conn.execute(
                f"UPDATE datos SET cat = '{cat}', val = {_fmt(val)} WHERE id = {target}"
            )
            assert cur.rowcount == 1
            model.working[target] = (cat, val)
        elif dice < 0.65 and model.working:  # UPDATE por rango con expresión
            threshold = rng.randrange(0, 400) / 4.0
            cur = conn.execute(f"UPDATE datos SET val = val + 0.25 WHERE val < {threshold!r}")
            affected = 0
            for rowid, (cat, val) in model.working.items():
                if val is not None and val < threshold:
                    model.working[rowid] = (cat, val + 0.25)
                    affected += 1
            assert cur.rowcount == affected
        elif dice < 0.75 and model.working:  # DELETE por id
            target = rng.choice(sorted(model.working))
            cur = conn.execute(f"DELETE FROM datos WHERE id = {target}")
            assert cur.rowcount == 1
            del model.working[target]
        elif dice < 0.80 and model.working:  # DELETE por predicado de categoría
            cat = rng.choice(CATEGORIES)
            cur = conn.execute(f"DELETE FROM datos WHERE cat = '{cat}'")
            doomed = [rowid for rowid, (c, _) in model.working.items() if c == cat]
            assert cur.rowcount == len(doomed)
            for rowid in doomed:
                del model.working[rowid]
        elif dice < 0.88:  # ROLLBACK
            conn.rollback()
            model.rollback()
            verify()
        else:  # COMMIT + verificación
            conn.commit()
            model.commit()
            verify()

        if step and step % REOPEN_EVERY == 0:
            # Cierre y reapertura completos: lo no confirmado debe perderse
            # (rollback implícito del close) y lo confirmado debe sobrevivir
            # al ciclo de recuperación WAL + rearranque del backend.
            conn.close()
            model.rollback()
            conn = nanosql.connect(db_path)
            verify()

    conn.commit()
    model.commit()
    verify()

    # Verificación final agregada por un segundo camino: suma por categoría.
    expected_totals: dict[str, float] = {}
    for _rowid, (cat, val) in model.working.items():
        if val is not None:
            expected_totals[cat] = expected_totals.get(cat, 0.0) + val
    got_rows = conn.execute(
        "SELECT cat, SUM(val) AS total FROM datos WHERE val IS NOT NULL GROUP BY cat"
    ).fetchall()
    got_totals = {cat: total for cat, total in got_rows}
    assert set(got_totals) == set(expected_totals)
    for cat, total in expected_totals.items():
        assert got_totals[cat] == pytest.approx(total)
    conn.close()
