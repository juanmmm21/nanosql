"""El checker WGL: acepta historias linealizables (también concurrentes y
con operaciones de resultado desconocido) y DETECTA violaciones inyectadas:
lectura rancia tras una escritura confirmada, incremento perdido, lectura de
un valor nunca escrito, orden de tiempo real roto."""

from __future__ import annotations

import random

from nanosql.cluster.linearizability import (
    INFINITY,
    CounterModel,
    Operation,
    RegisterModel,
    check_history,
    check_operations,
)


def op(
    kind: str,
    call: float,
    ret: float,
    value: int | None = None,
    output: int | None = None,
    key: str = "reg:0",
    process: int = 0,
) -> Operation:
    return Operation(process, key, kind, value, output, call, ret)


def test_sequential_register_history() -> None:
    history = [
        op("write", 0, 1, value=5),
        op("read", 2, 3, output=5),
        op("write", 4, 5, value=7),
        op("read", 6, 7, output=7),
    ]
    assert check_operations(history, RegisterModel())[0] is True


def test_concurrent_read_may_see_either_value() -> None:
    write = op("write", 0, 10, value=1)
    assert check_operations([write, op("read", 1, 2, output=0)], RegisterModel())[0] is True
    assert check_operations([write, op("read", 1, 2, output=1)], RegisterModel())[0] is True


def test_injected_stale_read_is_detected() -> None:
    """La violación típica de un líder depuesto que sirve lecturas: la
    escritura terminó (t=1) antes de que empezara la lectura (t=2), y aun
    así la lectura devuelve el valor anterior."""
    history = [op("write", 0, 1, value=5), op("read", 2, 3, output=0)]
    assert check_operations(history, RegisterModel())[0] is False


def test_injected_lost_increment_is_detected() -> None:
    history = [
        op("inc", 0, 1, key="cnt:0"),
        op("inc", 2, 3, key="cnt:0", process=1),
        op("read", 4, 5, key="cnt:0", output=1),
    ]
    assert check_operations(history, CounterModel())[0] is False
    fixed = history[:2] + [op("read", 4, 5, key="cnt:0", output=2)]
    assert check_operations(fixed, CounterModel())[0] is True


def test_real_time_order_is_enforced() -> None:
    # dos lecturas no solapadas no pueden ver la nueva y luego la vieja
    history = [op("write", 0, 10, value=1), op("read", 1, 2, output=1), op("read", 3, 4, output=0)]
    assert check_operations(history, RegisterModel())[0] is False


def test_pending_write_may_or_may_not_have_happened() -> None:
    pending = op("write", 0, INFINITY, value=9)
    assert check_operations([pending, op("read", 5, 6, output=9)], RegisterModel())[0] is True
    assert check_operations([pending, op("read", 5, 6, output=0)], RegisterModel())[0] is True
    # pero una vez visto el 9, no se puede "des-ver"
    seen_then_lost = [pending, op("read", 5, 6, output=9), op("read", 7, 8, output=0)]
    assert check_operations(seen_then_lost, RegisterModel())[0] is False


def test_history_is_partitioned_by_key_and_reports_the_bad_key() -> None:
    history = [
        op("write", 0, 1, value=3, key="reg:1"),
        op("read", 2, 3, output=3, key="reg:1"),
        op("inc", 0, 1, key="cnt:2"),
        op("read", 2, 3, output=0, key="cnt:2"),  # violación sólo aquí
        op("read", 0, INFINITY, output=None, key="reg:1"),  # lectura pendiente: se omite
    ]
    result = check_history(history)
    assert not result.linearizable
    assert [p.key for p in result.violations] == ["cnt:2"]
    assert {p.key: p.operations for p in result.partitions} == {"cnt:2": 2, "reg:1": 2}


def _random_linearizable_history(rng: random.Random, size: int, clients: int) -> list[Operation]:
    """Ejecución real simulada: cada operación tiene un punto de
    linealización dentro de su intervalo [call, ret]; aplicarlas en el
    orden de esos puntos da salidas correctas por construcción."""
    clock = [0.0] * clients
    planned = []
    for _ in range(size):
        process = rng.randrange(clients)
        call = clock[process] + rng.random()
        point = call + rng.random() * 3
        ret = point + rng.random() * 3
        clock[process] = ret
        kind = rng.choice(["read", "write", "read"])
        planned.append((point, process, kind, call, ret, rng.randint(1, 4)))
    planned.sort()
    value = 0
    history = []
    for _, process, kind, call, ret, write_value in planned:
        if kind == "write":
            value = write_value
            history.append(op("write", call, ret, value=write_value, process=process))
        else:
            history.append(op("read", call, ret, output=value, process=process))
    return history


def test_random_linearizable_histories_pass_and_injected_violations_fail() -> None:
    rng = random.Random(2026)
    for _ in range(150):
        history = _random_linearizable_history(rng, rng.randint(5, 40), rng.randint(1, 5))
        assert check_operations(history, RegisterModel())[0] is True
        reads = [i for i, o in enumerate(history) if o.kind == "read"]
        if not reads:
            continue
        victim = rng.choice(reads)
        broken = list(history)
        bad = history[victim]
        # un valor que ninguna escritura produjo jamás: nunca linealizable
        broken[victim] = op("read", bad.call, bad.ret, output=99, process=bad.process)
        assert check_operations(broken, RegisterModel())[0] is False
