"""Checker de linealizabilidad: algoritmo de Wing & Gong con la caché de
estados de Lowe (la misma variante que implementa Porcupine).

Una historia es una lista de `Operation` con su instante de invocación y de
respuesta (reloj monotónico del arnés). Es linealizable si existe un orden
total de las operaciones que (1) respeta el tiempo real — si A respondió
antes de que B se invocara, A va antes — y (2) es una ejecución válida del
modelo secuencial (un registro, un contador).

Algoritmo: se ordenan los eventos de llamada/retorno en una lista doblemente
enlazada; se intenta "linealizar" la primera llamada pendiente cuyo paso
sea válido en el modelo — se saca (junto con su retorno) de la lista y se
apila —; si se llega al retorno de una operación aún no linealizada, no hay
forma de ordenarla antes de ese instante y se retrocede. La caché de pares
(conjunto linealizado, estado del modelo) poda las ramas ya exploradas
(Lowe, 2017), que es lo que hace el algoritmo práctico.

Operaciones con resultado desconocido (el cliente agotó el plazo sin
respuesta definitiva) se modelan con retorno en +∞: pueden haber ocurrido
en cualquier momento posterior a su invocación (o, equivalente a efectos
observables, después de todo lo demás). Las lecturas con resultado
desconocido no tienen efecto observable y se omiten. La historia se parte
por clave (P-composicionalidad de la linealizabilidad: una historia es
linealizable si y sólo si lo es la subhistoria de cada objeto), lo que
mantiene el problema —NP-completo en general— en tamaños manejables.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Hashable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

INFINITY = math.inf


@dataclass(frozen=True, slots=True)
class Operation:
    process: int
    key: str
    kind: str  # "read" | "write" | "inc"
    value: int | None
    output: int | None
    call: float
    ret: float  # INFINITY = resultado desconocido

    @property
    def pending(self) -> bool:
        return self.ret == INFINITY


class Model(Protocol):
    initial: Hashable

    def step(self, state: Hashable, op: Operation) -> tuple[bool, Hashable]: ...


class RegisterModel:
    """Registro entero: `write(v)` fija el valor; `read` debe devolverlo."""

    initial: Hashable = 0

    def step(self, state: Hashable, op: Operation) -> tuple[bool, Hashable]:
        if op.kind == "write":
            return True, op.value
        if op.kind == "read":
            return op.output == state, state
        return False, state


class CounterModel:
    """Contador: `inc` suma uno; `read` debe ver exactamente el total."""

    initial: Hashable = 0

    def step(self, state: Hashable, op: Operation) -> tuple[bool, Hashable]:
        assert isinstance(state, int)
        if op.kind == "inc":
            return True, state + 1
        if op.kind == "read":
            return op.output == state, state
        return False, state


@dataclass(slots=True)
class _Entry:
    op: Operation | None
    ident: int
    is_call: bool
    time: float
    match: _Entry | None = None
    prev: _Entry | None = None
    next: _Entry | None = None


@dataclass(slots=True)
class PartitionResult:
    key: str
    operations: int
    linearizable: bool | None  # None = presupuesto de búsqueda agotado
    explored: int


@dataclass(slots=True)
class CheckResult:
    partitions: list[PartitionResult] = field(default_factory=list)

    @property
    def linearizable(self) -> bool:
        return all(p.linearizable is True for p in self.partitions)

    @property
    def violations(self) -> list[PartitionResult]:
        return [p for p in self.partitions if p.linearizable is False]

    @property
    def undecided(self) -> list[PartitionResult]:
        return [p for p in self.partitions if p.linearizable is None]


def _lift(entry: _Entry) -> None:
    assert entry.prev is not None and entry.next is not None and entry.match is not None
    entry.prev.next = entry.next
    entry.next.prev = entry.prev
    match = entry.match
    assert match.prev is not None
    match.prev.next = match.next
    if match.next is not None:
        match.next.prev = match.prev


def _unlift(entry: _Entry) -> None:
    match = entry.match
    assert match is not None and match.prev is not None
    match.prev.next = match
    if match.next is not None:
        match.next.prev = match
    assert entry.prev is not None and entry.next is not None
    entry.prev.next = entry
    entry.next.prev = entry


def check_operations(
    operations: Sequence[Operation], model: Model, *, max_steps: int = 50_000_000
) -> tuple[bool | None, int]:
    """WGL sobre una subhistoria. Devuelve `(linealizable, pasos)`;
    `None` si se agota `max_steps` sin decidir."""
    events: list[_Entry] = []
    for ident, op in enumerate(operations):
        call = _Entry(op, ident, True, op.call)
        ret = _Entry(op, ident, False, op.ret)
        call.match = ret
        events.extend((call, ret))
    # A igualdad de instante, las llamadas antes que los retornos: dos
    # operaciones que se tocan se consideran concurrentes (lo más permisivo).
    events.sort(key=lambda e: (e.time, 0 if e.is_call else 1))
    head = _Entry(None, -1, False, -INFINITY)
    previous = head
    for event in events:
        previous.next = event
        event.prev = previous
        previous = event

    state: Hashable = model.initial
    linearized = 0
    cache: set[tuple[int, Hashable]] = set()
    stack: list[tuple[_Entry, Hashable]] = []
    entry = head.next
    steps = 0
    while head.next is not None:
        steps += 1
        if steps > max_steps:
            return None, steps
        assert entry is not None
        if entry.is_call:
            assert entry.op is not None
            ok, new_state = model.step(state, entry.op)
            if ok:
                candidate = linearized | (1 << entry.ident)
                key = (candidate, new_state)
                if key not in cache:
                    cache.add(key)
                    stack.append((entry, state))
                    state = new_state
                    linearized = candidate
                    _lift(entry)
                    entry = head.next
                    continue
            entry = entry.next
        else:
            if not stack:
                return False, steps
            top, state = stack.pop()
            linearized &= ~(1 << top.ident)
            _unlift(top)
            entry = top.next
    return True, steps


def check_history(
    operations: Iterable[Operation],
    model_for_key: dict[str, Model] | None = None,
    *,
    max_steps: int = 50_000_000,
) -> CheckResult:
    """Comprueba cada clave por separado. Las claves `reg:*` usan el modelo
    de registro y `cnt:*` el de contador, salvo que `model_for_key` diga
    otra cosa."""
    by_key: dict[str, list[Operation]] = defaultdict(list)
    for op in operations:
        if op.kind == "read" and op.pending:
            continue  # lectura sin respuesta: sin efecto observable
        by_key[op.key].append(op)
    result = CheckResult()
    for key in sorted(by_key):
        model: Model
        if model_for_key is not None and key in model_for_key:
            model = model_for_key[key]
        elif key.startswith("cnt"):
            model = CounterModel()
        else:
            model = RegisterModel()
        ok, steps = check_operations(by_key[key], model, max_steps=max_steps)
        result.partitions.append(PartitionResult(key, len(by_key[key]), ok, steps))
    return result
