"""Índice en memoria de las claves únicas de una tabla.

Las comprobaciones de unicidad decodificaban *todas* las filas visibles de
la tabla en cada INSERT/UPDATE y otra vez en cada commit: cargar 60.000
filas en sentencias de 500 era cuadrático (14 s de 83 s de una carga
perfilada). Este índice guarda, por restricción, clave → rowid y rowid →
clave, y se mantiene con cada escritura en vez de reconstruirse.

El engine usa dos: uno por transacción (su snapshot más sus propias
escrituras, para la comprobación por sentencia) y uno por tabla con el
último estado confirmado (para la revalidación en el commit). Las claves
con algún NULL no entran: en SQL los NULL nunca chocan entre sí.
"""

from __future__ import annotations

from collections.abc import Iterable

from .catalog import ColumnValue

Constraint = tuple[str, ...]
Key = tuple[ColumnValue, ...]


def key_of(row: dict[str, ColumnValue], constraint: Constraint) -> Key | None:
    key = tuple(row.get(column) for column in constraint)
    return None if None in key else key


class UniqueKeyIndex:
    def __init__(self, constraints: Iterable[Constraint]) -> None:
        self.constraints: tuple[Constraint, ...] = tuple(constraints)
        self._owner: dict[Constraint, dict[Key, int]] = {c: {} for c in self.constraints}
        self._key: dict[Constraint, dict[int, Key]] = {c: {} for c in self.constraints}

    @classmethod
    def build(
        cls, constraints: Iterable[Constraint], rows: Iterable[tuple[int, dict[str, ColumnValue]]]
    ) -> UniqueKeyIndex:
        index = cls(constraints)
        for rowid, row in rows:
            index.add(rowid, row)
        return index

    def add(self, rowid: int, row: dict[str, ColumnValue]) -> None:
        for constraint in self.constraints:
            key = key_of(row, constraint)
            if key is not None:
                self._owner[constraint][key] = rowid
                self._key[constraint][rowid] = key

    def remove(self, rowid: int) -> None:
        for constraint in self.constraints:
            key = self._key[constraint].pop(rowid, None)
            if key is not None and self._owner[constraint].get(key) == rowid:
                del self._owner[constraint][key]

    def replace(self, rowid: int, row: dict[str, ColumnValue] | None) -> None:
        """La fila `rowid` pasa a valer `row` (`None`: se borró)."""
        self.remove(rowid)
        if row is not None:
            self.add(rowid, row)

    def owner(self, constraint: Constraint, key: Key) -> int | None:
        return self._owner[constraint].get(key)
