"""Jerarquía de excepciones del driver, con la forma clásica de DB-API 2.0.

Todo error que cruza la frontera pública de `nanosql` es una subclase de
`NanosqlError`: los errores internos de los subproyectos ensamblados
(parser, optimizador, ejecutor, storage, MVCC, locks) se capturan en la capa
de integración y se re-lanzan como el tipo de esta jerarquía que corresponda
a su significado para el usuario del driver, siempre encadenados con
`raise ... from` para no perder la causa original.
"""

from __future__ import annotations


class NanosqlError(Exception):
    """Raíz de todos los errores del driver."""


class InterfaceError(NanosqlError):
    """Uso incorrecto de la interfaz del driver (no de la base de datos):
    operar sobre una conexión o cursor ya cerrados, por ejemplo."""


class DatabaseError(NanosqlError):
    """Raíz de los errores relacionados con la propia base de datos."""


class DataError(DatabaseError):
    """Problema con los datos procesados: valor de tipo incompatible con la
    columna, división por cero dentro de una expresión, etc."""


class OperationalError(DatabaseError):
    """Error operacional no necesariamente causado por el programador:
    E/S de disco, timeout de un lock, base de datos en uso."""


class IntegrityError(DatabaseError):
    """Violación de una restricción declarada: NOT NULL, PRIMARY KEY,
    UNIQUE."""


class InternalError(DatabaseError):
    """La base de datos alcanzó un estado inconsistente: catálogo corrupto,
    formato de fila irreconocible, invariante interno roto."""


class ProgrammingError(DatabaseError):
    """Error de programación SQL: sintaxis inválida, tabla o columna
    inexistente, sentencia mal usada."""


class NotSupportedError(DatabaseError):
    """La sentencia es SQL válido pero usa una característica que nanosql
    no soporta (documentada como fuera del subconjunto)."""


class TransactionConflictError(OperationalError):
    """La transacción fue abortada por un conflicto con otra concurrente:
    conflicto escritura-escritura (first-committer-wins), fallo de
    serialización (write skew detectado) o deadlock. La transacción ya ha
    hecho rollback cuando esta excepción llega al llamador; reintentarla
    entera es la respuesta esperada."""
