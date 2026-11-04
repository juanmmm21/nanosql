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
    """Raíz de todos los errores del driver.

    Cada error lleva un código SQLSTATE de cinco caracteres (`sqlstate`):
    el de su clase por defecto, o uno más específico pasado por el sitio que
    lo lanza (`ProgrammingError(..., sqlstate="42P01")`). El servidor de
    protocolo Postgres lo envía tal cual en el campo `C` de ErrorResponse,
    así que psql, psycopg y SQLAlchemy reciben la misma clasificación que
    recibirían de un PostgreSQL real.
    """

    default_sqlstate = "XX000"

    def __init__(self, *args: object, sqlstate: str | None = None) -> None:
        super().__init__(*args)
        self.sqlstate = sqlstate if sqlstate is not None else self.default_sqlstate


class InterfaceError(NanosqlError):
    """Uso incorrecto de la interfaz del driver (no de la base de datos):
    operar sobre una conexión o cursor ya cerrados, por ejemplo."""

    default_sqlstate = "08000"


class DatabaseError(NanosqlError):
    """Raíz de los errores relacionados con la propia base de datos."""

    default_sqlstate = "XX000"


class DataError(DatabaseError):
    """Problema con los datos procesados: valor de tipo incompatible con la
    columna, división por cero dentro de una expresión, etc."""

    default_sqlstate = "22000"


class OperationalError(DatabaseError):
    """Error operacional no necesariamente causado por el programador:
    E/S de disco, timeout de un lock, base de datos en uso."""

    default_sqlstate = "58000"


class IntegrityError(DatabaseError):
    """Violación de una restricción declarada: NOT NULL, PRIMARY KEY,
    UNIQUE."""

    default_sqlstate = "23000"


class InternalError(DatabaseError):
    """La base de datos alcanzó un estado inconsistente: catálogo corrupto,
    formato de fila irreconocible, invariante interno roto."""

    default_sqlstate = "XX000"


class ProgrammingError(DatabaseError):
    """Error de programación SQL: sintaxis inválida, tabla o columna
    inexistente, sentencia mal usada."""

    default_sqlstate = "42000"


class NotSupportedError(DatabaseError):
    """La sentencia es SQL válido pero usa una característica que nanosql
    no soporta (documentada como fuera del subconjunto)."""

    default_sqlstate = "0A000"


class TransactionConflictError(OperationalError):
    """La transacción fue abortada por un conflicto con otra concurrente:
    conflicto escritura-escritura (first-committer-wins), fallo de
    serialización (write skew detectado) o deadlock. La transacción ya ha
    hecho rollback cuando esta excepción llega al llamador; reintentarla
    entera es la respuesta esperada."""

    default_sqlstate = "40001"
