"""nanosql: motor SQL embebible que ensambla los subproyectos de
strata-database-engine — storage (B+Tree/LSM), WAL, MVCC, locking, parser,
optimizador y ejecutor vectorizado — detrás de un driver DB-API simplificado
y un CLI/REPL."""

from .driver import Connection, Cursor, apilevel, connect, paramstyle, threadsafety
from .errors import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NanosqlError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TransactionConflictError,
)

__version__ = "0.2.0"

__all__ = [
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NanosqlError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "TransactionConflictError",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]
