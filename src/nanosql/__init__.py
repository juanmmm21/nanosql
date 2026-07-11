"""nanosql: motor SQL embebible que ensambla los subproyectos de
strata-database-engine — storage (B+Tree/LSM), WAL, MVCC, locking, parser,
optimizador y ejecutor vectorizado — detrás de un driver DB-API simplificado
y un CLI/REPL."""

from .driver import Connection, Cursor, apilevel, connect, threadsafety
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

__version__ = "0.1.0"

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
    "threadsafety",
]
