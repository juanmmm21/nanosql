"""Clúster nanosql multiproceso sobre Raft real (TCP + log en disco), con
cliente que encuentra al líder, arnés de caos y checker de
linealizabilidad. Ver la sección "Real cluster and chaos testing" del
README."""

from .client import ClusterClient, ClusterSqlError, IndeterminateError
from .linearizability import CheckResult, Operation, check_history

__all__ = [
    "CheckResult",
    "ClusterClient",
    "ClusterSqlError",
    "IndeterminateError",
    "Operation",
    "check_history",
]
