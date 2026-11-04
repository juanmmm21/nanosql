"""Servidor del protocolo de red de PostgreSQL (v3) para nanosql.

`nanosql serve <db> --port 5432` expone una base de datos nanosql a psql,
psycopg, SQLAlchemy y cualquier cliente que hable el protocolo v3. Ver la
sección "PostgreSQL wire protocol" del README para lo soportado y lo no.
"""

from .server import AUTH_METHODS, PgServer

__all__ = ["AUTH_METHODS", "PgServer"]
