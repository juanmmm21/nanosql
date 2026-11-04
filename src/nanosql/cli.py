"""CLI/REPL interactivo de nanosql.

`nanosql <ruta.db>` abre un bucle de lectura interactivo con historial
(readline), metacomandos estilo sqlite (`.tables`, `.schema`, `.help`) y
salida en tabla. `-c "SQL"` ejecuta sentencias y termina (útil en scripts) y
`--benchmark` corre la batería de medición usada por el README.

El REPL trabaja en autocommit (cada sentencia confirma sola) hasta que el
usuario emite `BEGIN`; a partir de ahí el prompt cambia a `nanosql*>` y nada
se confirma hasta `COMMIT`/`ROLLBACK` — el mismo contrato que el driver.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path

from sql_query_parser import SqlParserError, tokenize
from sql_query_parser.models import TokenType

from . import __version__, connect
from .catalog import ColumnValue
from .driver import Connection
from .errors import NanosqlError

_PROMPT = "nanosql> "
_PROMPT_TXN = "nanosql*> "
_PROMPT_CONTINUATION = "   ....> "

_HELP = """\
Metacomandos:
  .help            esta ayuda
  .tables          lista las tablas de la base de datos
  .schema [tabla]  muestra las columnas e índices (de una tabla o de todas)
  .quit / .exit    salir (también Ctrl-D)

Las sentencias SQL terminan en ';' y pueden ocupar varias líneas.
Transacciones: BEGIN; ... COMMIT; / ROLLBACK; (sin BEGIN, cada sentencia
se confirma automáticamente)."""


def _format_value(value: ColumnValue) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def format_table(columns: Sequence[str], rows: Sequence[tuple[ColumnValue, ...]]) -> str:
    """Tabla de texto con anchos ajustados al contenido, estilo psql."""
    headers = list(columns)
    rendered = [[_format_value(cell) for cell in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in rendered:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    separator = "-+-".join("-" * w for w in widths)
    lines = [
        " | ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)),
        separator,
    ]
    lines.extend(
        " | ".join(cell.ljust(w) for cell, w in zip(row, widths, strict=True)) for row in rendered
    )
    return "\n".join(lines)


def split_statements(buffer: str) -> list[str]:
    """Divide un buffer en sentencias individuales por sus tokens `;`.

    Usa el lexer real del ecosistema (no un split de cadena): un `;` dentro
    de un literal de cadena o de un identificador entrecomillado no separa
    nada. Si el buffer no tokeniza, se devuelve entero: el error, bien
    formateado, lo dará el parser al ejecutar.
    """
    try:
        tokens = tokenize(buffer)
    except SqlParserError:
        return [buffer]
    # Offset absoluto de cada inicio de línea, para convertir la posición
    # (línea, columna) de un token en un índice dentro del buffer.
    line_offsets = [0]
    for line in buffer.splitlines(keepends=True):
        line_offsets.append(line_offsets[-1] + len(line))
    statements: list[str] = []
    start = 0
    for token in tokens:
        if token.type is TokenType.SEMICOLON:
            cut = line_offsets[token.position.line - 1] + token.position.column - 1
            piece = buffer[start:cut].strip()
            if piece:
                statements.append(piece)
            start = cut + 1
    tail = buffer[start:].strip()
    if tail:
        statements.append(tail)
    return statements


def _statement_complete(buffer: str) -> bool:
    """Una sentencia REPL está completa cuando el último token significativo
    es `;`. Si el buffer aún no tokeniza (una cadena sin cerrar a mitad de
    escritura), se sigue leyendo líneas."""
    try:
        tokens = tokenize(buffer)
    except SqlParserError:
        return False
    meaningful = [t for t in tokens if t.type is not TokenType.EOF]
    return bool(meaningful) and meaningful[-1].type is TokenType.SEMICOLON


def _execute_and_print(connection: Connection, sql: str, out: Callable[[str], None]) -> None:
    started = time.perf_counter()
    cursor = connection.execute(sql)
    elapsed_ms = (time.perf_counter() - started) * 1000
    if cursor.description is not None:
        rows = cursor.fetchall()
        out(format_table([d[0] for d in cursor.description], rows))
        out(f"({len(rows)} filas, {elapsed_ms:.1f} ms)")
    elif cursor.rowcount >= 0:
        out(f"OK ({cursor.rowcount} filas afectadas, {elapsed_ms:.1f} ms)")
    else:
        out(f"OK ({elapsed_ms:.1f} ms)")


def _meta_command(connection: Connection, line: str, out: Callable[[str], None]) -> bool:
    """Ejecuta un metacomando. Devuelve False si el REPL debe terminar."""
    parts = line.split()
    command, args = parts[0], parts[1:]
    if command in (".quit", ".exit"):
        return False
    if command == ".help":
        out(_HELP)
    elif command == ".tables":
        catalog = connection._require_engine().catalog
        for name in sorted(catalog.tables):
            out(name)
    elif command == ".schema":
        catalog = connection._require_engine().catalog
        names = args if args else sorted(catalog.tables)
        for name in names:
            table = catalog.tables.get(name)
            if table is None:
                out(f"Error: la tabla '{name}' no existe")
                continue
            out(f"CREATE TABLE {table.name} (")
            for column in table.columns:
                flags = "".join(
                    (
                        " PRIMARY KEY" if column.primary_key else "",
                        " NOT NULL" if column.not_null and not column.primary_key else "",
                        " UNIQUE" if column.unique else "",
                        f" DEFAULT {_format_value(column.default)}" if column.has_default else "",
                    )
                )
                out(f"  {column.name} {column.column_type.name}{flags}")
            out(")")
            for index in catalog.indexes_of(name):
                unique = "UNIQUE " if index.unique else ""
                out(f"CREATE {unique}INDEX {index.name} ON {name} ({', '.join(index.columns)})")
    else:
        out(f"Error: metacomando desconocido {command!r} (usa .help)")
    return True


def repl(connection: Connection, out: Callable[[str], None] = print) -> None:
    out(f"nanosql {__version__} — motor: {connection.backend}")
    out('Escribe SQL terminado en ";", ".help" para ayuda, ".quit" para salir.')
    buffer = ""
    while True:
        if buffer:
            prompt = _PROMPT_CONTINUATION
        else:
            prompt = _PROMPT_TXN if connection.in_transaction else _PROMPT
        try:
            line = input(prompt)
        except EOFError:
            out("")
            return
        except KeyboardInterrupt:
            buffer = ""
            out("")
            continue
        stripped = line.strip()
        if not buffer and not stripped:
            continue
        if not buffer and stripped.startswith("."):
            if not _meta_command(connection, stripped, out):
                return
            continue
        buffer = f"{buffer}\n{line}" if buffer else line
        if not _statement_complete(buffer):
            continue
        statements, buffer = split_statements(buffer), ""
        for statement in statements:
            try:
                _execute_and_print(connection, statement, out)
            except NanosqlError as exc:
                out(f"Error: {exc}")
                break


def _run_command(connection: Connection, script: str, out: Callable[[str], None]) -> int:
    try:
        for statement in split_statements(script):
            _execute_and_print(connection, statement, out)
    except NanosqlError as exc:
        out(f"Error: {exc}")
        return 1
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nanosql",
        description=(
            "Motor SQL embebible construido ensamblando los subproyectos de strata-database-engine."
        ),
    )
    parser.add_argument(
        "database",
        nargs="?",
        default=":memory:",
        help="directorio de la base de datos, o :memory: (por defecto)",
    )
    parser.add_argument(
        "--backend",
        choices=["btree", "lsm"],
        default=None,
        help="motor de storage al crear la base de datos (por defecto: btree)",
    )
    parser.add_argument(
        "--isolation",
        choices=["read_committed", "repeatable_read", "serializable"],
        default="repeatable_read",
        help="nivel de aislamiento de la conexión",
    )
    parser.add_argument(
        "-c",
        "--command",
        metavar="SQL",
        help="ejecuta las sentencias y termina (sin REPL)",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="ejecuta la batería de benchmark y termina",
    )
    parser.add_argument("--version", action="version", version=f"nanosql {__version__}")
    return parser


def _build_serve_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nanosql serve",
        description=(
            "Sirve una base de datos nanosql por el protocolo de red de PostgreSQL (v3): "
            "psql, psycopg, SQLAlchemy y cualquier cliente de Postgres pueden conectarse."
        ),
    )
    parser.add_argument("database", help="directorio de la base de datos, o :memory:")
    parser.add_argument("--host", default="127.0.0.1", help="interfaz (por defecto 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5432, help="puerto TCP (por defecto 5432)")
    parser.add_argument(
        "--auth",
        choices=["trust", "scram-sha-256"],
        default="trust",
        help="método de autenticación (por defecto trust)",
    )
    parser.add_argument(
        "--user",
        action="append",
        default=[],
        metavar="USUARIO[:CONTRASEÑA]",
        help=(
            "usuario para scram-sha-256 (repetible); sin ':CONTRASEÑA' se lee de la "
            "variable de entorno NANOSQL_PASSWORD"
        ),
    )
    parser.add_argument("--backend", choices=["btree", "lsm"], default=None)
    parser.add_argument(
        "--isolation",
        choices=["read_committed", "repeatable_read", "serializable"],
        default="read_committed",
        help="aislamiento por defecto de las sesiones (por defecto read_committed, como Postgres)",
    )
    parser.add_argument("--log-level", default="INFO", help="nivel de logging (DEBUG muestra SQL)")
    return parser


def serve_main(argv: Sequence[str]) -> int:
    import logging
    import os

    from .pgwire import PgServer

    args = _build_serve_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    users: dict[str, str] = {}
    for spec in args.user:
        name, sep, password = spec.partition(":")
        if not sep:
            password = os.environ.get("NANOSQL_PASSWORD", "")
            if not password:
                print(f"Error: falta la contraseña de '{name}' (NANOSQL_PASSWORD)", file=sys.stderr)
                return 2
        users[name] = password
    try:
        server = PgServer(
            args.database,
            host=args.host,
            port=args.port,
            auth=args.auth,
            users=users,
            backend=args.backend,
            isolation_level=args.isolation,
        )
    except (ValueError, OSError, NanosqlError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    host, port = server.address
    print(f"nanosql {__version__} sirviendo {args.database} en {host}:{port} (auth={args.auth})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
    return 0


def _build_cluster_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nanosql cluster",
        description=(
            "Clúster nanosql replicado por Raft real (TCP + log en disco), un proceso por nodo."
        ),
    )
    sub = parser.add_subparsers(dest="action", required=True)
    start = sub.add_parser("start", help="arranca N nodos y espera (Ctrl-C para parar)")
    chaos = sub.add_parser("chaos", help="carga + fallos + verificación de linealizabilidad")
    for p in (start, chaos):
        p.add_argument("--nodes", type=int, default=3, choices=[3, 4, 5])
        p.add_argument("--data-dir", type=Path, default=None)
    chaos.add_argument("--duration", type=float, default=60.0, help="segundos de carga")
    chaos.add_argument("--clients", type=int, default=6)
    chaos.add_argument("--keys", type=int, default=4)
    chaos.add_argument("--seed", type=int, default=1)
    chaos.add_argument(
        "--faults",
        default="kill-leader,partition,partition-leader,pause",
        help="tipos de fallo separados por comas ('' = ninguno)",
    )
    chaos.add_argument("--report", type=Path, default=None, help="escribe el informe JSON")
    chaos.add_argument("--history", type=Path, default=None, help="vuelca la historia JSONL")
    return parser


def cluster_main(argv: Sequence[str]) -> int:
    import json
    import tempfile

    from .cluster.chaos import LocalCluster, run_chaos

    args = _build_cluster_parser().parse_args(argv)
    root = args.data_dir or Path(tempfile.mkdtemp(prefix="nanosql-cluster-"))
    if args.action == "start":
        cluster = LocalCluster(args.nodes, root).start()
        print(f"clúster de {args.nodes} nodos en {root}")
        for node_id, (host, port) in cluster.client_addr.items():
            print(f"  {node_id}: clientes en {host}:{port}")
        print("ClusterClient([...]) encuentra al líder solo; Ctrl-C para parar.")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass
        finally:
            cluster.stop()
        return 0
    faults = tuple(f for f in args.faults.split(",") if f)
    report, history = run_chaos(
        nodes=args.nodes,
        duration=args.duration,
        clients=args.clients,
        keys=args.keys,
        seed=args.seed,
        root=root,
        fault_kinds=faults,
        progress=True,
    )
    summary = report.to_json()
    print(json.dumps(summary, indent=2))
    if args.report is not None:
        args.report.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    if args.history is not None:
        with open(args.history, "w", encoding="utf-8") as handle:
            for op in history:
                handle.write(
                    json.dumps(
                        {
                            "process": op.process,
                            "key": op.key,
                            "kind": op.kind,
                            "value": op.value,
                            "output": op.output,
                            "call": op.call,
                            "ret": None if op.pending else op.ret,
                        }
                    )
                    + "\n"
                )
    return 0 if summary["linearizable"] else 1


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments[:1] == ["serve"]:
        return serve_main(arguments[1:])
    if arguments[:1] == ["cluster"]:
        return cluster_main(arguments[1:])
    args = _build_parser().parse_args(arguments)
    if args.benchmark:
        from .benchmark import run_benchmark

        run_benchmark(backend=args.backend, out=print)
        return 0
    try:
        connection = connect(
            args.database,
            backend=args.backend,
            isolation_level=args.isolation,
            autocommit=True,
        )
    except NanosqlError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    try:
        if args.command is not None:
            return _run_command(connection, args.command, print)
        try:
            import readline  # noqa: F401  — historial y edición de línea del REPL
        except ImportError:
            pass
        repl(connection)
        return 0
    finally:
        connection.close()
