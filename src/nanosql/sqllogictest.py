"""Runner del formato sqllogictest de SQLite contra nanosql.

Formato (el de https://www.sqlite.org/sqllogictest/): registros separados
por líneas en blanco —

* ``statement ok`` / ``statement error`` + SQL;
* ``query <tipos> [nosort|rowsort|valuesort] [etiqueta]`` + SQL + ``----``
  + resultado esperado (un valor por línea, o ``N values hashing to <md5>``);
* ``hash-threshold N``, ``halt``, ``skipif <db>`` / ``onlyif <db>``.

Cada valor se formatea como lo hace la implementación de referencia para
SQLite (``sqllogictest.c``): tipo ``I`` → entero (los reales se truncan),
``R`` → ``%.3f``, ``T`` → texto con los no imprimibles como ``@``; ``NULL``
y ``(empty)`` para nulo y cadena vacía. Este runner se identifica como la
base de datos ``nanosql``: los registros ``onlyif sqlite``/``onlyif mysql``
se saltan y los ``skipif <otra>`` se ejecutan.

Cada fallo se clasifica:

* ``unsupported`` — nanosql lo rechaza con un error tipado de "fuera del
  subconjunto documentado": ``NotSupportedError`` o error de sintaxis
  (SQLSTATE 42601/0A000), o cualquier fallo sobre una tabla que una
  sentencia anterior no soportada dejó sin crear o sin poblar (cascada:
  `INSERT INTO t SELECT ...` no soportado deja `t` vacía y todas sus
  consultas fallan por eso, no por un resultado incorrecto propio).
* ``bug`` — nanosql da un resultado incorrecto: valores distintos, número
  de columnas distinto, éxito donde se esperaba error, u otro error tipado
  sobre SQL dentro del subconjunto.
* ``resource`` — la sentencia superó el límite de tiempo o de memoria
  (``--timeout``, ``--memory-limit-mb``): ni correcto ni incorrecto, sin
  terminar.

Uso::

    python -m nanosql.sqllogictest tests/sqllogictest/corpus/*.test.gz \\
        --json results.json [--check expected.json]
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import signal
import sys
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType

from .catalog import ColumnValue
from .driver import connect
from .errors import NanosqlError, NotSupportedError

DATABASE_NAME = "nanosql"
_HASH_LINE = re.compile(r"^(\d+) values hashing to ([0-9a-f]{32})$")


@dataclass(slots=True)
class Record:
    kind: str  # "statement" | "query"
    line: int
    sql: str
    expect_error: bool = False
    types: str = ""
    sort_mode: str = "nosort"
    label: str | None = None
    expected: list[str] | None = None
    expected_hash: tuple[int, str] | None = None
    hash_threshold: int = 0


@dataclass(slots=True)
class Failure:
    line: int
    category: str  # unsupported | bug | resource
    sql: str
    detail: str


@dataclass(slots=True)
class FileResult:
    name: str
    run: int = 0
    passed: int = 0
    skipped: int = 0
    seconds: float = 0.0
    failures: list[Failure] = field(default_factory=list)

    @property
    def percent(self) -> float:
        return 100.0 * self.passed / self.run if self.run else 0.0

    def counts(self) -> dict[str, int]:
        counts = {"unsupported": 0, "bug": 0, "resource": 0}
        for failure in self.failures:
            counts[failure.category] += 1
        return counts


# ---------------------------------------------------------------------------
# Lectura del formato
# ---------------------------------------------------------------------------


def read_text(path: Path) -> str:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
            return handle.read()
    return path.read_text(encoding="utf-8", errors="replace")


def display_name(path: Path) -> str:
    name = path.name
    for suffix in (".gz", ".test"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name


def parse_records(text: str) -> Iterator[Record]:
    """Itera los registros ejecutables del fichero, ya filtrados por
    `skipif`/`onlyif` (los saltados salen con kind="skip")."""
    lines = text.split("\n")
    i = 0
    hash_threshold = 0
    while i < len(lines):
        line = lines[i].rstrip("\r")
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            i += 1
            continue
        start = i
        skip = False
        # condiciones
        while stripped.split(" ", 1)[0] in ("skipif", "onlyif"):
            words = stripped.split()
            target = words[1] if len(words) > 1 else ""
            if words[0] == "skipif" and target == DATABASE_NAME:
                skip = True
            if words[0] == "onlyif" and target != DATABASE_NAME:
                skip = True
            i += 1
            stripped = lines[i].strip() if i < len(lines) else ""
        words = stripped.split()
        head = words[0] if words else ""
        if head == "halt":
            if skip:
                i += 1
                continue
            return
        if head == "hash-threshold":
            if not skip:
                hash_threshold = int(words[1])
            i += 1
            continue
        if head not in ("statement", "query"):
            # Directiva desconocida: se ignora el bloque entero.
            while i < len(lines) and lines[i].strip():
                i += 1
            continue
        i += 1
        sql_lines: list[str] = []
        while i < len(lines) and lines[i].strip() and lines[i].rstrip("\r") != "----":
            sql_lines.append(lines[i].rstrip("\r"))
            i += 1
        expected: list[str] | None = None
        if i < len(lines) and lines[i].rstrip("\r") == "----":
            i += 1
            expected = []
            while i < len(lines) and lines[i].strip():
                expected.append(lines[i].rstrip("\r"))
                i += 1
        sql = "\n".join(sql_lines)
        if skip:
            yield Record(kind="skip", line=start + 1, sql=sql)
            continue
        if head == "statement":
            yield Record(
                kind="statement",
                line=start + 1,
                sql=sql,
                expect_error=len(words) > 1 and words[1] == "error",
            )
            continue
        record = Record(
            kind="query",
            line=start + 1,
            sql=sql,
            types=words[1] if len(words) > 1 else "",
            sort_mode=words[2] if len(words) > 2 else "nosort",
            label=words[3] if len(words) > 3 else None,
            hash_threshold=hash_threshold,
        )
        if expected is not None and len(expected) == 1:
            match = _HASH_LINE.match(expected[0].strip())
            if match is not None:
                record.expected_hash = (int(match.group(1)), match.group(2))
                expected = None
        record.expected = [value.strip() for value in expected] if expected is not None else []
        yield record


# ---------------------------------------------------------------------------
# Formateo de valores (reglas de sqllogictest.c para SQLite)
# ---------------------------------------------------------------------------

_LEADING_INT = re.compile(r"^\s*[+-]?\d+")
_LEADING_REAL = re.compile(r"^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")


def format_value(value: ColumnValue, type_char: str) -> str:
    if value is None:
        return "NULL"
    if type_char == "I":
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            return str(int(value)) if value == value and abs(value) != float("inf") else "0"
        match = _LEADING_INT.match(value)
        return str(int(match.group(0))) if match else "0"
    if type_char == "R":
        if isinstance(value, bool):
            return "1.000" if value else "0.000"
        if isinstance(value, int | float):
            return f"{float(value):.3f}"
        match = _LEADING_REAL.match(value)
        return f"{float(match.group(0)):.3f}" if match else "0.000"
    # T (y cualquier otro carácter de tipo)
    if isinstance(value, bool):
        text = "1" if value else "0"
    elif isinstance(value, float):
        text = repr(value) if not value.is_integer() else f"{value:.1f}"
    else:
        text = str(value)
    if text == "":
        return "(empty)"
    return "".join(ch if " " <= ch <= "~" else "@" for ch in text)


def hash_values(values: Sequence[str]) -> str:
    digest = hashlib.md5()  # noqa: S324 — el formato de referencia usa MD5
    for value in values:
        digest.update(value.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def arrange(values: list[str], ncols: int, sort_mode: str) -> list[str]:
    if sort_mode == "rowsort" and ncols > 0:
        rows = [values[i : i + ncols] for i in range(0, len(values), ncols)]
        rows.sort()
        return [value for row in rows for value in row]
    if sort_mode == "valuesort":
        return sorted(values)
    return values


# ---------------------------------------------------------------------------
# Ejecución
# ---------------------------------------------------------------------------


class _Timeout(Exception):  # noqa: N818 — señal interna, nunca escapa del runner
    pass


def _on_alarm(signum: int, frame: FrameType | None) -> None:
    raise _Timeout()


# SQLSTATEs de "fuera del subconjunto": sintaxis no soportada, característica
# no soportada, y la división por cero, que nanosql rechaza como PostgreSQL
# (22012) donde SQLite devuelve NULL — una diferencia documentada, no un
# resultado incorrecto.
_UNSUPPORTED_SQLSTATES = frozenset({"42601", "0A000", "22012"})


def _classify_error(exc: NanosqlError) -> str:
    if isinstance(exc, NotSupportedError) or exc.sqlstate in _UNSUPPORTED_SQLSTATES:
        return "unsupported"
    return "bug"


# Tabla cuyo contenido o existencia depende de una sentencia: si esa
# sentencia falla, todo lo que lea la tabla después hereda el fallo.
_TARGET_TABLE = re.compile(
    r"^\s*(?:CREATE\s+(?:TEMP(?:ORARY)?\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"|INSERT\s+(?:OR\s+\w+\s+)?INTO\s+|REPLACE\s+INTO\s+|UPDATE\s+(?:OR\s+\w+\s+)?"
    r"|DELETE\s+FROM\s+|DROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?)(\w+)",
    re.I,
)


def _cascade(sql: str, tainted: dict[str, tuple[int, str]]) -> tuple[int, str] | None:
    """Si `sql` lee una tabla "contaminada" por un fallo anterior, devuelve
    la línea y categoría de ese fallo de origen."""
    for name, origin in tainted.items():
        if re.search(rf"\b{re.escape(name)}\b", sql, re.I):
            return origin
    return None


def run_file(path: Path, *, timeout: float = 10.0) -> FileResult:
    result = FileResult(name=display_name(path))
    started = time.perf_counter()
    conn = connect(":memory:", autocommit=True)
    tainted: dict[str, tuple[int, str]] = {}
    use_alarm = hasattr(signal, "setitimer") and timeout > 0
    previous = signal.signal(signal.SIGALRM, _on_alarm) if use_alarm else None
    try:
        for record in parse_records(read_text(path)):
            if record.kind == "skip":
                result.skipped += 1
                continue
            result.run += 1
            failure = _run_record(conn, record, timeout if use_alarm else 0.0)
            if failure is None:
                result.passed += 1
                continue
            origin = _cascade(record.sql, tainted)
            if origin is not None and failure.category != "resource":
                # Fallo heredado: la tabla quedó mal por una sentencia
                # anterior; se clasifica como aquella (no soportada → no
                # soportado; bug → bug) y se anota el origen.
                failure.category = origin[1]
                failure.detail = f"cascada del fallo de L{origin[0]}: {failure.detail}"
            result.failures.append(failure)
            if record.kind == "statement" and not record.expect_error:
                match = _TARGET_TABLE.match(record.sql)
                if match is not None:
                    tainted.setdefault(match.group(1).lower(), (record.line, failure.category))
            if conn.in_transaction:
                conn.rollback()
    finally:
        if use_alarm:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
        conn.close()
    result.seconds = time.perf_counter() - started
    return result


def _run_record(conn: object, record: Record, timeout: float) -> Failure | None:
    from .driver import Connection

    assert isinstance(conn, Connection)
    rows: list[tuple[ColumnValue, ...]] = []
    ncols = 0
    try:
        if timeout:
            signal.setitimer(signal.ITIMER_REAL, timeout)
        try:
            cursor = conn.execute(record.sql)
            if cursor.description is not None:
                ncols = len(cursor.description)
                rows = cursor.fetchall()
        finally:
            if timeout:
                signal.setitimer(signal.ITIMER_REAL, 0)
    except _Timeout:
        if conn.in_transaction:
            conn.rollback()
        return Failure(record.line, "resource", record.sql, f"más de {timeout:.0f} s")
    except MemoryError:
        if timeout:
            signal.setitimer(signal.ITIMER_REAL, 0)
        if conn.in_transaction:
            conn.rollback()
        return Failure(record.line, "resource", record.sql, "límite de memoria del proceso")
    except NanosqlError as exc:
        if record.kind == "statement" and record.expect_error:
            return None
        first_line = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
        detail = f"{type(exc).__name__} [{exc.sqlstate}]: {first_line}"
        return Failure(record.line, _classify_error(exc), record.sql, detail)
    if record.kind == "statement":
        if record.expect_error:
            return Failure(record.line, "bug", record.sql, "se esperaba un error y no lo hubo")
        return None
    if ncols != len(record.types):
        return Failure(
            record.line,
            "bug",
            record.sql,
            f"{ncols} columnas devueltas, se esperaban {len(record.types)}",
        )
    values = [format_value(value, record.types[i]) for row in rows for i, value in enumerate(row)]
    values = arrange(values, ncols, record.sort_mode)
    if record.expected_hash is not None:
        count, digest = record.expected_hash
        if len(values) == count and hash_values(values) == digest:
            return None
        return Failure(
            record.line,
            "bug",
            record.sql,
            f"esperados {count} valores con hash {digest}; obtenidos {len(values)} "
            f"con hash {hash_values(values)}",
        )
    expected = record.expected or []
    if values == expected:
        return None
    shown = values[:8]
    return Failure(
        record.line,
        "bug",
        record.sql,
        f"esperado {expected[:8]}{'…' if len(expected) > 8 else ''}, "
        f"obtenido {shown}{'…' if len(values) > 8 else ''}",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def summary_json(results: list[FileResult]) -> dict[str, object]:
    files: dict[str, object] = {}
    for result in results:
        files[result.name] = {
            "run": result.run,
            "passed": result.passed,
            "skipped": result.skipped,
            "percent": round(result.percent, 2),
            **result.counts(),
        }
    return {"files": files}


def format_table(results: list[FileResult]) -> str:
    lines = [
        "| file | run | passed | % | unsupported | bug | resource | seconds |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    total_run = total_passed = 0
    for r in results:
        c = r.counts()
        total_run += r.run
        total_passed += r.passed
        lines.append(
            f"| {r.name} | {r.run} | {r.passed} | {r.percent:.1f} | {c['unsupported']} | "
            f"{c['bug']} | {c['resource']} | {r.seconds:.1f} |"
        )
    pct = 100.0 * total_passed / total_run if total_run else 0.0
    lines.append(f"| **total** | {total_run} | {total_passed} | {pct:.1f} | | | | |")
    return "\n".join(lines)


def check_regressions(results: list[FileResult], expected_path: Path) -> list[str]:
    """Compara contra un JSON guardado: cualquier fichero con menos
    registros superados que antes es una regresión."""
    expected = json.loads(expected_path.read_text(encoding="utf-8"))["files"]
    problems: list[str] = []
    for result in results:
        previous = expected.get(result.name)
        if previous is None:
            continue
        if result.passed < int(previous["passed"]):
            problems.append(
                f"{result.name}: {result.passed} superados, antes {previous['passed']} "
                f"({result.percent:.2f}% < {previous['percent']}%)"
            )
    return problems


def limit_memory(megabytes: int) -> None:
    """Tope de memoria virtual (RLIMIT_AS): una consulta patológica (un
    producto cartesiano enorme) produce `MemoryError`, que se clasifica como
    `resource`, en vez de que el kernel mate el run entero."""
    try:
        import resource

        limit = megabytes * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    except (ImportError, ValueError, OSError):
        pass  # plataforma sin RLIMIT_AS: se sigue sin tope


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m nanosql.sqllogictest")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--json", type=Path, help="escribe el resumen por fichero en JSON")
    parser.add_argument("--check", type=Path, help="falla si algún fichero supera menos que aquí")
    parser.add_argument("--timeout", type=float, default=10.0, help="segundos por sentencia")
    parser.add_argument(
        "--memory-limit-mb",
        type=int,
        default=4096,
        help="límite de memoria virtual del proceso (0 = sin límite)",
    )
    parser.add_argument(
        "--failures", type=int, default=0, help="muestra hasta N fallos por fichero"
    )
    parser.add_argument("--category", default=None, help="filtra los fallos mostrados")
    args = parser.parse_args(argv)
    if args.memory_limit_mb > 0:
        limit_memory(args.memory_limit_mb)
    results: list[FileResult] = []
    for path in args.files:
        result = run_file(path, timeout=args.timeout)
        results.append(result)
        c = result.counts()
        print(
            f"{result.name}: {result.passed}/{result.run} ({result.percent:.1f}%) "
            f"unsupported={c['unsupported']} bug={c['bug']} resource={c['resource']} "
            f"skipped={result.skipped} [{result.seconds:.1f}s]",
            flush=True,
        )
        shown = [f for f in result.failures if args.category in (None, f.category)]
        for failure in shown[: args.failures]:
            sql = " ".join(failure.sql.split())
            print(f"  L{failure.line} {failure.category}: {sql[:160]}\n    → {failure.detail}")
    print(format_table(results))
    if args.json is not None:
        args.json.write_text(
            json.dumps(summary_json(results), indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    if args.check is not None:
        problems = check_regressions(results, args.check)
        for problem in problems:
            print(f"REGRESIÓN: {problem}", file=sys.stderr)
        return 1 if problems else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
