"""Análisis léxico mínimo del SQL que llega por el protocolo de PostgreSQL.

El servidor necesita entender el texto *antes* de dárselo a nanosql: partir
un Query simple en sentencias, reconocer BEGIN/SET/SHOW, traducir los
parámetros `$1, $2...` del protocolo al estilo qmark (`?`) del driver,
aplicar los casts `::tipo` sobre parámetros y literales (SQLAlchemy los
genera en cada parámetro) y detectar consultas al catálogo del sistema. Para
todo eso basta un lexer que respete las reglas de citado de PostgreSQL
(`'..'`, `E'..'`, `"..."`, `$tag$..$tag$`, comentarios `--` y `/* */`
anidados); la gramática de verdad sigue siendo la de `sql-query-parser`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from ..catalog import ColumnValue
from ..errors import ProgrammingError
from .types import SQL_TYPE_NAME_TO_OID, coerce_to_oid


@dataclass(frozen=True, slots=True)
class Tok:
    kind: str  # ws, comment, string, ident, qident, number, param, op, punct
    text: str  # texto original
    value: str = ""  # contenido decodificado (cadenas, identificadores)

    @property
    def upper(self) -> str:
        return self.text.upper() if self.kind == "ident" else ""

    @property
    def lower(self) -> str:
        return self.text.lower() if self.kind == "ident" else ""


_IDENT_START = re.compile(r"[A-Za-z_\u0080-￿]")
_IDENT_BODY = re.compile(r"[A-Za-z0-9_$\u0080-￿]*")
_NUMBER = re.compile(r"(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")
_PARAM = re.compile(r"\$(\d+)")
_DOLLAR_TAG = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")
_OPERATOR_CHARS = set("+-*/<>=~!@#%^&|`?")


def tokenize(sql: str) -> list[Tok]:
    tokens: list[Tok] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch.isspace():
            j = i
            while j < n and sql[j].isspace():
                j += 1
            tokens.append(Tok("ws", sql[i:j]))
            i = j
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j < 0 else j
            tokens.append(Tok("comment", sql[i:j]))
            i = j
        elif sql.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif sql.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            tokens.append(Tok("comment", sql[i:j]))
            i = j
        elif ch in "eE" and i + 1 < n and sql[i + 1] == "'":
            j, value = _scan_escape_string(sql, i + 2)
            tokens.append(Tok("string", sql[i:j], value))
            i = j
        elif ch == "'":
            j, value = _scan_quoted(sql, i + 1, "'")
            tokens.append(Tok("string", sql[i:j], value))
            i = j
        elif ch == '"':
            j, value = _scan_quoted(sql, i + 1, '"')
            tokens.append(Tok("qident", sql[i:j], value))
            i = j
        elif ch == "$":
            param = _PARAM.match(sql, i)
            tag = _DOLLAR_TAG.match(sql, i)
            if param is not None:
                tokens.append(Tok("param", param.group(0), param.group(1)))
                i = param.end()
            elif tag is not None:
                delimiter = tag.group(0)
                end = sql.find(delimiter, tag.end())
                if end < 0:
                    raise ProgrammingError("cadena con dólares sin terminar", sqlstate="42601")
                tokens.append(Tok("string", sql[i : end + len(delimiter)], sql[tag.end() : end]))
                i = end + len(delimiter)
            else:
                tokens.append(Tok("punct", ch))
                i += 1
        elif ch.isdigit() or (ch == "." and i + 1 < n and sql[i + 1].isdigit()):
            match = _NUMBER.match(sql, i)
            assert match is not None
            tokens.append(Tok("number", match.group(0)))
            i = match.end()
        elif _IDENT_START.match(ch):
            match = _IDENT_BODY.match(sql, i + 1)
            j = match.end() if match else i + 1
            tokens.append(Tok("ident", sql[i:j], sql[i:j]))
            i = j
        elif ch == ":" and sql.startswith("::", i):
            tokens.append(Tok("op", "::"))
            i += 2
        elif ch in _OPERATOR_CHARS:
            j = i
            while j < n and sql[j] in _OPERATOR_CHARS and not sql.startswith("--", j):
                j += 1
            tokens.append(Tok("op", sql[i:j]))
            i = j
        else:
            tokens.append(Tok("punct", ch))
            i += 1
    return tokens


def _scan_quoted(sql: str, start: int, quote: str) -> tuple[int, str]:
    parts: list[str] = []
    i = start
    while True:
        end = sql.find(quote, i)
        if end < 0:
            kind = "cadena" if quote == "'" else "identificador entrecomillado"
            raise ProgrammingError(f"{kind} sin terminar", sqlstate="42601")
        parts.append(sql[i:end])
        if sql.startswith(quote * 2, end):
            parts.append(quote)
            i = end + 2
            continue
        return end + 1, "".join(parts)


_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f", "\\": "\\", "'": "'"}


def _scan_escape_string(sql: str, start: int) -> tuple[int, str]:
    parts: list[str] = []
    i, n = start, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "\\" and i + 1 < n:
            parts.append(_ESCAPES.get(sql[i + 1], sql[i + 1]))
            i += 2
        elif ch == "'":
            if sql.startswith("''", i):
                parts.append("'")
                i += 2
            else:
                return i + 1, "".join(parts)
        else:
            parts.append(ch)
            i += 1
    raise ProgrammingError("cadena sin terminar", sqlstate="42601")


def meaningful(tokens: list[Tok]) -> list[Tok]:
    return [t for t in tokens if t.kind not in ("ws", "comment")]


def split_statements(sql: str) -> list[str]:
    """Parte un mensaje Query simple por los `;` de nivel superior. Las
    sentencias vacías (sólo espacios/comentarios) se descartan."""
    pieces: list[str] = []
    current: list[str] = []
    for token in tokenize(sql):
        if token.kind == "punct" and token.text == ";":
            pieces.append("".join(current))
            current = []
        else:
            current.append(token.text)
    pieces.append("".join(current))
    return [p for p in pieces if meaningful(tokenize(p))]


def head_words(sql: str, count: int = 3) -> list[str]:
    """Primeras palabras clave (en mayúsculas) de una sentencia."""
    words: list[str] = []
    for token in meaningful(tokenize(sql)):
        if token.kind != "ident":
            break
        words.append(token.upper)
        if len(words) == count:
            break
    return words


# ---------------------------------------------------------------------------
# Casts y parámetros
# ---------------------------------------------------------------------------

# Primeras palabras de tipos multi-palabra y sus continuaciones posibles.
_MULTIWORD_CONTINUATIONS = {
    "double": ("precision",),
    "character": ("varying",),
    "timestamp": ("with", "without", "time", "zone"),
    "time": ("with", "without", "time", "zone"),
    "bit": ("varying",),
}


def _read_type_name(tokens: list[Tok], start: int) -> tuple[str, int]:
    """Lee un nombre de tipo tras `::` empezando en `tokens[start]` (lista sin
    espacios). Devuelve `(nombre_normalizado, índice_siguiente)`."""
    i = start
    if i >= len(tokens) or tokens[i].kind not in ("ident", "qident"):
        raise ProgrammingError("se esperaba un nombre de tipo tras '::'", sqlstate="42601")
    name = tokens[i].value.lower() if tokens[i].kind == "qident" else tokens[i].lower
    i += 1
    if name == "pg_catalog" and i + 1 < len(tokens) and tokens[i].text == ".":
        name = tokens[i + 1].lower or tokens[i + 1].value.lower()
        i += 2
    continuations = _MULTIWORD_CONTINUATIONS.get(name, ())
    while i < len(tokens) and tokens[i].lower in continuations:
        name = f"{name} {tokens[i].lower}"
        i += 1
    if i < len(tokens) and tokens[i].text == "(":
        depth = 0
        while i < len(tokens):
            if tokens[i].text == "(":
                depth += 1
            elif tokens[i].text == ")":
                depth -= 1
                if depth == 0:
                    i += 1
                    break
            i += 1
    while i + 1 < len(tokens) and tokens[i].text == "[" and tokens[i + 1].text == "]":
        name += "[]"
        i += 2
    return name, i


def _cast_oid(type_name: str) -> int | None:
    base = re.sub(r"\s+(with|without) time zone$", "", type_name)
    return SQL_TYPE_NAME_TO_OID.get(base)


@dataclass(frozen=True, slots=True)
class PreparedText:
    """SQL listo para el driver qmark de nanosql.

    `sql` usa `?`; `param_order[k]` es el índice (0-based) del parámetro `$n`
    que corresponde al k-ésimo `?`; `param_casts` el OID impuesto por un
    `$n::tipo` (el cast fija el tipo del parámetro, como en PostgreSQL);
    `param_count` el mayor `$n` visto.
    """

    sql: str
    param_order: tuple[int, ...]
    param_casts: dict[int, int]
    param_count: int

    def bind(self, values: list[ColumnValue]) -> list[ColumnValue]:
        bound: list[ColumnValue] = []
        for index in self.param_order:
            if index >= len(values):
                raise ProgrammingError(
                    f"no hay valor para el parámetro ${index + 1}", sqlstate="08P01"
                )
            value = values[index]
            cast = self.param_casts.get(index)
            bound.append(coerce_to_oid(value, cast) if cast is not None else value)
        return bound


def _literal_sql(value: ColumnValue) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    return "'" + value.replace("'", "''") + "'"


def prepare_text(sql: str) -> PreparedText:
    """Traduce `$n` → `?` y elimina los casts `::tipo` sobre parámetros y
    literales (aplicándolos al valor). Los casts sobre columnas o
    expresiones se dejan intactos: nanosql los rechazará con un error de
    sintaxis claro, nunca con un resultado silenciosamente distinto."""
    tokens = tokenize(sql)
    out: list[str] = []
    order: list[int] = []
    casts: dict[int, int] = {}
    max_param = 0
    sig = [i for i, t in enumerate(tokens) if t.kind not in ("ws", "comment")]
    sig_tokens = [tokens[i] for i in sig]
    position = {raw: k for k, raw in enumerate(sig)}
    skip_until = -1
    for raw_index, token in enumerate(tokens):
        if raw_index < skip_until:
            continue
        k = position.get(raw_index)
        if token.kind == "param":
            index = int(token.value) - 1
            if index < 0:
                raise ProgrammingError("parámetro $0 inválido", sqlstate="42P02")
            max_param = max(max_param, index + 1)
            order.append(index)
            out.append("?")
            if k is not None and k + 1 < len(sig_tokens) and sig_tokens[k + 1].text == "::":
                type_name, next_k = _read_type_name(sig_tokens, k + 2)
                oid = _cast_oid(type_name)
                if oid is not None:
                    previous = casts.get(index)
                    if previous is None:
                        casts[index] = oid
                skip_until = sig[next_k - 1] + 1 if next_k - 1 < len(sig) else len(tokens)
            continue
        if (
            token.kind in ("string", "number")
            or (token.kind == "ident" and token.upper in ("NULL", "TRUE", "FALSE"))
        ) and k is not None:
            if k + 1 < len(sig_tokens) and sig_tokens[k + 1].text == "::":
                type_name, next_k = _read_type_name(sig_tokens, k + 2)
                oid = _cast_oid(type_name)
                if oid is not None:
                    if token.kind == "string":
                        value: ColumnValue = token.value
                    elif token.kind == "number":
                        value = (
                            float(token.text)
                            if any(c in token.text for c in ".eE")
                            else int(token.text)
                        )
                    else:
                        value = {"NULL": None, "TRUE": True, "FALSE": False}[token.upper]
                    out.append(_literal_sql(coerce_to_oid(value, oid)))
                    skip_until = sig[next_k - 1] + 1 if next_k - 1 < len(sig) else len(tokens)
                    continue
        if token.kind == "string" and token.text[:1] in ("e", "E", "$"):
            # E'..' y $$..$$ no existen en el lexer de nanosql: se re-emiten
            # como literal estándar con el mismo contenido.
            out.append(_literal_sql(token.value))
            continue
        out.append(token.text)
    text = "".join(out).strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    return PreparedText(
        sql=text, param_order=tuple(order), param_casts=casts, param_count=max_param
    )


def substitute_params(sql: str, values: list[ColumnValue]) -> str:
    """Sustituye `$n` por literales SQL (sólo para el emulador de catálogo,
    que reconoce consultas por patrones de texto; nunca se ejecuta)."""
    out: list[str] = []
    for token in tokenize(sql):
        if token.kind == "param":
            index = int(token.value) - 1
            out.append(_literal_sql(values[index]) if 0 <= index < len(values) else "NULL")
        else:
            out.append(token.text)
    return "".join(out)
