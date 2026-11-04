"""Correspondencia entre los tipos de nanosql y los OIDs de PostgreSQL, y
codificación de valores en formato texto y binario.

nanosql tiene cuatro tipos columnares (los de `vectorized-execution-engine`):
INTEGER (int64), FLOAT (float64), STRING y BOOLEAN. En el cable viajan como
`int8`, `float8`, `text` y `bool` — los tipos de PostgreSQL con la misma
representación exacta, así que ningún valor pierde precisión al cruzar.
"""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass

from ..catalog import ColumnValue
from ..errors import DataError, NotSupportedError

OID_BOOL = 16
OID_BYTEA = 17
OID_NAME = 19
OID_INT8 = 20
OID_INT2 = 21
OID_INT4 = 23
OID_REGPROC = 24
OID_TEXT = 25
OID_OID = 26
OID_CHAR = 18
OID_FLOAT4 = 700
OID_FLOAT8 = 701
OID_UNKNOWN = 705
OID_BPCHAR = 1042
OID_VARCHAR = 1043
OID_NUMERIC = 1700
OID_REGTYPE = 2206


@dataclass(frozen=True, slots=True)
class PgType:
    oid: int
    name: str
    size: int  # typlen: -1 = longitud variable


PG_TYPES: dict[int, PgType] = {
    t.oid: t
    for t in (
        PgType(OID_BOOL, "boolean", 1),
        PgType(OID_INT2, "smallint", 2),
        PgType(OID_INT4, "integer", 4),
        PgType(OID_INT8, "bigint", 8),
        PgType(OID_OID, "oid", 4),
        PgType(OID_FLOAT4, "real", 4),
        PgType(OID_FLOAT8, "double precision", 8),
        PgType(OID_NUMERIC, "numeric", -1),
        PgType(OID_TEXT, "text", -1),
        PgType(OID_VARCHAR, "character varying", -1),
        PgType(OID_BPCHAR, "character", -1),
        PgType(OID_NAME, "name", 64),
        PgType(OID_CHAR, '"char"', 1),
        PgType(OID_UNKNOWN, "unknown", -2),
    )
}

# Tipo columnar de nanosql (nombre del ColumnType del ejecutor) → OID.
NANOSQL_TYPE_TO_OID: dict[str, int] = {
    "INTEGER": OID_INT8,
    "FLOAT": OID_FLOAT8,
    "STRING": OID_TEXT,
    "BOOLEAN": OID_BOOL,
}

# Nombre de tipo SQL (como aparece en un cast `::tipo`) → OID. Las formas de
# varias palabras se normalizan colapsando espacios y en minúsculas.
SQL_TYPE_NAME_TO_OID: dict[str, int] = {
    "bool": OID_BOOL,
    "boolean": OID_BOOL,
    "smallint": OID_INT2,
    "int2": OID_INT2,
    "integer": OID_INT4,
    "int": OID_INT4,
    "int4": OID_INT4,
    "bigint": OID_INT8,
    "int8": OID_INT8,
    "oid": OID_OID,
    "real": OID_FLOAT4,
    "float4": OID_FLOAT4,
    "float": OID_FLOAT8,
    "float8": OID_FLOAT8,
    "double precision": OID_FLOAT8,
    "numeric": OID_NUMERIC,
    "decimal": OID_NUMERIC,
    "text": OID_TEXT,
    "varchar": OID_VARCHAR,
    "character varying": OID_VARCHAR,
    "char": OID_BPCHAR,
    "character": OID_BPCHAR,
    "bpchar": OID_BPCHAR,
    "name": OID_NAME,
    "regclass": OID_TEXT,
    "regtype": OID_TEXT,
    "unknown": OID_UNKNOWN,
}

_INT_OIDS = frozenset({OID_INT2, OID_INT4, OID_INT8, OID_OID})
_FLOAT_OIDS = frozenset({OID_FLOAT4, OID_FLOAT8})
_TEXT_OIDS = frozenset({OID_TEXT, OID_VARCHAR, OID_BPCHAR, OID_NAME, OID_UNKNOWN, 0, OID_CHAR})

_TRUE_WORDS = frozenset({"t", "true", "y", "yes", "on", "1"})
_FALSE_WORDS = frozenset({"f", "false", "n", "no", "off", "0"})


def type_size(oid: int) -> int:
    known = PG_TYPES.get(oid)
    return known.size if known is not None else -1


def oid_for_python_value(value: ColumnValue) -> int:
    if isinstance(value, bool):
        return OID_BOOL
    if isinstance(value, int):
        return OID_INT8
    if isinstance(value, float):
        return OID_FLOAT8
    return OID_TEXT


# ---------------------------------------------------------------------------
# Parámetros entrantes
# ---------------------------------------------------------------------------


def decode_text_value(text: str, oid: int) -> ColumnValue:
    """Convierte el texto de un parámetro al valor Python que entiende
    nanosql, según el tipo declarado (Parse) o forzado por un cast."""
    if oid in _TEXT_OIDS:
        return text
    if oid == OID_BOOL:
        lowered = text.strip().lower()
        if lowered in _TRUE_WORDS:
            return True
        if lowered in _FALSE_WORDS:
            return False
        raise DataError(
            f'sintaxis de entrada inválida para tipo boolean: "{text}"', sqlstate="22P02"
        )
    if oid in _INT_OIDS:
        try:
            return int(text.strip())
        except ValueError as exc:
            raise DataError(
                f'sintaxis de entrada inválida para tipo entero: "{text}"', sqlstate="22P02"
            ) from exc
    if oid in _FLOAT_OIDS or oid == OID_NUMERIC:
        stripped = text.strip()
        if oid == OID_NUMERIC and _looks_integral(stripped):
            return int(stripped)
        try:
            return float(stripped)
        except ValueError as exc:
            raise DataError(
                f'sintaxis de entrada inválida para tipo numérico: "{text}"', sqlstate="22P02"
            ) from exc
    # Tipo que nanosql no modela (fechas, json, uuid...): viaja como texto y
    # la columna de destino decide si lo acepta.
    return text


def _looks_integral(text: str) -> bool:
    body = text[1:] if text[:1] in "+-" else text
    return body.isdigit()


def decode_binary_value(data: bytes, oid: int) -> ColumnValue:
    if oid in _TEXT_OIDS or oid not in PG_TYPES:
        return data.decode("utf-8")
    if oid == OID_BOOL:
        return data != b"\x00"
    if oid == OID_INT2:
        value: int = struct.unpack("!h", data)[0]
        return value
    if oid in (OID_INT4, OID_OID):
        fmt = "!i" if oid == OID_INT4 else "!I"
        value = struct.unpack(fmt, data)[0]
        return value
    if oid == OID_INT8:
        value = struct.unpack("!q", data)[0]
        return value
    if oid == OID_FLOAT4:
        fvalue: float = struct.unpack("!f", data)[0]
        return fvalue
    if oid == OID_FLOAT8:
        fvalue = struct.unpack("!d", data)[0]
        return fvalue
    raise NotSupportedError(f"formato binario no soportado para el tipo OID {oid}")


def coerce_to_oid(value: ColumnValue, oid: int) -> ColumnValue:
    """Aplica un cast `::tipo` a un valor ya decodificado (p.ej. un parámetro
    enviado como texto `unknown` que el SQL fuerza a `::INTEGER`)."""
    if value is None:
        return None
    if isinstance(value, str):
        return decode_text_value(value, oid)
    if oid in _INT_OIDS and isinstance(value, float) and value.is_integer():
        return int(value)
    if oid in _FLOAT_OIDS and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if oid in _TEXT_OIDS:
        return encode_text_value(value)
    return value


# ---------------------------------------------------------------------------
# Resultados salientes
# ---------------------------------------------------------------------------


def format_float(value: float) -> str:
    """Salida de `float8` como PostgreSQL ≥ 12 con extra_float_digits=1: el
    texto más corto que reproduce el valor exacto, sin `.0` final."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    text = repr(value)
    if text.endswith(".0"):
        text = text[:-2]
    return text


def encode_text_value(value: ColumnValue) -> str:
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, float):
        return format_float(value)
    return str(value)


def encode_value(value: ColumnValue, oid: int, format_code: int) -> bytes | None:
    if value is None:
        return None
    if format_code == 0:
        return encode_text_value(value).encode("utf-8")
    if oid == OID_BOOL:
        return b"\x01" if value else b"\x00"
    if oid == OID_INT8 and isinstance(value, int):
        return struct.pack("!q", value)
    if oid in (OID_INT4, OID_OID) and isinstance(value, int):
        return struct.pack("!i" if oid == OID_INT4 else "!I", value)
    if oid == OID_INT2 and isinstance(value, int):
        return struct.pack("!h", value)
    if oid == OID_FLOAT8 and isinstance(value, int | float):
        return struct.pack("!d", float(value))
    if oid in _TEXT_OIDS:
        return encode_text_value(value).encode("utf-8")
    raise NotSupportedError(f"formato binario no soportado para el tipo OID {oid}")
