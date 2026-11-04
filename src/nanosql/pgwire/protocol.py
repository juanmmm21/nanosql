"""Codificación y decodificación de mensajes del protocolo v3 de PostgreSQL.

Sólo framing y formatos de mensaje: ningún estado de sesión vive aquí. Las
referencias de formato son las de la documentación oficial ("Frontend/
Backend Protocol → Message Formats"); cada constructor de mensaje backend
devuelve los bytes listos para escribir en el socket.
"""

from __future__ import annotations

import socket
import struct
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

PROTOCOL_VERSION_3 = 196608  # 3 << 16
SSL_REQUEST_CODE = 80877103
GSSENC_REQUEST_CODE = 80877104
CANCEL_REQUEST_CODE = 80877102

# Tope defensivo para un mensaje entrante: protege al servidor de una
# longitud absurda (cliente roto o malicioso) que lo haría reservar GBs.
MAX_MESSAGE_BYTES = 64 * 1024 * 1024


class ProtocolError(Exception):
    """El cliente violó el protocolo (mensaje truncado, longitud inválida)."""


class ConnectionClosedError(Exception):
    """El cliente cerró el socket."""


# ---------------------------------------------------------------------------
# Lectura
# ---------------------------------------------------------------------------


def recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(min(remaining, 1 << 16))
        if not chunk:
            raise ConnectionClosedError()
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_startup_packet(sock: socket.socket) -> tuple[int, bytes]:
    """Primer paquete de una conexión: `int32 longitud, int32 código, cuerpo`.
    Devuelve `(código, cuerpo)` donde código es la versión de protocolo o uno
    de los códigos especiales (SSLRequest, GSSENCRequest, CancelRequest)."""
    (length,) = struct.unpack("!i", recv_exact(sock, 4))
    if length < 8 or length > 10_000:
        raise ProtocolError(f"longitud de paquete de arranque inválida: {length}")
    payload = recv_exact(sock, length - 4)
    (code,) = struct.unpack("!i", payload[:4])
    return code, payload[4:]


def read_message(sock: socket.socket) -> tuple[bytes, bytes]:
    """Un mensaje frontend normal: `byte tipo, int32 longitud, cuerpo`."""
    header = recv_exact(sock, 5)
    kind = header[:1]
    (length,) = struct.unpack("!i", header[1:])
    if length < 4 or length > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"longitud de mensaje inválida: {length}")
    return kind, recv_exact(sock, length - 4)


def parse_startup_parameters(body: bytes) -> dict[str, str]:
    """Pares clave/valor terminados en NUL, con un NUL final."""
    parts = body.split(b"\x00")
    params: dict[str, str] = {}
    for i in range(0, len(parts) - 1, 2):
        key = parts[i].decode("utf-8", errors="replace")
        if not key:
            break
        params[key] = parts[i + 1].decode("utf-8", errors="replace")
    return params


class Reader:
    """Cursor sobre el cuerpo de un mensaje frontend."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    def _take(self, size: int) -> bytes:
        if self._pos + size > len(self._data):
            raise ProtocolError("mensaje truncado")
        chunk = self._data[self._pos : self._pos + size]
        self._pos += size
        return chunk

    def byte(self) -> bytes:
        return self._take(1)

    def int16(self) -> int:
        value: int = struct.unpack("!h", self._take(2))[0]
        return value

    def int32(self) -> int:
        value: int = struct.unpack("!i", self._take(4))[0]
        return value

    def cstring(self) -> str:
        end = self._data.find(b"\x00", self._pos)
        if end < 0:
            raise ProtocolError("cadena sin terminador NUL")
        raw = self._data[self._pos : end]
        self._pos = end + 1
        return raw.decode("utf-8")

    def raw(self, size: int) -> bytes:
        return self._take(size)

    def rest(self) -> bytes:
        chunk = self._data[self._pos :]
        self._pos = len(self._data)
        return chunk


# ---------------------------------------------------------------------------
# Mensajes frontend decodificados (protocolo extendido)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ParseMessage:
    statement: str
    query: str
    param_oids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class BindMessage:
    portal: str
    statement: str
    param_formats: tuple[int, ...]
    params: tuple[bytes | None, ...]
    result_formats: tuple[int, ...]


def decode_parse(body: bytes) -> ParseMessage:
    reader = Reader(body)
    name = reader.cstring()
    query = reader.cstring()
    count = reader.int16()
    oids = tuple(reader.int32() for _ in range(count))
    return ParseMessage(statement=name, query=query, param_oids=oids)


def decode_bind(body: bytes) -> BindMessage:
    reader = Reader(body)
    portal = reader.cstring()
    statement = reader.cstring()
    format_count = reader.int16()
    formats = tuple(reader.int16() for _ in range(format_count))
    param_count = reader.int16()
    params: list[bytes | None] = []
    for _ in range(param_count):
        size = reader.int32()
        params.append(None if size == -1 else reader.raw(size))
    result_count = reader.int16()
    result_formats = tuple(reader.int16() for _ in range(result_count))
    return BindMessage(
        portal=portal,
        statement=statement,
        param_formats=formats,
        params=tuple(params),
        result_formats=result_formats,
    )


def expand_formats(formats: Sequence[int], count: int) -> tuple[int, ...]:
    """Regla del protocolo: 0 códigos = todo texto, 1 código = aplica a
    todos, N códigos = uno por parámetro/columna."""
    if not formats:
        return (0,) * count
    if len(formats) == 1:
        return (formats[0],) * count
    if len(formats) != count:
        raise ProtocolError(f"se esperaban {count} códigos de formato, llegaron {len(formats)}")
    return tuple(formats)


# ---------------------------------------------------------------------------
# Mensajes backend
# ---------------------------------------------------------------------------


def message(kind: bytes, payload: bytes = b"") -> bytes:
    return kind + struct.pack("!i", len(payload) + 4) + payload


def cstr(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def authentication_ok() -> bytes:
    return message(b"R", struct.pack("!i", 0))


def authentication_cleartext_password() -> bytes:
    return message(b"R", struct.pack("!i", 3))


def authentication_sasl(mechanisms: Iterable[str]) -> bytes:
    body = struct.pack("!i", 10) + b"".join(cstr(m) for m in mechanisms) + b"\x00"
    return message(b"R", body)


def authentication_sasl_continue(data: bytes) -> bytes:
    return message(b"R", struct.pack("!i", 11) + data)


def authentication_sasl_final(data: bytes) -> bytes:
    return message(b"R", struct.pack("!i", 12) + data)


def parameter_status(name: str, value: str) -> bytes:
    return message(b"S", cstr(name) + cstr(value))


def backend_key_data(pid: int, secret: int) -> bytes:
    return message(b"K", struct.pack("!ii", pid, secret))


def ready_for_query(status: str) -> bytes:
    return message(b"Z", status.encode("ascii"))


@dataclass(frozen=True, slots=True)
class FieldDescription:
    name: str
    type_oid: int
    type_size: int
    format_code: int = 0
    table_oid: int = 0
    column_number: int = 0
    type_modifier: int = -1


def row_description(fields: Sequence[FieldDescription]) -> bytes:
    parts = [struct.pack("!h", len(fields))]
    for f in fields:
        parts.append(cstr(f.name))
        parts.append(
            struct.pack(
                "!ihihih",
                f.table_oid,
                f.column_number,
                f.type_oid,
                f.type_size,
                f.type_modifier,
                f.format_code,
            )
        )
    return message(b"T", b"".join(parts))


def data_row(values: Sequence[bytes | None]) -> bytes:
    parts = [struct.pack("!h", len(values))]
    for value in values:
        if value is None:
            parts.append(struct.pack("!i", -1))
        else:
            parts.append(struct.pack("!i", len(value)))
            parts.append(value)
    return message(b"D", b"".join(parts))


def command_complete(tag: str) -> bytes:
    return message(b"C", cstr(tag))


def empty_query_response() -> bytes:
    return message(b"I")


def parse_complete() -> bytes:
    return message(b"1")


def bind_complete() -> bytes:
    return message(b"2")


def close_complete() -> bytes:
    return message(b"3")


def no_data() -> bytes:
    return message(b"n")


def portal_suspended() -> bytes:
    return message(b"s")


def parameter_description(oids: Sequence[int]) -> bytes:
    return message(
        b"t", struct.pack("!h", len(oids)) + b"".join(struct.pack("!i", o) for o in oids)
    )


def _notice_fields(
    severity: str, code: str, text: str, detail: str | None, hint: str | None
) -> bytes:
    fields = [
        b"S" + cstr(severity),
        b"V" + cstr(severity),
        b"C" + cstr(code),
        b"M" + cstr(text),
    ]
    if detail:
        fields.append(b"D" + cstr(detail))
    if hint:
        fields.append(b"H" + cstr(hint))
    return b"".join(fields) + b"\x00"


def error_response(
    code: str,
    text: str,
    *,
    severity: str = "ERROR",
    detail: str | None = None,
    hint: str | None = None,
) -> bytes:
    return message(b"E", _notice_fields(severity, code, text, detail, hint))


def notice_response(
    code: str, text: str, *, severity: str = "NOTICE", detail: str | None = None
) -> bytes:
    return message(b"N", _notice_fields(severity, code, text, detail, None))
