"""Autenticación del servidor: `trust` y SCRAM-SHA-256 (RFC 5802 / 7677).

El servidor nunca guarda contraseñas en claro: al arrancar convierte cada
contraseña en un verificador SCRAM (sal, iteraciones, StoredKey, ServerKey),
exactamente lo que PostgreSQL guarda en `pg_authid.rolpassword`. El
intercambio sigue el perfil de PostgreSQL: el nombre de usuario de SCRAM se
ignora (manda el del paquete de arranque) y, sin TLS, no se ofrece channel
binding (`SCRAM-SHA-256-PLUS`), así que se aceptan las cabeceras GS2 `n,,` e
`y,,` y se rechaza `p=`.

Normalización de la contraseña: se aplica NFKC (el núcleo de SASLprep); el
resto de SASLprep (prohibición de caracteres de control) no se implementa,
documentado en el README — irrelevante para contraseñas ASCII.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import unicodedata
from dataclasses import dataclass

MECHANISM = "SCRAM-SHA-256"
DEFAULT_ITERATIONS = 4096


class AuthenticationFailedError(Exception):
    """El cliente no demostró conocer la contraseña (o rompió el protocolo)."""


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _hmac(key: bytes, message: bytes) -> bytes:
    return hmac.new(key, message, hashlib.sha256).digest()


@dataclass(frozen=True, slots=True)
class ScramVerifier:
    salt: bytes
    iterations: int
    stored_key: bytes
    server_key: bytes

    @classmethod
    def from_password(
        cls, password: str, *, salt: bytes | None = None, iterations: int = DEFAULT_ITERATIONS
    ) -> ScramVerifier:
        salt = salt if salt is not None else secrets.token_bytes(16)
        normalized = unicodedata.normalize("NFKC", password).encode("utf-8")
        salted = hashlib.pbkdf2_hmac("sha256", normalized, salt, iterations)
        client_key = _hmac(salted, b"Client Key")
        return cls(
            salt=salt,
            iterations=iterations,
            stored_key=hashlib.sha256(client_key).digest(),
            server_key=_hmac(salted, b"Server Key"),
        )

    def to_string(self) -> str:
        """Formato de `pg_authid`: SCRAM-SHA-256$iter:sal$StoredKey:ServerKey."""
        return (
            f"{MECHANISM}${self.iterations}:{_b64(self.salt)}"
            f"${_b64(self.stored_key)}:{_b64(self.server_key)}"
        )


def _parse_attributes(message: str) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for part in message.split(","):
        if len(part) < 2 or part[1] != "=":
            raise AuthenticationFailedError(f"atributo SCRAM malformado: {part!r}")
        attributes[part[0]] = part[2:]
    return attributes


class ScramServerExchange:
    """Máquina de estados de un intercambio SCRAM del lado servidor.

    Uso: `first = exchange.handle_client_first(msg)` → enviar como
    SASLContinue; `final = exchange.handle_client_final(msg)` → enviar como
    SASLFinal (lanza `AuthenticationFailedError` si la prueba no cuadra).
    """

    def __init__(self, verifier: ScramVerifier, *, server_nonce: str | None = None) -> None:
        self._verifier = verifier
        # Inyectable sólo para reproducir los vectores de prueba del RFC.
        self._server_nonce = server_nonce
        self._client_first_bare = ""
        self._server_first = ""
        self._nonce = ""
        self._gs2_header = ""

    def handle_client_first(self, data: bytes) -> bytes:
        text = data.decode("utf-8")
        # gs2-header: cbind-flag "," [authzid] ","
        parts = text.split(",", 2)
        if len(parts) != 3:
            raise AuthenticationFailedError("client-first-message malformado")
        cbind_flag, authzid, bare = parts
        if cbind_flag.startswith("p="):
            raise AuthenticationFailedError("channel binding no soportado (no hay TLS)")
        if cbind_flag not in ("n", "y"):
            raise AuthenticationFailedError(f"flag de channel binding inválido: {cbind_flag!r}")
        if authzid:
            raise AuthenticationFailedError("authzid no soportado")
        self._gs2_header = f"{cbind_flag},{authzid},"
        attributes = _parse_attributes(bare)
        client_nonce = attributes.get("r")
        if not client_nonce:
            raise AuthenticationFailedError("client-first-message sin nonce")
        self._client_first_bare = bare
        server_nonce = self._server_nonce or _b64(secrets.token_bytes(18))
        self._nonce = client_nonce + server_nonce
        self._server_first = (
            f"r={self._nonce},s={_b64(self._verifier.salt)},i={self._verifier.iterations}"
        )
        return self._server_first.encode("ascii")

    def handle_client_final(self, data: bytes) -> bytes:
        text = data.decode("utf-8")
        proof_marker = text.rfind(",p=")
        if proof_marker < 0:
            raise AuthenticationFailedError("client-final-message sin prueba")
        without_proof = text[:proof_marker]
        attributes = _parse_attributes(text)
        if attributes.get("c") != _b64(self._gs2_header.encode("ascii")):
            raise AuthenticationFailedError("cabecera de channel binding inconsistente")
        if attributes.get("r") != self._nonce:
            raise AuthenticationFailedError("nonce inconsistente")
        try:
            proof = base64.b64decode(attributes["p"], validate=True)
        except (KeyError, ValueError) as exc:
            raise AuthenticationFailedError("prueba SCRAM ilegible") from exc
        auth_message = ",".join((self._client_first_bare, self._server_first, without_proof))
        client_signature = _hmac(self._verifier.stored_key, auth_message.encode("utf-8"))
        if len(proof) != len(client_signature):
            raise AuthenticationFailedError("prueba SCRAM de longitud inválida")
        client_key = bytes(a ^ b for a, b in zip(proof, client_signature, strict=True))
        if not hmac.compare_digest(hashlib.sha256(client_key).digest(), self._verifier.stored_key):
            raise AuthenticationFailedError("contraseña incorrecta")
        server_signature = _hmac(self._verifier.server_key, auth_message.encode("utf-8"))
        return f"v={_b64(server_signature)}".encode("ascii")
