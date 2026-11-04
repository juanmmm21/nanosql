"""Proxy TCP en espacio de usuario para inyectar particiones de red.

El arnés de caos da a cada nodo, como dirección de cada peer, la de un
proxy propio de ese enlace dirigido (i → j), que reenvía al puerto Raft
real de j. Bloquear un enlace corta sus conexiones vivas y rechaza las
nuevas (se aceptan y se cierran al instante): para Raft es indistinguible
de una red caída en esa dirección. Es lo que permite particiones
asimétricas o por grupos sin `iptables` ni privilegios.
"""

from __future__ import annotations

import socket
import threading


class LinkProxy:
    def __init__(self, listen: tuple[str, int], target: tuple[str, int]) -> None:
        self.target = target
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(listen)
        self._server.listen(64)
        self._blocked = False
        self._lock = threading.Lock()
        self._active: set[socket.socket] = set()
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.getsockname()[:2]
        return str(host), int(port)

    def start(self) -> LinkProxy:
        self._thread.start()
        return self

    def set_blocked(self, blocked: bool) -> None:
        with self._lock:
            self._blocked = blocked
            if blocked:
                for sock in list(self._active):
                    _close(sock)
                self._active.clear()

    @property
    def blocked(self) -> bool:
        return self._blocked

    def stop(self) -> None:
        self._stopped.set()
        _close(self._server)
        self.set_blocked(True)

    def _accept_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                client, _ = self._server.accept()
            except OSError:
                return
            with self._lock:
                blocked = self._blocked
            if blocked:
                _close(client)
                continue
            try:
                upstream = socket.create_connection(self.target, timeout=0.5)
            except OSError:
                _close(client)
                continue
            upstream.settimeout(None)
            with self._lock:
                if self._blocked:
                    _close(client)
                    _close(upstream)
                    continue
                self._active.update({client, upstream})
            for source, sink in ((client, upstream), (upstream, client)):
                threading.Thread(target=self._pump, args=(source, sink), daemon=True).start()

    def _pump(self, source: socket.socket, sink: socket.socket) -> None:
        try:
            while True:
                chunk = source.recv(1 << 16)
                if not chunk:
                    break
                sink.sendall(chunk)
        except OSError:
            pass
        finally:
            with self._lock:
                self._active.discard(source)
                self._active.discard(sink)
            _close(source)
            _close(sink)


def _close(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass
