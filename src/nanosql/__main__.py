"""Punto de entrada `python -m nanosql` (equivalente al script `nanosql`)."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
