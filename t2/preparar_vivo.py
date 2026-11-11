"""Deja Redis listo para la hora en vivo: stream vacío y los DOS grupos creados en '0'.

    python -m t2.preparar_vivo [--stream sensores:t2] [--prefijo ""]

Se borra el estado del tablero para que la verificación cuente solo esta corrida.
"""
from __future__ import annotations

import argparse

from . import config, redis_util
from .consumidor_tablero import claves_tablero


def preparar(stream: str, prefijo: str = "") -> None:
    r = redis_util.conectar()
    r.delete(stream, config.k_fin(stream), config.k_producidos(stream))
    redis_util.borrar_patron(r, f"{stream}:stats:*")
    for patron in claves_tablero(prefijo).patrones():
        redis_util.borrar_patron(r, patron)
    for grupo in (config.GRUPO_INGESTA, config.GRUPO_TABLERO):
        redis_util.crear_grupo(r, stream, grupo, desde="0")
    print(f"[preparar] {stream}: grupos {config.GRUPO_INGESTA!r} y {config.GRUPO_TABLERO!r} creados en 0")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default=config.STREAM)
    ap.add_argument("--prefijo", default="")
    a = ap.parse_args()
    preparar(a.stream, a.prefijo)


if __name__ == "__main__":
    main()
