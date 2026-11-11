"""Conexión a Redis y utilidades del stream."""
from __future__ import annotations

import time

import redis

from . import config


def conectar(intentos: int = 60) -> redis.Redis:
    """Cliente con decode_responses=True y reintentos de arranque."""
    r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True,
                    socket_keepalive=True, health_check_interval=30)
    for _ in range(intentos):
        try:
            r.ping()
            return r
        except (redis.ConnectionError, redis.BusyLoadingError):
            time.sleep(1)
    raise RuntimeError(f"Redis no responde en {config.REDIS_HOST}:{config.REDIS_PORT}")


def crear_grupo(r: redis.Redis, stream: str, grupo: str, desde: str = "0") -> None:
    """XGROUP CREATE <stream> <grupo> 0 MKSTREAM (idempotente).

    Se crea en '0' y ANTES de producir: así ningún evento queda fuera del grupo
    aunque un consumidor arranque tarde. Con '$' se perderían los eventos
    escritos entre el XADD y la creación del grupo.
    """
    try:
        r.xgroup_create(stream, grupo, id=desde, mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def borrar_patron(r: redis.Redis, patron: str) -> int:
    n = 0
    for k in r.scan_iter(match=patron, count=1000):
        n += r.delete(k)
    return n


def pendientes(r: redis.Redis, stream: str, grupo: str) -> dict:
    """XPENDING resumido: {'pending', 'min', 'max', 'consumers': [{'name','pending'}]}."""
    try:
        return r.xpending(stream, grupo)
    except redis.ResponseError as e:          # NOGROUP / stream inexistente
        return {"pending": None, "error": str(e), "consumers": []}
