"""Espera a que Redis y HBase acepten trabajo de verdad (no solo que el puerto abra).

El healthcheck de compose ya garantiza Master activo + Thrift escuchando; aquí
además se crea y borra una tabla de prueba, porque justo después del arranque
el Master puede responder PleaseHoldException mientras asigna hbase:meta.
"""
from __future__ import annotations

import time

from . import hbase_util, redis_util


def main(timeout_s: int = 300) -> None:
    redis_util.conectar().ping()
    print("[esperar] Redis OK")
    conn = hbase_util.conectar()
    limite = time.time() + timeout_s
    while True:
        try:
            if "t2_sonda" in hbase_util.tablas(conn):
                conn.delete_table("t2_sonda", disable=True)
            conn.create_table("t2_sonda", {"d": {}})
            conn.delete_table("t2_sonda", disable=True)
            break
        except Exception as e:  # noqa: BLE001
            if time.time() > limite:
                raise RuntimeError(f"HBase no acepta DDL tras {timeout_s} s: {e}") from e
            time.sleep(3)
            conn = hbase_util.conectar()
    print("[esperar] HBase OK (DDL por Thrift funciona)")


if __name__ == "__main__":
    main()
