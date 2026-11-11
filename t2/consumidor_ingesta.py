"""B.2 — Grupo ``ingesta``: del stream a HBase (lecturas + lecturas_tipo).

    python -m t2.consumidor_ingesta [--nombre ingesta-1] [--morir-tras-lote N]
                                    [--tabla-lecturas T] [--tabla-tipo T] [--stream S]

Por cada lote de XREADGROUP:
  1. batch a ``lecturas``       (un mutateRows por Thrift)
  2. batch a ``lecturas_tipo``  (otro mutateRows)
  3. recién entonces XACK.

Si 2 falla después de que 1 se escribió, no hay XACK: el lote entero se
reintenta y 1 se reescribe sobre las MISMAS filas (clave idempotente). Las dos
tablas quedan consistentes "eventualmente", sin necesidad de transacciones
entre tablas (que HBase no tiene).
"""
from __future__ import annotations

import argparse
import time

from . import config, escritura, hbase_util
from .consumidor_base import Consumidor, Entrada


class Ingesta:
    def __init__(self, tabla_lecturas: str, tabla_tipo: str) -> None:
        self.nombres = (tabla_lecturas, tabla_tipo)
        self._conectar()

    def _conectar(self) -> None:
        self.conn = hbase_util.conectar()
        self.t_lect = self.conn.table(self.nombres[0])
        self.t_tipo = self.conn.table(self.nombres[1])

    def __call__(self, entradas: list[Entrada]) -> None:
        for intento in range(5):
            try:
                self._escribir(entradas)
                return
            except Exception as e:  # noqa: BLE001 — Thrift lanza tipos variados
                if intento == 4:
                    raise           # sin XACK: el lote queda pendiente para otro consumidor
                print(f"[ingesta] fallo escribiendo en HBase ({type(e).__name__}: {e}); reconecto", flush=True)
                time.sleep(1 + intento)
                self._conectar()

    def _escribir(self, entradas: list[Entrada]) -> None:
        # transaction=True: si algo falla ARMANDO el lote, no se envía a medias.
        with self.t_lect.batch(transaction=True) as b:
            for _, c in entradas:
                b.put(*escritura.fila_lecturas(c["estacion_id"], c["tipo"], c["valor"], int(c["ts"]), c["ts_iso"]))
        with self.t_tipo.batch(transaction=True) as b:
            for _, c in entradas:
                b.put(*escritura.fila_tipo(c["estacion_id"], c["tipo"], c["valor"], int(c["ts"])))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stream", default=config.STREAM)
    ap.add_argument("--nombre", default="ingesta-1")
    ap.add_argument("--lote", type=int, default=config.LOTE)
    ap.add_argument("--tabla-lecturas", default=config.TABLA_LECTURAS)
    ap.add_argument("--tabla-tipo", default=config.TABLA_TIPO)
    ap.add_argument("--min-idle-ms", type=int, default=config.MIN_IDLE_MS)
    ap.add_argument("--espera-lote-ms", type=int, default=250,
                    help="micro-batching: espera para juntar más entradas por lote (0 = sin espera)")
    ap.add_argument("--morir-tras-lote", type=int, default=None,
                    help="D.1: os._exit(1) después del put del lote N y antes del XACK")
    a = ap.parse_args()
    Consumidor(config.GRUPO_INGESTA, a.nombre, Ingesta(a.tabla_lecturas, a.tabla_tipo),
               stream=a.stream, lote=a.lote, min_idle_ms=a.min_idle_ms,
               morir_tras_lote=a.morir_tras_lote, espera_lote_ms=a.espera_lote_ms).ejecutar()


if __name__ == "__main__":
    main()
