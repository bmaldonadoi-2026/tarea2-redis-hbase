"""B.2 — Productor: la hora en vivo al stream ``sensores:t2`` con XADD, a 20x.

    python -m t2.productor [--stream S] [--ritmo 0.025] [--limite N] [--sin-pausa]

* XADD sensores:t2 MAXLEN ~ 10000 * estacion_id .. tipo .. valor .. ts .. ts_iso .. seq
  (el ID lo asigna Redis: es el reloj de LLEGADA; ``ts`` es el reloj de MEDICIÓN).
* El ritmo se agenda contra el reloj absoluto (t_inicio + i * 25 ms), no con
  sleep(0.025) acumulado: así un XADD lento no corre toda la hora hacia atrás.
* '~' (recorte aproximado): Redis solo recorta nodos enteros del radix tree, lo
  que hace el recorte O(1) amortizado; la longitud real puede superar MAXLEN
  por a lo más un nodo (~100 entradas), nunca quedar por debajo.
* Al terminar marca ``<stream>:fin``, que los consumidores usan para saber que
  no vendrá nada más.
"""
from __future__ import annotations

import argparse
import time

import redis

from . import config, datos, redis_util


def producir(stream: str = config.STREAM, ritmo: float = config.RITMO_VIVO_S,
             limite: int | None = None, maxlen: int = config.MAXLEN) -> int:
    r = redis_util.conectar()
    eventos = datos.en_vivo()[:limite] if limite else datos.en_vivo()
    r.delete(config.k_fin(stream))
    t0 = time.perf_counter()
    atraso_max = 0.0
    for i, l in enumerate(eventos):
        if ritmo > 0:
            objetivo = t0 + i * ritmo
            espera = objetivo - time.perf_counter()
            if espera > 0:
                time.sleep(espera)
            else:
                atraso_max = max(atraso_max, -espera)
        campos = {"estacion_id": l.estacion, "tipo": l.tipo, "valor": l.valor_txt,
                  "ts": l.ts_ms, "ts_iso": l.ts_iso, "seq": l.seq}
        # MULTI/EXEC: el XADD y el contador quedan juntos o no queda ninguno.
        # Si Redis se cae y la respuesta se pierde, reintentamos: en el peor caso
        # el evento entra DOS veces al stream (at-least-once). No importa: la
        # row key idempotente lo deja en la misma fila de HBase.
        while True:
            try:
                p = r.pipeline(transaction=True)
                p.xadd(stream, campos, maxlen=maxlen, approximate=True)
                p.incr(config.k_producidos(stream))
                p.execute()
                break
            except (redis.ConnectionError, redis.BusyLoadingError, redis.TimeoutError, OSError):
                time.sleep(0.5)
        if (i + 1) % 1000 == 0:
            print(f"[productor] {i + 1:>5}/{len(eventos)}  XLEN={r.xlen(stream)}", flush=True)
    r.set(config.k_fin(stream), len(eventos))
    dur = time.perf_counter() - t0
    print(f"[productor] {len(eventos)} eventos en {dur:.1f} s ({len(eventos) / dur:.1f} ev/s; "
          f"atraso máx. respecto al reloj 20x: {atraso_max * 1000:.0f} ms)")
    return len(eventos)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stream", default=config.STREAM)
    ap.add_argument("--ritmo", type=float, default=config.RITMO_VIVO_S)
    ap.add_argument("--limite", type=int, default=None)
    ap.add_argument("--sin-pausa", action="store_true", help="escribir tan rápido como se pueda")
    a = ap.parse_args()
    producir(a.stream, 0.0 if a.sin_pausa else a.ritmo, a.limite)


if __name__ == "__main__":
    main()
