"""B.1 — Carga histórica: 864.000 eventos directo a HBase, con batch.

Uso:
    python -m t2.carga_historica            # salta si ya está cargada (marca en t2_meta)
    python -m t2.carga_historica --forzar   # recrea las tablas y recarga

Decisiones:
* El histórico NO pasa por Redis: es la historia que ya existe; meterla al
  stream solo agregaría memoria y una copia intermedia sin ningún consumidor
  que la necesite "ahora".
* ``Table.batch(batch_size=N)``: happybase acumula N mutaciones y las manda en
  UN mutateRows por Thrift. Con put fila a fila serían 864.000 viajes de ida y
  vuelta (latencia de red x 864.000); con N=5.000 son ~173.
* Las dos tablas se cargan en pasadas separadas para medir por separado lo que
  cuesta cada una (P4: "lo que cuesta tener la segunda tabla").
"""
from __future__ import annotations

import argparse
import json
import time

from . import claves, config, datos, escritura, hbase_util


def _marca(conn):
    if config.TABLA_META not in hbase_util.tablas(conn):
        return None
    fila = conn.table(config.TABLA_META).row(b"carga_historica")
    return json.loads(fila[b"d:info"]) if fila else None


def crear_tablas(conn, cat: dict[str, list[str]]) -> dict[str, list[str]]:
    estaciones = [e for es in cat.values() for e in es]
    splits = {
        config.TABLA_LECTURAS: claves.splits_estaciones(estaciones),
        config.TABLA_TIPO: claves.splits_tipos(cat.keys()),
    }
    for nombre, sp in splits.items():
        hbase_util.crear_tabla(conn, nombre, sp)
        print(f"  creada {nombre:<14} regiones={len(sp) + 1}  SPLITS={sp}")
    return splits


def cargar(conn, tabla_nombre: str, lecturas, fila_fn) -> float:
    tabla = conn.table(tabla_nombre)
    t = time.perf_counter()
    with tabla.batch(batch_size=config.BATCH_HIST) as b:
        for l in lecturas:
            b.put(*fila_fn(l))
    return time.perf_counter() - t


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--forzar", action="store_true", help="recrear tablas aunque ya estén cargadas")
    args = ap.parse_args()

    conn = hbase_util.conectar()
    marca = _marca(conn)
    if marca and not args.forzar:
        print(f"[carga] histórico ya cargado ({marca['eventos']} eventos, {marca['segundos_total']:.1f} s). "
              "Use --forzar para recargar.")
        return

    t_gen = time.perf_counter()
    hist = datos.historico()
    cat = datos.catalogo(hist)
    t_gen = time.perf_counter() - t_gen
    print(f"[carga] generados {len(hist):,} eventos en {t_gen:.1f} s; catálogo: "
          + ", ".join(f"{t}={len(e)}" for t, e in cat.items()))

    splits = crear_tablas(conn, cat)
    hbase_util.crear_tabla(conn, config.TABLA_META, [], versiones=1)

    seg_lect = cargar(conn, config.TABLA_LECTURAS, hist,
                      lambda l: escritura.fila_lecturas(l.estacion, l.tipo, l.valor_txt, l.ts_ms, l.ts_iso))
    print(f"  {config.TABLA_LECTURAS:<14} {len(hist):,} puts en {seg_lect:6.1f} s = {len(hist) / seg_lect:,.0f} filas/s")
    seg_tipo = cargar(conn, config.TABLA_TIPO, hist,
                      lambda l: escritura.fila_tipo(l.estacion, l.tipo, l.valor_txt, l.ts_ms))
    print(f"  {config.TABLA_TIPO:<14} {len(hist):,} puts en {seg_tipo:6.1f} s = {len(hist) / seg_tipo:,.0f} filas/s")

    info = {
        "eventos": len(hist),
        "batch_size": config.BATCH_HIST,
        "segundos_generacion": round(t_gen, 2),
        "segundos_lecturas": round(seg_lect, 2),
        "segundos_lecturas_tipo": round(seg_tipo, 2),
        "segundos_total": round(seg_lect + seg_tipo, 2),
        "filas_por_s_lecturas": round(len(hist) / seg_lect),
        "filas_por_s_lecturas_tipo": round(len(hist) / seg_tipo),
        "splits": splits,
        "catalogo": cat,
    }
    conn.table(config.TABLA_META).put(b"carga_historica", {b"d:info": json.dumps(info).encode()})
    config.RESULTADOS.mkdir(exist_ok=True)
    (config.RESULTADOS / "carga_historica.json").write_text(json.dumps(info, indent=2, ensure_ascii=False))
    print(f"[carga] total {info['segundos_total']:.1f} s  (marca escrita en {config.TABLA_META})")


if __name__ == "__main__":
    main()
