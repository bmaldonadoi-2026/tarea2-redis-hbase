"""B.3 — Verificación de punta a punta.

    python -m t2.verificar [--solo-historico]

Imprime y compara:
  * números de control recalculados desde el generador vs los del enunciado;
  * eventos producidos, entradas procesadas y XPENDING de cada grupo (debe ser 0);
  * filas de cada tabla de HBase (conteo con FirstKeyOnlyFilter AND KeyOnlyFilter);
  * coherencia del tablero (último POR MEDICIÓN, ventanas de 10 min, catálogo).

Si algo no calza lo dice y explica la causa probable. Código de salida 1 si
hay algún error.
"""
from __future__ import annotations

import argparse
import sys
import time

from . import config, datos, hbase_util, redis_util
from .consumidor_tablero import ClavesTablero

errores: list[str] = []


def ok(cond: bool, msg: str, explicacion: str = "", fatal: bool = True) -> None:
    print(f"  [{'OK' if cond else ('XX' if fatal else '!!')}] {msg}")
    if not cond:
        if fatal:
            errores.append(msg)
        if explicacion:
            print(f"       -> {explicacion}")


def seccion(t: str) -> None:
    print(f"\n== {t} " + "=" * (70 - len(t)))


def verificar_control() -> dict:
    seccion("Números de control (recalculados desde el generador)")
    hist, vivo = datos.historico(), datos.en_vivo()
    calc = {"hist": datos.estadisticas_control(hist), "vivo": datos.estadisticas_control(vivo),
            "total": datos.estadisticas_control(hist + vivo)}
    print(f"  {'':8}{'eventos':>10}{'repetidos':>11}{'claves':>10}{'colisiones':>12}{'tardías~':>10}")
    for parte in ("hist", "vivo", "total"):
        c = calc[parte]
        print(f"  {parte:8}{c['eventos']:>10,}{c['repetidos']:>11,}{c['claves']:>10,}{c['colisiones']:>12}{c['tardias_aprox']:>10,}")
    for parte in ("hist", "vivo", "total"):
        for campo in ("eventos", "repetidos", "claves"):
            esperado = config.CONTROL[parte][campo]
            ok(calc[parte][campo] == esperado, f"{parte}.{campo} = {calc[parte][campo]:,} (enunciado {esperado:,})",
               "Revise que t2/generador.py sea el del Lab 06 sin cambios, semilla 2026 y "
               "t0 = 2026-11-02T00:00-03:00.")
    ok(calc["total"]["colisiones"] == config.CONTROL["colisiones_total"],
       f"colisiones (misma estación+ms, distinto valor) = {calc['total']['colisiones']} (enunciado 4)",
       "Con clave estacion#ts HBase conserva el último escrito: tolerado por la pauta.")
    claves_total = len({l.clave() for l in hist + vivo})
    calc["filas_esperadas_total"] = claves_total
    calc["filas_esperadas_hist"] = calc["hist"]["claves"]
    print(f"  871.200 - 17.243 repetidos - 4 colisiones = {871_200 - 17_243 - 4:,} claves (enunciado 853.953)")
    return calc


def verificar_redis(calc: dict) -> None:
    seccion("Redis: stream y consumer groups")
    r = redis_util.conectar()
    s = config.STREAM
    producidos = int(r.get(config.k_producidos(s)) or 0)
    ok(producidos == config.N_VIVO, f"eventos producidos = {producidos:,} (esperado {config.N_VIVO:,})",
       "El productor no terminó o el stream se borró (¿Redis reinició sin persistencia?).")
    try:
        info = r.xinfo_stream(s)
        print(f"  XLEN={r.xlen(s):,}  entries-added={info.get('entries-added')}  "
              f"max-deleted-entry-id={info.get('max-deleted-entry-id')}  (MAXLEN ~ {config.MAXLEN:,})")
        grupos = {g["name"]: g for g in r.xinfo_groups(s)}
    except Exception as e:  # noqa: BLE001
        ok(False, f"el stream {s} existe", str(e))
        return
    for g in (config.GRUPO_INGESTA, config.GRUPO_TABLERO):
        if g not in grupos:
            ok(False, f"grupo {g} existe")
            continue
        info_g = grupos[g]
        pend = r.xpending(s, g)
        procesadas = int(r.get(config.k_stats(s, g)) or 0)
        consumidores = ", ".join(f"{c['name']}:{c['pending']}" for c in pend["consumers"]) or "-"
        print(f"  grupo {g:8} entries-read={info_g.get('entries-read')}  lag={info_g.get('lag')}  "
              f"procesadas={procesadas:,}  XPENDING={pend['pending']}  ({consumidores})")
        ok(pend["pending"] == 0, f"XPENDING {g} = {pend['pending']}",
           "Hay entradas entregadas sin XACK: un consumidor murió; corra otro consumidor del grupo "
           "(XAUTOCLAIM las recupera tras min-idle).")
        ok(procesadas >= producidos, f"{g} procesó {procesadas:,} >= {producidos:,} producidas",
           "Si es MAYOR es por reprocesos tras una caída (at-least-once); si es MENOR faltan entradas.")


def verificar_tablero() -> None:
    seccion("Redis: tablero de 'ahora'")
    r = redis_util.conectar()
    k = ClavesTablero()
    vivo = datos.en_vivo()
    cat = datos.catalogo(vivo)
    # último por medición esperado (empate -> el que llegó después, como HBase)
    esperado: dict[str, datos.Lectura] = {}
    for l in vivo:
        if l.estacion not in esperado or l.ts_ms >= esperado[l.estacion].ts_ms:
            esperado[l.estacion] = l
    malos = []
    for est, l in esperado.items():
        h = r.hgetall(k.ultimo(est))
        if not h or int(h["ts"]) != l.ts_ms or h["valor"] != l.valor_txt:
            malos.append((est, h, l.ts_ms, l.valor_txt))
    ok(not malos, f"ultimo:<est> = último POR MEDICIÓN en las {len(esperado)} estaciones",
       f"discrepan: {malos[:3]}")
    flujo = sorted(e for t, es in cat.items() if datos.es_flujo(t) for e in es)
    rank = sorted(r.zrange(k.ranking, 0, -1))
    ok(rank == flujo, f"ranking:flujo tiene las {len(flujo)} estaciones de flujo")
    ts_max = int(r.get(k.ts_max) or 0)
    ok(ts_max == max(l.ts_ms for l in vivo), f"tablero:ts_max = {ts_max} (máx. medición del stream)")
    fuera = [e for e in flujo if r.zcount(k.ventana(e), "-inf", ts_max - config.VENTANA_MS) > 0]
    ok(not fuera, "ventana:<est> recortadas a (ts_max - 10 min, ts_max]", f"sin recortar: {fuera}")
    tamanos = {e: r.zcard(k.ventana(e)) for e in flujo}
    print(f"  tamaños de ventana: {tamanos}")
    for tipo, ests in cat.items():
        ok(sorted(r.smembers(k.tipo(tipo))) == ests, f"tipo:{tipo} = {ests}")


def verificar_hbase(calc: dict, solo_hist: bool) -> None:
    seccion("HBase: filas por tabla")
    conn = hbase_util.conectar()
    esperado = calc["filas_esperadas_hist"] if solo_hist else calc["filas_esperadas_total"]
    oficial = config.CONTROL["hist"]["claves"] if solo_hist else config.CONTROL["total"]["claves"]
    for nombre in (config.TABLA_LECTURAS, config.TABLA_TIPO):
        t = time.perf_counter()
        n = hbase_util.contar_filas(conn.table(nombre))
        dur = time.perf_counter() - t
        print(f"  {nombre:<14} {n:>9,} filas  (conteo {dur:.1f} s)  esperado {esperado:,} / enunciado {oficial:,}")
        if n < esperado:
            exp = "FALTAN filas: un lote no llegó a HBase (¿XACK antes del put? ¿entradas recortadas por MAXLEN?)."
        elif n > esperado:
            exp = "SOBRAN filas: la clave no es idempotente (¿usa la llegada o el ID del stream?)."
        else:
            exp = ""
        ok(n == esperado, f"{nombre}: {n:,} == {esperado:,}", exp)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--solo-historico", action="store_true", help="verificar solo tras la carga histórica")
    a = ap.parse_args()
    calc = verificar_control()
    if not a.solo_historico:
        verificar_redis(calc)
        verificar_tablero()
    verificar_hbase(calc, a.solo_historico)
    seccion("Resumen")
    if errores:
        print(f"  {len(errores)} verificaciones fallaron:")
        for e in errores:
            print(f"   - {e}")
        sys.exit(1)
    print("  Todo calza.")


if __name__ == "__main__":
    main()
