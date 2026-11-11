"""D.1 — El consumidor de ingesta muere a mitad de lote (entre el put y el XACK).

    python -m t2.exp_d1 [--morir-tras-lote 4]

Aislado del pipeline principal (stream ``sensores:t2:d1`` y tablas ``d1_*``),
para poder repetirlo sin tocar ``lecturas``. Pasos:

 1. tablas d1_lecturas / d1_lecturas_tipo recién creadas (VERSIONS=3);
 2. la hora en vivo completa (7.200) al stream d1, sin pausa;
 3. ``ingesta-A`` procesa y hace ``os._exit(1)`` DESPUÉS del put del lote N y
    ANTES de su XACK;
 4. se muestra XPENDING (cuántas, a nombre de quién) y que esas filas YA están
    en HBase;
 5. ``ingesta-B`` arranca, XAUTOCLAIM le transfiere las entradas huérfanas,
    las reprocesa y sigue con el resto;
 6. XPENDING = 0, filas = 7.047 (control "en vivo") en ambas tablas, y las
    versiones de una celda reprocesada.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time

from . import claves, config, datos, hbase_util, productor, redis_util

STREAM = "sensores:t2:d1"
T_LECT, T_TIPO = "d1_lecturas", "d1_lecturas_tipo"


def _consumidor(nombre: str, extra: list[str]) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-m", "t2.consumidor_ingesta", "--stream", STREAM, "--nombre", nombre,
           "--tabla-lecturas", T_LECT, "--tabla-tipo", T_TIPO, *extra]
    print("  $ " + " ".join(cmd[1:]), flush=True)
    p = subprocess.run(cmd, cwd=config.RAIZ, text=True, capture_output=True)
    print("    " + p.stdout.strip().replace("\n", "\n    "))
    return p


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--morir-tras-lote", type=int, default=4)
    a = ap.parse_args()
    out: dict = {}

    print("== D.1 · preparación")
    conn, r = hbase_util.conectar(), redis_util.conectar()
    cat = datos.catalogo(datos.en_vivo())
    hbase_util.crear_tabla(conn, T_LECT, claves.splits_estaciones([e for es in cat.values() for e in es]))
    hbase_util.crear_tabla(conn, T_TIPO, claves.splits_tipos(cat))
    r.delete(STREAM, config.k_fin(STREAM), config.k_producidos(STREAM), config.k_stats(STREAM, config.GRUPO_INGESTA))
    redis_util.crear_grupo(r, STREAM, config.GRUPO_INGESTA)
    productor.producir(STREAM, ritmo=0.0)

    print(f"\n== D.1 · ingesta-A muere tras el put del lote {a.morir_tras_lote}")
    pa = _consumidor("ingesta-A", ["--morir-tras-lote", str(a.morir_tras_lote)])
    out["codigo_salida_A"] = pa.returncode

    pend = r.xpending(STREAM, config.GRUPO_INGESTA)
    detalle = r.xpending_range(STREAM, config.GRUPO_INGESTA, min="-", max="+", count=3)
    out["xpending_tras_muerte"] = {"pending": pend["pending"], "min": pend["min"], "max": pend["max"],
                                   "consumidores": pend["consumers"], "primeras": detalle}
    print(f"\n  XPENDING ingesta: {pend['pending']} entradas  {pend['consumers']}")
    for d in detalle:
        print(f"    {d}")

    # Evidencia de que el put ocurrió: la fila de la primera entrada pendiente ya existe.
    _, campos = r.xrange(STREAM, pend["min"], pend["min"])[0]
    clave = claves.clave_lecturas(campos["estacion_id"], int(campos["ts"]))
    t = conn.table(T_LECT)
    out["fila_pendiente_ya_en_hbase"] = {"clave": clave.decode(), "existe": bool(t.row(clave))}
    out["filas_antes_de_recuperar"] = hbase_util.contar_filas(t)
    print(f"  la fila {clave.decode()} (entrada pendiente, sin XACK) ¿ya está en HBase? "
          f"{out['fila_pendiente_ya_en_hbase']['existe']}")
    print(f"  filas en {T_LECT} antes de recuperar: {out['filas_antes_de_recuperar']:,}")

    print("\n== D.1 · ingesta-B recupera con XAUTOCLAIM (min-idle 1 s) y termina el stream")
    time.sleep(1.5)
    pb = _consumidor("ingesta-B", ["--min-idle-ms", "1000"])
    out["codigo_salida_B"] = pb.returncode

    print("\n== D.1 · verificación final")
    pend = r.xpending(STREAM, config.GRUPO_INGESTA)
    esperado = config.CONTROL["vivo"]["claves"]
    calc = datos.estadisticas_control(datos.en_vivo())["claves"]
    filas = {n: hbase_util.contar_filas(conn.table(n)) for n in (T_LECT, T_TIPO)}
    celdas = t.cells(clave, b"d:valor", versions=config.VERSIONES, include_timestamp=True)
    out.update({
        "xpending_final": pend["pending"],
        "procesadas_total": int(r.get(config.k_stats(STREAM, config.GRUPO_INGESTA)) or 0),
        "filas_finales": filas, "filas_esperadas": calc, "control_enunciado": esperado,
        "versiones_celda_reprocesada": [{"valor": v.decode(), "ts_escritura": ts} for v, ts in celdas],
    })
    print(f"  XPENDING final = {pend['pending']}")
    print(f"  entradas procesadas (con reproceso) = {out['procesadas_total']:,} de {config.N_VIVO:,} producidas")
    for n, f in filas.items():
        print(f"  {n:<17} {f:,} filas  (esperado {calc:,}; enunciado {esperado:,})  "
              f"{'OK' if f == calc else 'NO CALZA'}")
    print(f"  versiones de d:valor en {clave.decode()}: {out['versiones_celda_reprocesada']}")
    print("  -> mismas filas, misma clave: el reproceso agregó una VERSIÓN a la celda, no una fila.")
    config.RESULTADOS.mkdir(exist_ok=True)
    (config.RESULTADOS / "d1.json").write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    if pend["pending"] != 0 or any(f != calc for f in filas.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
