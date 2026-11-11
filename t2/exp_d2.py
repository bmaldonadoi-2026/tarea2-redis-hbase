"""D.2 — Redis se reinicia con el pipeline a medio camino.

    python -m t2.exp_d2 aof        # la configuración elegida en la Parte A
    python -m t2.exp_d2 sin        # la contraria: sin persistencia
    python -m t2.exp_d2 aof-kill   # extra: AOF everysec + SIGKILL (crash real)
    python -m t2.exp_d2 restaurar  # vuelve Redis a la config del compose

Aislado del pipeline principal: stream ``sensores:t2:d2``, tablero con prefijo
``d2:`` y tablas ``d2_lecturas*``. Flujo:

 1. recrea el contenedor redis con la config pedida (sin editar el compose:
    REDIS_APPENDONLY=yes|no);
 2. arranca tablero + ingesta en segundo plano y el productor a 20x;
 3. con ~3.000 eventos producidos toma una FOTO del estado y hace
    ``docker compose restart redis`` (o ``kill -s SIGKILL`` + ``start``);
 4. FOTO después del reinicio: ¿sobrevivió el stream? ¿los grupos y sus
    pendientes? ¿ultimo:*? ¿las ventanas?
 5. deja terminar todo y cuenta cuántas filas llegaron a HBase vs 7.047.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

from . import claves, config, datos, hbase_util, redis_util
from .consumidor_tablero import ClavesTablero
from .preparar_vivo import preparar

STREAM = "sensores:t2:d2"
PREFIJO = "d2:"
T_LECT, T_TIPO = "d2_lecturas", "d2_lecturas_tipo"
K = ClavesTablero(PREFIJO)


def compose(*args: str, env: dict | None = None) -> None:
    cmd = ["docker", "compose", *args]
    print("  $ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=config.RAIZ, check=True, env={**os.environ, **(env or {})})


def recrear_redis(appendonly: str) -> None:
    compose("up", "-d", "--force-recreate", "--wait", "redis", env={"REDIS_APPENDONLY": appendonly})


def foto(conn) -> dict:
    """Estado observable de Redis (y de HBase) en este instante."""
    r = redis_util.conectar()
    est: dict = {"producidos": int(r.get(config.k_producidos(STREAM)) or 0),
                 "stream_existe": bool(r.exists(STREAM)), "xlen": r.xlen(STREAM)}
    try:
        est["grupos"] = [{k: g.get(k) for k in ("name", "pending", "last-delivered-id", "entries-read", "lag")}
                         for g in r.xinfo_groups(STREAM)]
    except Exception as e:  # noqa: BLE001
        est["grupos"] = f"ERROR: {e}"
    est["ingesta_confirmadas"] = int(r.get(config.k_stats(STREAM, config.GRUPO_INGESTA)) or 0)
    est["ultimo_hashes"] = sum(1 for _ in r.scan_iter(match=f"{PREFIJO}ultimo:*"))
    est["ventanas"] = {k.split(":")[-1]: r.zcard(k) for k in r.scan_iter(match=f"{PREFIJO}ventana:*")}
    est["catalogo_tipos"] = sum(1 for _ in r.scan_iter(match=f"{PREFIJO}tipo:*"))
    # Conexión nueva en cada foto: el Thrift de HBase cierra las conexiones que pasan
    # más de 60 s inactivas (hbase.thrift.server.socket.read.timeout) y entre fotos
    # pasan más de 60 s.
    est["filas_hbase"] = hbase_util.contar_filas(hbase_util.conectar().table(T_LECT))
    # Ventana de pérdida = eventos que están en Redis pero aún NO confirmados en HBase.
    est["en_redis_no_en_hbase"] = est["producidos"] - est["ingesta_confirmadas"]
    return est


def imprimir(titulo: str, f: dict) -> None:
    print(f"\n  --- {titulo} ---")
    for k, v in f.items():
        print(f"    {k:<22} {v}")


def correr(modo: str) -> dict:
    appendonly = "no" if modo == "sin" else "yes"
    print(f"\n== D.2 · modo {modo}: appendonly={appendonly}, appendfsync=everysec, save=''")
    recrear_redis(appendonly)
    conn = hbase_util.conectar()
    cat = datos.catalogo(datos.en_vivo())
    hbase_util.crear_tabla(conn, T_LECT, claves.splits_estaciones([e for es in cat.values() for e in es]))
    hbase_util.crear_tabla(conn, T_TIPO, claves.splits_tipos(cat))
    preparar(STREAM, PREFIJO)

    config.LOGS.mkdir(exist_ok=True)
    py = sys.executable
    procs = {
        "tablero": [py, "-m", "t2.consumidor_tablero", "--stream", STREAM, "--prefijo", PREFIJO],
        "ingesta": [py, "-m", "t2.consumidor_ingesta", "--stream", STREAM,
                    "--tabla-lecturas", T_LECT, "--tabla-tipo", T_TIPO],
        "productor": [py, "-m", "t2.productor", "--stream", STREAM],
    }
    vivos = {n: subprocess.Popen(c, cwd=config.RAIZ, stdout=open(config.LOGS / f"d2_{modo}_{n}.log", "w"),
                                 stderr=subprocess.STDOUT) for n, c in procs.items()}
    r = redis_util.conectar()
    while int(r.get(config.k_producidos(STREAM)) or 0) < 3_000:
        time.sleep(0.1)
    antes = foto(conn)
    imprimir("ANTES del reinicio", antes)

    t = time.perf_counter()
    # El "up" posterior DEBE llevar la misma config: sin ella, compose ve una diferencia
    # con el compose por defecto (AOF), RECREA el contenedor con AOF y éste carga el AOF
    # viejo del volumen, contaminando la foto "después".
    env = {"REDIS_APPENDONLY": appendonly}
    if modo == "aof-kill":
        compose("kill", "-s", "SIGKILL", "redis")
        compose("up", "-d", "--wait", "redis", env=env)
    else:
        compose("restart", "redis")
        compose("up", "-d", "--wait", "redis", env=env)
    caida_s = time.perf_counter() - t
    despues = foto(conn)
    imprimir(f"DESPUÉS del reinicio ({caida_s:.1f} s sin Redis)", despues)

    codigos = {}
    for n in ("productor", "ingesta", "tablero"):
        try:
            codigos[n] = vivos[n].wait(timeout=400)
        except subprocess.TimeoutExpired:
            vivos[n].kill()
            codigos[n] = "timeout"
    final = foto(conn)
    imprimir("AL TERMINAR", final)
    esperado = datos.estadisticas_control(datos.en_vivo())["claves"]
    res = {"modo": modo, "antes": antes, "despues": despues, "final": final, "codigos_salida": codigos,
           "segundos_sin_redis": round(caida_s, 1),
           "filas_esperadas": esperado, "filas_perdidas": esperado - final["filas_hbase"]}
    print(f"\n  filas en HBase al final: {final['filas_hbase']:,} de {esperado:,} "
          f"-> perdidas: {res['filas_perdidas']:,}  (códigos de salida: {codigos})")
    config.RESULTADOS.mkdir(exist_ok=True)
    (config.RESULTADOS / f"d2_{modo}.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=str))
    return res


def main(argv: list[str]) -> None:
    modos = argv or ["aof", "sin"]
    for m in modos:
        if m == "restaurar":
            continue
        if m not in ("aof", "sin", "aof-kill"):
            raise SystemExit(f"modo desconocido: {m}")
        correr(m)
    print("\n== D.2 · restaurando Redis a la configuración del compose (AOF)")
    compose("up", "-d", "--force-recreate", "--wait", "redis")


if __name__ == "__main__":
    main(sys.argv[1:])
