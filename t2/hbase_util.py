"""Acceso a HBase: conexión Thrift, creación de tablas pre-divididas, conteo, JMX.

Thrift 1 (happybase) no permite fijar SPLITS al crear una tabla, así que la
creación se hace con el ``hbase shell`` del contenedor (igual que en el Lab 07)
y todo lo demás (escrituras en batch, scans, conteos) por Thrift.
"""
from __future__ import annotations

import json
import re
import shlex
import subprocess
import time
import urllib.request
from typing import Iterable, Iterator

import happybase

from . import config


# ------------------------------------------------------------------ conexión
def conectar(intentos: int = 60, espera_s: float = 2.0) -> happybase.Connection:
    """Conexión Thrift con reintentos (HBase tarda en levantar el Master)."""
    ultimo_error: Exception | None = None
    for _ in range(intentos):
        try:
            c = happybase.Connection(config.HBASE_HOST, config.HBASE_PORT, timeout=None, autoconnect=True)
            c.tables()
            return c
        except Exception as e:  # noqa: BLE001 — Thrift lanza tipos variados
            ultimo_error = e
            time.sleep(espera_s)
    raise RuntimeError(f"No pude conectar a HBase Thrift {config.HBASE_HOST}:{config.HBASE_PORT}: {ultimo_error}")


def tablas(conn: happybase.Connection) -> set[str]:
    return {t.decode() if isinstance(t, bytes) else t for t in conn.tables()}


# ------------------------------------------------------------------ shell
def shell(comandos: str, timeout: int = 600) -> str:
    """Ejecuta comandos en ``hbase shell -n`` dentro del contenedor.

    -n (no interactivo) hace que el shell termine con código != 0 ante el
    primer error, así que un CREATE fallido no pasa desapercibido.
    """
    proc = subprocess.run(
        shlex.split(config.HBASE_SHELL),
        input=comandos.rstrip() + "\nexit\n",
        text=True, capture_output=True, cwd=config.RAIZ, timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"hbase shell falló ({proc.returncode}):\n{comandos}\n{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}")
    return proc.stdout


def _ruby_lista(xs: Iterable[str]) -> str:
    return "[" + ", ".join("'" + x.replace("'", "\\'") + "'" for x in xs) + "]"


def crear_tabla(conn: happybase.Connection, nombre: str, splits: list[str],
                versiones: int = config.VERSIONES, borrar_si_existe: bool = True) -> None:
    """CREATE con SPLITS, CF 'd', VERSIONS=versiones y bloom por fila.

    BLOOMFILTER=>'ROW': los gets/seek por clave exacta descartan HFiles sin abrirlos.
    """
    if nombre in tablas(conn):
        if not borrar_si_existe:
            return
        conn.delete_table(nombre, disable=True)
    split_txt = f", SPLITS => {_ruby_lista(splits)}" if splits else ""
    shell(f"create '{nombre}', {{NAME => '{config.CF}', VERSIONS => {versiones}, "
          f"BLOOMFILTER => 'ROW'}}{split_txt}")


def flush(nombre: str) -> None:
    shell(f"flush '{nombre}'")


def tamano_en_disco(nombre: str) -> int:
    """Bytes en disco de la tabla (tras FLUSH, para que los memstores bajen a HFiles)."""
    flush(nombre)
    cmd = shlex.split(config.HBASE_EXEC) + ["du", "-sb", f"/data/hbase/data/default/{nombre}"]
    out = subprocess.run(cmd, text=True, capture_output=True, cwd=config.RAIZ, check=True).stdout
    return int(out.split()[0])


# ------------------------------------------------------------------ lectura
FILTRO_CONTEO = "FirstKeyOnlyFilter() AND KeyOnlyFilter()"


def contar_filas(tabla: happybase.Table, row_start: bytes | None = None,
                 row_stop: bytes | None = None, batch_size: int = 10_000) -> int:
    """Cuenta filas trayendo SOLO la clave de la primera celda de cada fila.

    FirstKeyOnlyFilter: el RegionServer corta cada fila tras la primera celda.
    KeyOnlyFilter: de esa celda no viaja el valor. Resultado: por la red pasa
    ~una clave por fila en vez de todas las celdas con sus valores.
    """
    n = 0
    for _ in tabla.scan(row_start=row_start, row_stop=row_stop,
                        filter=FILTRO_CONTEO, batch_size=batch_size):
        n += 1
    return n


_ORDEN_REVERSO: dict[str, bool] = {}


def scan_reverso(tabla: happybase.Table, alto: bytes, bajo: bytes, **kw) -> Iterator[tuple[bytes, dict]]:
    """Scan descendente desde ``alto`` (incluido) hasta ``bajo`` (excluido).

    HBase espera, para un scan reversed, startRow = la clave MAYOR. Algunas
    versiones de happybase la pasan tal cual y otras no; se prueba una vez y
    se recuerda el orden que funciona.
    """
    clave = "orden"
    ordenes = [_ORDEN_REVERSO[clave]] if clave in _ORDEN_REVERSO else [True, False]
    for alto_primero in ordenes:
        ini, fin = (alto, bajo) if alto_primero else (bajo, alto)
        it = tabla.scan(row_start=ini, row_stop=fin, reverse=True, **kw)
        try:
            primero = next(it)
        except StopIteration:
            continue
        if bajo <= primero[0] <= alto:
            _ORDEN_REVERSO[clave] = alto_primero
            yield primero
            yield from it
            return
    return


def ultima_fila(tabla: happybase.Table, estacion: str, **kw) -> tuple[bytes, dict] | None:
    """Última fila (por MEDICIÓN) de una estación en una tabla estacion#ts13.

    1.º intento: scan reverso con limit=1 -> un seek, sin importar cuánta historia haya.
    Si esta versión de happybase no acepta ``reverse``, se cae a ventanas
    crecientes hacia atrás desde la última medición posible (fin de la hora en
    vivo): 5 min, 10, 20... hasta encontrar algo. Misma respuesta, más lecturas.
    (El Lab 07 resolvía "lo último" con otra tabla de clave invertida
    10**13 - ts; aquí se evita duplicar la tabla.)
    """
    from . import claves
    try:
        return next(scan_reverso(tabla, f"{estacion}#~".encode(), f"{estacion}#".encode(), limit=1, **kw), None)
    except TypeError:
        pass
    fin = claves.ms(config.T0) + (config.N_HIST + config.N_VIVO) * config.INTERVALO_LLEGADA_MS
    ventana = 5 * 60_000
    while ventana < 400 * 86_400_000:
        filas = list(tabla.scan(row_start=claves.clave_lecturas(estacion, fin - ventana),
                                row_stop=f"{estacion}#~".encode(), **kw))
        if filas:
            return filas[-1]
        ventana *= 2
    return None


# ------------------------------------------------------------------ JMX
def metricas_regiones(tabla: str) -> dict[str, dict[str, float]]:
    """Métricas por región desde /jmx (bean RegionServer,sub=Regions).

    Devuelve {encoded_region_name: {"writeRequestCount": ..., "putCount": ..., ...}}.
    HBase publica estas métricas cada ~10 s: esperar antes de leerlas.
    """
    url = f"{config.HBASE_JMX_URL}?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions"
    with urllib.request.urlopen(url, timeout=10) as r:
        beans = json.load(r)["beans"]
    patron = re.compile(rf"^Namespace_default_table_{re.escape(tabla)}_region_([0-9a-f]+)_metric_(\w+)$")
    out: dict[str, dict[str, float]] = {}
    for bean in beans:
        for k, v in bean.items():
            m = patron.match(k)
            if m and isinstance(v, (int, float)):
                out.setdefault(m.group(1), {})[m.group(2)] = v
    return out


def regiones(tabla: happybase.Table) -> list[dict]:
    """[{start_key, end_key, name, id, ...}] vía Thrift getTableRegions."""
    return sorted(tabla.regions(), key=lambda r: r["start_key"])


def encoded_name(nombre_region: bytes) -> str:
    """'tabla,start,ts.ENCODED.' -> 'ENCODED' (lo que usa JMX)."""
    txt = nombre_region.decode(errors="replace")
    partes = txt.rstrip(".").split(".")
    return partes[-1]
