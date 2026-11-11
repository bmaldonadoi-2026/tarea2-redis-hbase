"""Diseño de row keys y de cortes de región (SPLITS).

Principios (detalle en README §Row keys):

* Las claves se comparan como BYTES. El tiempo va como epoch en milisegundos
  rellenado a 13 dígitos: ancho fijo => orden lexicográfico == orden numérico,
  sin zonas horarias de por medio (no se mezcla UTC con hora de Chile) y en la
  misma unidad que el score de los sorted sets de Redis.
* La clave se arma con la ESTACIÓN y el TIMESTAMP DE MEDICIÓN, nunca con el
  momento de llegada ni con el ID del stream. Por eso es idempotente: el mismo
  evento escrito dos veces (repetido en el origen, reentregado por Redis tras
  una caída, o reprocesado) cae en la MISMA fila; una lectura tardía cae en su
  lugar cronológico y no al final.
"""
from __future__ import annotations

import zlib
from datetime import datetime, timedelta
from typing import Iterable

from . import config

ANCHO_TS = 13


def ms(dt: datetime) -> int:
    """datetime con zona -> epoch en milisegundos."""
    if dt.tzinfo is None:
        raise ValueError("datetime sin zona horaria: ambiguo")
    return int(round(dt.timestamp() * 1000))


def ms_chile(texto: str) -> int:
    """'2026-11-04T08:00' (hora de Chile, -03:00) -> epoch ms."""
    dt = datetime.fromisoformat(texto)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=config.TZ_CHILE)
    return ms(dt)


def ts13(ts_ms: int) -> str:
    return f"{ts_ms:0{ANCHO_TS}d}"


# ------------------------------------------------------------------ tablas
def clave_lecturas(estacion: str, ts_ms: int) -> bytes:
    """lecturas:  <estacion>#<ts13>     (rápida: una estación en un rango de tiempo)."""
    return f"{estacion}#{ts13(ts_ms)}".encode()


def clave_tipo(tipo: str, ts_ms: int, estacion: str) -> bytes:
    """lecturas_tipo:  <tipo>#<ts13>#<estacion>   (rápida: un tipo en un rango de tiempo).

    La estación va al final solo para desempatar: dos estaciones del mismo tipo
    pueden medir en el mismo milisegundo.
    """
    return f"{tipo}#{ts13(ts_ms)}#{estacion}".encode()


def clave_ts_estacion(estacion: str, ts_ms: int) -> bytes:
    """P5: <ts13>#<estacion> (todas las escrituras en vivo van al final => hotspot)."""
    return f"{ts13(ts_ms)}#{estacion}".encode()


def salt(estacion: str, n: int = config.N_SALT) -> int:
    """hash(estacion) mod n con CRC32.

    NO se usa hash() de Python: está aleatorizado por proceso (PYTHONHASHSEED),
    así que la misma estación caería en cubetas distintas en cada corrida y la
    clave dejaría de ser idempotente.
    """
    return zlib.crc32(estacion.encode()) % n


def clave_salt(estacion: str, ts_ms: int, n: int = config.N_SALT) -> bytes:
    """P5: <salt>#<ts13>#<estacion>."""
    return f"{salt(estacion, n)}#{ts13(ts_ms)}#{estacion}".encode()


def rango_estacion(estacion: str, desde_ms: int, hasta_ms: int) -> tuple[bytes, bytes]:
    """[desde, hasta) en la tabla lecturas (row_stop es exclusivo en HBase)."""
    return clave_lecturas(estacion, desde_ms), clave_lecturas(estacion, hasta_ms)


def rango_tipo(tipo: str, desde_ms: int, hasta_ms: int) -> tuple[bytes, bytes]:
    """[desde, hasta) en lecturas_tipo. '<tipo>#<hasta13>' es menor que cualquier
    '<tipo>#<hasta13>#<est>', así que el borde superior queda excluido."""
    return f"{tipo}#{ts13(desde_ms)}".encode(), f"{tipo}#{ts13(hasta_ms)}".encode()


# ------------------------------------------------------------------ SPLITS
def splits_estaciones(estaciones: Iterable[str], n_regiones: int = config.N_REGIONES_ESTACION) -> list[str]:
    """Cortes en BORDES DE ESTACIÓN: 16 estaciones -> 4 regiones de 4 estaciones.

    * Cada estación queda entera dentro de una región: un scan por rango de una
      estación (P3, P4a) toca exactamente una región.
    * Las 16 estaciones emiten a ritmo parecido, así que cada región recibe
      ~1/4 de las escrituras, también en vivo (las 16 escriben a la vez).
    * 4 y no 16: es un único RegionServer standalone con ~100 MB de datos;
      regiones de pocos MB solo agregan memstores y flushes sin ganar paralelismo.
    """
    est = sorted(set(estaciones))
    paso = len(est) / n_regiones
    return [est[round(i * paso)] for i in range(1, n_regiones)]


def splits_tipos(tipos: Iterable[str]) -> list[str]:
    """Un corte por cada tipo salvo el primero: una región por tipo de sensor."""
    t = sorted(set(tipos))
    return [f"{x}#" for x in t[1:]]


def splits_dias(desde: datetime, dias: int) -> list[str]:
    """Cortes en cada medianoche (-03:00), para la tabla ts#estacion de P5."""
    return [ts13(ms(desde + timedelta(days=i))) for i in range(1, dias + 1)]


def splits_salt(n: int = config.N_SALT) -> list[str]:
    return [str(i) for i in range(1, n)]
