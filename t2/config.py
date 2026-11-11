"""Parámetros únicos del proyecto.

Todo lo que otro módulo necesita saber (hosts, nombres de claves y tablas,
números de control, tamaños de lote) vive aquí, para que el repositorio corra
en una máquina limpia sin editar rutas. Lo que depende del entorno se puede
sobrescribir con variables de entorno, pero los valores por defecto son los
que usa ``run.sh``.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --------------------------------------------------------------------------
# Rutas
# --------------------------------------------------------------------------
RAIZ = Path(__file__).resolve().parent.parent
RESULTADOS = RAIZ / "resultados"
LOGS = RAIZ / "logs"

# --------------------------------------------------------------------------
# Datos (enunciado §2)
# --------------------------------------------------------------------------
TZ_CHILE = timezone(timedelta(hours=-3))          # offset fijo -03:00 (no mezclar con UTC)
T0 = datetime(2026, 11, 2, 0, 0, 0, tzinfo=TZ_CHILE)
SEMILLA = 2026                                    # ¡no la 2509 por defecto del generador!
N_HIST = 864_000                                  # 5 días x 172.800
N_VIVO = 7_200                                    # 1 hora
RITMO_VIVO_S = 0.025                              # 20x: un evento cada 25 ms
INTERVALO_LLEGADA_MS = 500                        # ~2 eventos/s al ritmo real

# Números de control del enunciado.
CONTROL = {
    "hist": {"eventos": 864_000, "repetidos": 17_090, "claves": 846_906},
    "vivo": {"eventos": 7_200, "repetidos": 153, "claves": 7_047},
    "total": {"eventos": 871_200, "repetidos": 17_243, "claves": 853_953},
    "colisiones_total": 4,   # misma estación + mismo ms, distinto valor
}

# Tipos de sensor. Se detectan por subcadena para no depender del nombre exacto
# que use el generador del Lab 06 (p. ej. "flujo_vehicular").
SUBCADENA_FLUJO = os.getenv("T2_TIPO_FLUJO", "flujo")
TIPO_AIRE = os.getenv("T2_TIPO_AIRE", "calidad_aire")

# --------------------------------------------------------------------------
# Redis
# --------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

STREAM = "sensores:t2"
GRUPO_INGESTA = "ingesta"
GRUPO_TABLERO = "tablero"

# MAXLEN ~ 10.000 (justificación en README §MAXLEN):
#  * >= 7.200: la hora en vivo completa cabe => en esta tarea nunca se recorta
#    nada que un grupo no haya leído, y se puede reconstruir el "último por llegada".
#  * Tolerancia de atraso = MAXLEN / ritmo:
#       a 20x  (40 ev/s): 10.000 / 40 = 250 s  (~4 min de reloj)
#       a 1x   ( 2 ev/s): 10.000 /  2 = 5.000 s (~83 min de datos)
#  * Memoria acotada: ~10.000 entradas x ~150 B ≈ 1,5 MB.
MAXLEN = int(os.getenv("T2_MAXLEN", "10000"))

LOTE = int(os.getenv("T2_LOTE", "500"))           # entradas por XREADGROUP
BLOCK_MS = 2_000                                  # espera bloqueante del XREADGROUP
MIN_IDLE_MS = int(os.getenv("T2_MIN_IDLE_MS", "30000"))  # XAUTOCLAIM: huérfanos > 30 s
VENTANA_MS = 10 * 60 * 1000                       # 10 minutos de MEDICIÓN


def k_fin(stream: str = STREAM) -> str:
    """Bandera que el productor pone al terminar."""
    return f"{stream}:fin"


def k_producidos(stream: str = STREAM) -> str:
    return f"{stream}:producidos"


def k_stats(stream: str, grupo: str) -> str:
    """Contador de entradas procesadas (y confirmadas) por grupo."""
    return f"{stream}:stats:{grupo}"


# --------------------------------------------------------------------------
# HBase
# --------------------------------------------------------------------------
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))
HBASE_JMX_URL = os.getenv("HBASE_JMX_URL", "http://localhost:16010/jmx")
# Comando para el hbase shell (SPLITS solo se pueden fijar desde el shell/Admin API;
# Thrift 1 no los expone).
HBASE_SHELL = os.getenv("HBASE_SHELL", "docker compose exec -T hbase hbase shell -n")
HBASE_EXEC = os.getenv("HBASE_EXEC", "docker compose exec -T hbase")

TABLA_LECTURAS = "lecturas"
TABLA_TIPO = "lecturas_tipo"
TABLA_META = "t2_meta"
CF = "d"
VERSIONES = 3          # para poder MOSTRAR en D.1 las versiones que deja un reproceso
N_REGIONES_ESTACION = 4
N_SALT = 4
BATCH_HIST = int(os.getenv("T2_BATCH_HIST", "5000"))

# Tablas del experimento de hotspotting (P5)
TABLA_HS_EST_TS = "hs_estacion_ts"
TABLA_HS_TS_EST = "hs_ts_estacion"
TABLA_HS_SALT = "hs_salt_ts_estacion"
