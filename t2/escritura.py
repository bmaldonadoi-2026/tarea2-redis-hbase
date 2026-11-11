"""Cómo se escribe UNA lectura en cada tabla (compartido por carga histórica,
consumidor de ingesta y experimentos, para que todos produzcan filas idénticas)."""
from __future__ import annotations

from . import claves, config

_V = f"{config.CF}:valor".encode()
_T = f"{config.CF}:tipo".encode()
_E = f"{config.CF}:estacion".encode()
_TS = f"{config.CF}:ts".encode()


def fila_lecturas(estacion: str, tipo: str, valor_txt: str, ts_ms: int, ts_iso: str):
    """(row_key, celdas) para la tabla ``lecturas``."""
    return claves.clave_lecturas(estacion, ts_ms), {
        _V: valor_txt.encode(), _T: tipo.encode(), _TS: ts_iso.encode(),
    }


def fila_tipo(estacion: str, tipo: str, valor_txt: str, ts_ms: int):
    """(row_key, celdas) para la tabla ``lecturas_tipo``."""
    return claves.clave_tipo(tipo, ts_ms, estacion), {
        _V: valor_txt.encode(), _E: estacion.encode(),
    }


def celdas_hs(estacion: str, tipo: str, valor_txt: str) -> dict:
    """Celdas de las tablas de hotspotting de P5 (misma información, distinta clave)."""
    return {_V: valor_txt.encode(), _T: tipo.encode(), _E: estacion.encode()}


COL_VALOR = _V
COL_TIPO = _T
COL_ESTACION = _E
COL_TS = _TS
