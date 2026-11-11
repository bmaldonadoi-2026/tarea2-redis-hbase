"""Medición de tiempos: 1 corrida de calentamiento (descartada) + mediana de 5."""
from __future__ import annotations

import os
import platform
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class Medicion:
    resultado: Any
    mediana_ms: float
    tiempos_ms: list[float] = field(default_factory=list)
    calentamiento_ms: float = 0.0

    def resumen(self) -> dict:
        return {"mediana_ms": round(self.mediana_ms, 3),
                "tiempos_ms": [round(t, 3) for t in self.tiempos_ms],
                "calentamiento_ms": round(self.calentamiento_ms, 3)}


def medir(fn: Callable[[], Any], repeticiones: int = 5) -> Medicion:
    """La primera corrida calienta cachés (block cache de HBase, page cache,
    conexiones, scripts cargados) y se reporta aparte; se miden 5 más."""
    t = time.perf_counter()
    fn()
    calentamiento = (time.perf_counter() - t) * 1000
    tiempos, resultado = [], None
    for _ in range(repeticiones):
        t = time.perf_counter()
        resultado = fn()
        tiempos.append((time.perf_counter() - t) * 1000)
    return Medicion(resultado, statistics.median(tiempos), tiempos, calentamiento)


def hardware() -> dict:
    """Para la tabla de tiempos del informe: en qué máquina se midió."""
    info = {"sistema": platform.platform(), "python": platform.python_version(),
            "cpu": platform.processor() or platform.machine(), "nucleos": os.cpu_count()}
    try:
        with open("/proc/cpuinfo") as f:
            for linea in f:
                if linea.startswith("model name"):
                    info["cpu"] = linea.split(":", 1)[1].strip()
                    break
    except OSError:
        pass
    try:
        info["ram_gb"] = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    except (ValueError, OSError, AttributeError):
        pass
    return info
