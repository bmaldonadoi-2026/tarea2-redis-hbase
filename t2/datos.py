"""Adaptador entre el generador del Lab 06 y el pipeline.

``eventos(t0, semilla=2026)`` del Lab 06 entrega los eventos en orden de
LLEGADA. Este módulo:

1. Importa ``t2/generador.py`` (copia sin cambios del Lab 06; es código, no
   datos). Sus eventos traen estacion_id, tipo_sensor, valor y timestamp (ISO
   con ms y offset -03:00); el tiempo avanza 0,5 s por evento desde t0.
2. Normaliza cada evento a una ``Lectura`` inmutable con el timestamp de
   MEDICIÓN en epoch ms. El valor se guarda también como texto original para
   no introducir diferencias de redondeo entre Redis y HBase.
3. Separa histórico (primeros 864.000) y en vivo (7.200 siguientes).
"""
from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from itertools import islice
from typing import Any

from . import config

# Nombres de campo aceptados (el generador real usa uno de cada grupo).
_ALIAS = {
    "estacion": ("estacion_id", "estacion", "station_id", "station"),
    "tipo": ("tipo", "tipo_sensor", "sensor", "type"),
    "valor": ("valor", "value", "medicion", "lectura"),
    "ts": ("timestamp", "ts", "ts_medicion", "medicion_ts", "time"),
}


@dataclass(frozen=True, slots=True)
class Lectura:
    """Un evento normalizado.

    seq    : posición en el orden de llegada (0..871.199).
    ts_ms  : timestamp de MEDICIÓN (epoch ms) — el reloj que manda en claves y tablero.
    """
    seq: int
    estacion: str
    tipo: str
    valor: float
    valor_txt: str
    ts_ms: int
    ts_iso: str

    def clave(self) -> tuple[str, int]:
        return (self.estacion, self.ts_ms)

    def tupla(self) -> tuple[str, str, str, int]:
        """Los cuatro campos del evento (para detectar 'repetidos tal cual')."""
        return (self.estacion, self.tipo, self.valor_txt, self.ts_ms)

    @property
    def llegada_ms(self) -> int:
        """Reloj de llegada nominal (t0 + seq * 500 ms al ritmo real)."""
        return int(config.T0.timestamp() * 1000) + self.seq * config.INTERVALO_LLEGADA_MS


def _cargar_generador():
    try:
        from . import generador  # copia sin cambios de Laboratorios/06/generador.py
    except ImportError as e:
        raise SystemExit("Falta t2/generador.py: copie Laboratorios/06/generador.py a t2/generador.py") from e
    return generador


GENERADOR = _cargar_generador()


def _campo(ev: Any, nombre: str) -> Any:
    if isinstance(ev, dict):
        for alias in _ALIAS[nombre]:
            if alias in ev:
                return ev[alias]
    else:
        for alias in _ALIAS[nombre]:
            if hasattr(ev, alias):
                return getattr(ev, alias)
    raise KeyError(f"El evento no trae el campo '{nombre}' (alias {_ALIAS[nombre]}): {ev!r}")


def _a_ms(ts: Any) -> tuple[int, str]:
    """Acepta ISO 8601, datetime o epoch (s o ms). Devuelve (epoch_ms, iso)."""
    if isinstance(ts, str):
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=config.TZ_CHILE)
        return int(round(dt.timestamp() * 1000)), ts
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=config.TZ_CHILE)
        return int(round(ts.timestamp() * 1000)), ts.isoformat(timespec="milliseconds")
    if hasattr(ts, "to_pydatetime"):            # pandas.Timestamp
        return _a_ms(ts.to_pydatetime())
    x = float(ts)
    ms = int(round(x if x > 1e11 else x * 1000))
    iso = datetime.fromtimestamp(ms / 1000, config.TZ_CHILE).isoformat(timespec="milliseconds")
    return ms, iso


def normalizar(seq: int, ev: Any) -> Lectura:
    valor = _campo(ev, "valor")
    ts_ms, ts_iso = _a_ms(_campo(ev, "ts"))
    return Lectura(
        seq=seq,
        estacion=str(_campo(ev, "estacion")),
        tipo=str(_campo(ev, "tipo")),
        valor=float(valor),
        # repr(float) = representación más corta que reproduce el mismo float
        # (y evita 'np.float64(...)' si el generador entrega tipos de numpy).
        valor_txt=valor if isinstance(valor, str) else repr(float(valor)),
        ts_ms=ts_ms,
        ts_iso=str(ts_iso),
    )


def _iterador():
    """Llama a eventos(t0, semilla=2026). Prueba datetime y, si no, ISO string."""
    fn = GENERADOR.eventos
    params = inspect.signature(fn).parameters
    kw = {"semilla": config.SEMILLA} if "semilla" in params else {}
    try:
        return iter(fn(config.T0, **kw)) if kw else iter(fn(config.T0, config.SEMILLA))
    except TypeError:
        return iter(fn(config.T0.isoformat(), **kw))


@lru_cache(maxsize=1)
def todas() -> tuple[Lectura, ...]:
    """Los 871.200 eventos, en orden de llegada (cacheado por proceso)."""
    n = config.N_HIST + config.N_VIVO
    out = tuple(normalizar(i, ev) for i, ev in enumerate(islice(_iterador(), n)))
    if len(out) != n:
        raise RuntimeError(f"El generador entregó {len(out)} eventos, se esperaban {n}")
    return out


def historico() -> tuple[Lectura, ...]:
    return todas()[: config.N_HIST]


def en_vivo() -> tuple[Lectura, ...]:
    return todas()[config.N_HIST:]


def catalogo(lecturas=None) -> dict[str, list[str]]:
    """tipo -> estaciones (ordenadas)."""
    cat: dict[str, set[str]] = {}
    for l in lecturas or todas():
        cat.setdefault(l.tipo, set()).add(l.estacion)
    return {t: sorted(e) for t, e in sorted(cat.items())}


def es_flujo(tipo: str) -> bool:
    return config.SUBCADENA_FLUJO in tipo


def estadisticas_control(lecturas) -> dict[str, int]:
    """Recalcula los números de control sobre un conjunto de lecturas.

    repetidos  = eventos cuyos 4 campos son idénticos a otro anterior
    claves     = pares (estacion, ts) distintos  -> filas esperadas en HBase
    colisiones = tuplas distintas que comparten clave (mismo ms, distinto valor)
    tardias    = medición > 2 s antes de su llegada nominal (aprox. del ~3 %)
    """
    lecturas = list(lecturas)
    tuplas = {l.tupla() for l in lecturas}
    claves = {l.clave() for l in lecturas}
    tardias = sum(1 for l in lecturas if l.llegada_ms - l.ts_ms > 2_000)
    return {
        "eventos": len(lecturas),
        "repetidos": len(lecturas) - len(tuplas),
        "claves": len(claves),
        "colisiones": len(tuplas) - len(claves),
        "tardias_aprox": tardias,
    }
