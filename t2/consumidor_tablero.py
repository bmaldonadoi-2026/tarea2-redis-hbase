"""B.2 — Grupo ``tablero``: el estado de "ahora" en Redis.

    python -m t2.consumidor_tablero [--nombre tablero-1] [--prefijo ""] [--stream S]

Estructuras (nombres exigidos por el enunciado, revisables con redis-cli):

  ultimo:<est>    HASH  valor, ts (medición, ms), tipo     -> último POR MEDICIÓN
  ranking:flujo   ZSET  estacion -> último valor            (solo flujo vehicular)
  ventana:<est>   ZSET  score = ts medición, miembro "ts:valor"; solo flujo,
                        recortado a (ts_max - 10 min, ts_max]
  tipo:<tipo>     SET   estaciones de ese tipo (catálogo)
  tablero:ts_max  STRING medición más reciente vista (ancla de las ventanas)

Cómo una lectura tardía NO hace retroceder el tablero
-----------------------------------------------------
Toda la actualización de un evento es UN script Lua (atómico en Redis):
compara el ``ts`` del evento con el ``ts`` guardado en ``ultimo:<est>`` y solo
escribe el hash (y el ranking) si ``ts_nuevo >= ts_guardado``. Hacerlo en Lua
y no como HGET + HSET desde Python evita la carrera "leer-comparar-escribir"
si hubiera varios consumidores del grupo tablero en paralelo.

El empate (``>=``) deja ganar al que llega último, igual que HBase con la
misma clave: así Redis y HBase coinciden incluso en las 4 colisiones de ms.

Idempotencia: SADD, HSET con los mismos valores y ZADD del mismo miembro no
cambian nada si el evento se reprocesa.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from . import config, datos, redis_util
from .consumidor_base import Consumidor, Entrada

LUA_ACTUALIZAR = r"""
-- KEYS: 1 ultimo:<est> 2 ranking:flujo 3 ventana:<est> 4 tipo:<tipo> 5 tablero:ts_max
-- ARGV: 1 estacion 2 tipo 3 valor 4 ts_ms 5 es_flujo(0/1) 6 ventana_ms
local ts = tonumber(ARGV[4])
redis.call('SADD', KEYS[4], ARGV[1])

local tsmax = tonumber(redis.call('GET', KEYS[5]) or '0')
if ts > tsmax then
  redis.call('SET', KEYS[5], ARGV[4])
  tsmax = ts
end

local avanza = 0
local previo = redis.call('HGET', KEYS[1], 'ts')
if (not previo) or ts >= tonumber(previo) then
  redis.call('HSET', KEYS[1], 'valor', ARGV[3], 'ts', ARGV[4], 'tipo', ARGV[2])
  if ARGV[5] == '1' then
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
  end
  avanza = 1
end

if ARGV[5] == '1' then
  local corte = tsmax - tonumber(ARGV[6])
  if ts > corte then
    redis.call('ZADD', KEYS[3], ARGV[4], ARGV[4] .. ':' .. ARGV[3])
  end
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', string.format('%d', corte))
end
return avanza
"""


@dataclass(frozen=True)
class ClavesTablero:
    prefijo: str = ""

    def ultimo(self, est: str) -> str: return f"{self.prefijo}ultimo:{est}"
    def ventana(self, est: str) -> str: return f"{self.prefijo}ventana:{est}"
    def tipo(self, tipo: str) -> str: return f"{self.prefijo}tipo:{tipo}"
    @property
    def ranking(self) -> str: return f"{self.prefijo}ranking:flujo"
    @property
    def ts_max(self) -> str: return f"{self.prefijo}tablero:ts_max"

    def patrones(self) -> list[str]:
        p = self.prefijo
        return [f"{p}ultimo:*", f"{p}ventana:*", f"{p}tipo:*", f"{p}ranking:flujo", f"{p}tablero:*"]


def claves_tablero(prefijo: str = "") -> ClavesTablero:
    return ClavesTablero(prefijo)


LUA_RECORTAR = r"""
-- KEYS: 1 tablero:ts_max, 2.. ventana:<est> ;  ARGV: 1 ventana_ms
local tsmax = tonumber(redis.call('GET', KEYS[1]) or '0')
local corte = string.format('%d', tsmax - tonumber(ARGV[1]))
local n = 0
for i = 2, #KEYS do
  n = n + redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', corte)
end
return n
"""


class Tablero:
    def __init__(self, prefijo: str = "") -> None:
        self.k = ClavesTablero(prefijo)
        self.r = redis_util.conectar()
        self.script = self.r.register_script(LUA_ACTUALIZAR)
        self.recortar = self.r.register_script(LUA_RECORTAR)
        self.retrocesos_evitados = 0
        # Estaciones de flujo conocidas (también las de antes de un reinicio del consumidor).
        self.flujo: set[str] = set()
        for k in self.r.scan_iter(match=f"{prefijo}tipo:*"):
            if datos.es_flujo(k.split(":")[-1]):
                self.flujo |= self.r.smembers(k)

    def __call__(self, entradas: list[Entrada]) -> None:
        p = self.r.pipeline(transaction=False)   # un viaje de red por lote
        for _, c in entradas:
            est, tipo = c["estacion_id"], c["tipo"]
            flujo = datos.es_flujo(tipo)
            if flujo:
                self.flujo.add(est)
            self.script(
                keys=[self.k.ultimo(est), self.k.ranking, self.k.ventana(est), self.k.tipo(tipo), self.k.ts_max],
                args=[est, tipo, c["valor"], c["ts"], "1" if flujo else "0", config.VENTANA_MS],
                client=p,
            )
        # Barrido final del lote: el ancla (ts_max) pudo avanzar por eventos de OTRAS
        # estaciones; sin esto, la ventana de una estación que no recibió eventos
        # recientemente quedaría con entradas de hace más de 10 minutos.
        if self.flujo:
            self.recortar(keys=[self.k.ts_max, *(self.k.ventana(e) for e in sorted(self.flujo))],
                          args=[config.VENTANA_MS], client=p)
        resultados = p.execute()
        n_eventos = len(entradas)
        self.retrocesos_evitados += sum(1 for x in resultados[:n_eventos] if x == 0)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stream", default=config.STREAM)
    ap.add_argument("--nombre", default="tablero-1")
    ap.add_argument("--prefijo", default="")
    ap.add_argument("--lote", type=int, default=config.LOTE)
    a = ap.parse_args()
    t = Tablero(a.prefijo)
    Consumidor(config.GRUPO_TABLERO, a.nombre, t, stream=a.stream, lote=a.lote).ejecutar()
    print(f"[{a.nombre}] lecturas tardías que NO movieron ultimo:* = {t.retrocesos_evitados}")


if __name__ == "__main__":
    main()
