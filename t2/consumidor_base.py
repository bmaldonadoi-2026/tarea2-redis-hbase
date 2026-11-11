"""Bucle común de un consumidor de un consumer group de Redis Streams.

Garantía: AT-LEAST-ONCE. El XACK de un lote se envía SOLO después de que el
manejador terminó sin excepción (para ingesta: después de que HBase confirmó
las escrituras en las dos tablas). Si el proceso muere entre medio, las
entradas quedan en la PEL (XPENDING) a su nombre y otro consumidor las
recupera con XAUTOCLAIM. Los manejadores son idempotentes, así que reprocesar
no duplica nada.

Orden de lectura en cada vuelta:
  1. al arrancar, lo PROPIO pendiente (XREADGROUP ... 0): un consumidor que se
     reinicia con el mismo nombre retoma lo que dejó sin confirmar;
  2. huérfanos de OTROS consumidores con más de ``min_idle_ms`` sin confirmar
     (XAUTOCLAIM);
  3. lo nuevo (XREADGROUP ... '>').
"""
from __future__ import annotations

import os
import sys
import time
from typing import Callable

import redis

from . import config, redis_util

Entrada = tuple[str, dict[str, str]]
Manejador = Callable[[list[Entrada]], None]


class Consumidor:
    def __init__(self, grupo: str, nombre: str, manejador: Manejador, *,
                 stream: str = config.STREAM, lote: int = config.LOTE,
                 block_ms: int = config.BLOCK_MS, min_idle_ms: int = config.MIN_IDLE_MS,
                 morir_tras_lote: int | None = None, reclamar_cada_s: float = 5.0,
                 espera_lote_ms: int = 0) -> None:
        self.r = redis_util.conectar()
        self.grupo, self.nombre, self.manejar = grupo, nombre, manejador
        self.stream, self.lote, self.block_ms = stream, lote, block_ms
        self.min_idle_ms, self.morir_tras_lote = min_idle_ms, morir_tras_lote
        self.reclamar_cada_s = reclamar_cada_s
        self.espera_lote_ms = espera_lote_ms
        self.lotes = 0
        self.procesadas = 0
        self.recuperadas = 0
        self.perdidas = 0          # entradas en la PEL cuyo contenido ya fue recortado
        self._ultimo_reclamo = 0.0

    # ------------------------------------------------------------ un lote
    def _procesar(self, entradas: list[Entrada], origen: str) -> None:
        validas = [(i, c) for i, c in entradas if c]
        borradas = [i for i, c in entradas if not c]
        if borradas:
            # El ID sigue en la PEL pero la entrada ya no existe (MAXLEN la recortó).
            self.perdidas += len(borradas)
            print(f"[{self.nombre}] ¡{len(borradas)} entradas pendientes ya recortadas del stream!", file=sys.stderr)
        if validas:
            self.manejar(validas)                       # <- put en HBase / tablero
        self.lotes += 1
        if self.morir_tras_lote and self.lotes >= self.morir_tras_lote:
            # Experimento D.1: muerte abrupta DESPUÉS del put y ANTES del XACK.
            print(f"[{self.nombre}] os._exit(1) tras el put del lote {self.lotes} "
                  f"({len(entradas)} entradas, {entradas[0][0]}..{entradas[-1][0]}) — sin XACK", flush=True)
            os._exit(1)
        ids = [i for i, _ in entradas]
        p = self.r.pipeline(transaction=True)
        p.xack(self.stream, self.grupo, *ids)            # <- XACK SIEMPRE DESPUÉS
        p.incrby(config.k_stats(self.stream, self.grupo), len(validas))
        p.execute()
        self.procesadas += len(validas)
        if origen != "nuevo":
            self.recuperadas += len(validas)

    # ------------------------------------------------------------ fuentes
    def _drenar_propios(self) -> None:
        while True:
            resp = self.r.xreadgroup(self.grupo, self.nombre, {self.stream: "0"}, count=self.lote)
            entradas = resp[0][1] if resp else []
            if not entradas:
                return
            print(f"[{self.nombre}] retomando {len(entradas)} pendientes propias", flush=True)
            self._procesar(entradas, "propio")

    def _reclamar_huerfanos(self, forzar: bool = False) -> None:
        if not forzar and time.monotonic() - self._ultimo_reclamo < self.reclamar_cada_s:
            return
        self._ultimo_reclamo = time.monotonic()
        inicio = "0-0"
        while True:
            res = self.r.xautoclaim(self.stream, self.grupo, self.nombre,
                                    min_idle_time=self.min_idle_ms, start_id=inicio, count=self.lote)
            inicio, entradas = res[0], res[1]
            borradas = res[2] if len(res) > 2 else []   # Redis >= 7: IDs ya inexistentes (se quitan de la PEL)
            if borradas:
                self.perdidas += len(borradas)
            if entradas:
                print(f"[{self.nombre}] XAUTOCLAIM recuperó {len(entradas)} entradas huérfanas "
                      f"({entradas[0][0]}..{entradas[-1][0]})", flush=True)
                self._procesar(entradas, "reclamado")
            if inicio == "0-0":
                return

    def _terminado(self) -> bool:
        pend = self.r.xpending(self.stream, self.grupo)["pending"]
        return pend == 0

    # ------------------------------------------------------------ bucle
    def ejecutar(self) -> None:
        t0 = time.perf_counter()
        print(f"[{self.nombre}] grupo={self.grupo} stream={self.stream} lote={self.lote}", flush=True)
        while True:
            try:
                self._drenar_propios()
                while True:
                    self._reclamar_huerfanos()
                    # La bandera se lee ANTES del XREADGROUP: si estaba puesta y aun
                    # así no llegó nada nuevo, de verdad no queda nada por leer.
                    fin = self.r.exists(config.k_fin(self.stream))
                    resp = self.r.xreadgroup(self.grupo, self.nombre, {self.stream: ">"},
                                             count=self.lote, block=self.block_ms)
                    entradas = resp[0][1] if resp else []
                    if entradas and len(entradas) < self.lote and self.espera_lote_ms:
                        # Micro-batching: con 40 ev/s cada XREADGROUP trae ~1 entrada.
                        # Esperar un poco junta más por lote (menos RPC a HBase) a cambio
                        # de <= espera_lote_ms de latencia hacia la historia.
                        time.sleep(self.espera_lote_ms / 1000)
                        resp = self.r.xreadgroup(self.grupo, self.nombre, {self.stream: ">"},
                                                 count=self.lote - len(entradas))
                        entradas += resp[0][1] if resp else []
                    if entradas:
                        self._procesar(entradas, "nuevo")
                        continue
                    if fin:
                        self._reclamar_huerfanos(forzar=True)
                        if self._terminado():
                            break
                break
            except (redis.ConnectionError, redis.BusyLoadingError, redis.TimeoutError, OSError) as e:
                print(f"[{self.nombre}] Redis no disponible ({type(e).__name__}); reintento en 1 s", flush=True)
                time.sleep(1)
            except redis.ResponseError as e:
                if "NOGROUP" in str(e):
                    print(f"[{self.nombre}] El grupo '{self.grupo}' ya no existe (¿Redis reinició sin "
                          f"persistencia?). Termino.", file=sys.stderr, flush=True)
                    sys.exit(2)
                raise
        dur = time.perf_counter() - t0
        print(f"[{self.nombre}] fin: {self.procesadas} entradas en {self.lotes} lotes "
              f"({dur:.1f} s); recuperadas={self.recuperadas} perdidas={self.perdidas}", flush=True)
