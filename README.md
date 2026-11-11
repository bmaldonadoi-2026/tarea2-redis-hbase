# Tarea 2 — Redis + HBase: del stream a la historia

Procesamiento Masivo de Datos · ELE051-B / EIN102B · USM 2026-2

```
                              ┌──────────────── Redis 7 (AOF everysec) ────────────────┐
 generador.py (semilla 2026)  │                                                         │
   │                          │  stream sensores:t2  (MAXLEN ~ 10000)                   │
   │ 7.200 en vivo, 20x ─────►│   ├─ grupo ingesta ──► consumidor_ingesta ──┐            │
   │  (productor, XADD)       │   └─ grupo tablero ──► consumidor_tablero   │            │
   │                          │        ultimo:<est>  ranking:flujo          │            │
   │                          │        ventana:<est> tipo:<tipo>            │            │
   │                          └─────────────────────────────────────────────┼────────────┘
   │ 864.000 histórico (batch)                                               │ batch + XACK después
   └───────────────────────────────────────►  HBase 2.5.15 (Lab 07)◄───────┘
                                              lecturas       (estacion#ts13)      4 regiones
                                              lecturas_tipo  (tipo#ts13#estacion) 3 regiones
```

## 1. Cómo reproducir desde cero

Requisitos: Docker con compose v2 (≥ 4 GB de RAM asignados a Docker, ~3 GB de disco) y `python3` ≥ 3.10.

```bash
git clone <este repo> && cd <repo>
# t2/generador.py ya viene: es Laboratorios/06/generador.py SIN cambios (es código, no datos)
./run.sh                      # todo: A + B + C + D
./run.sh --sin-experimentos   # sin la Parte D
./run.sh --desde-cero         # además borra los volúmenes (docker compose down -v)
```

`run.sh` hace, en orden y sin pasos manuales:

| Paso | Comando | Qué hace |
|---|---|---|
| 1 | `docker compose build` / `up -d --wait` | levanta `redis` y `hbase`; `--wait` espera a los dos healthchecks |
| 2 | `python3 -m venv .venv` + `pip install -r requirements.txt` | ambiente virtual |
| 3 | `python -m t2.esperar` | además del healthcheck, prueba un CREATE/DROP real por Thrift |
| 4 | `python -m t2.carga_historica` | B.1: 864.000 eventos → `lecturas` y `lecturas_tipo` (se salta si ya están; `--forzar` recarga) |
| 5 | `preparar_vivo` → `consumidor_tablero &` `consumidor_ingesta &` → `productor` | B.2: la hora en vivo a 20x |
| 6 | `python -m t2.verificar` | B.3: control, XPENDING = 0, filas por tabla |
| 7 | `python -m t2.preguntas` | Parte C → `resultados/preguntas.md` y `.json` (también `preguntas.ipynb`) |
| 8 | `python -m t2.exp_d1` · `python -m t2.exp_d2 aof sin aof-kill` | Parte D → `resultados/d1.json`, `d2_*.json` |

**Cuánto tarda** (medido en un AMD Ryzen 5 4600H, Windows 10 + WSL2 + Docker Desktop):

| Etapa | Tiempo |
|---|---|
| `docker compose build` (una vez) | **~30 min**: HBase 2.5.15 ya no está en el CDN y baja del archivo lento de Apache |
| `up --wait` (Redis + HBase sanos) | 22 s |
| `.venv` + requirements | ~30 s |
| Carga histórica (2 × 864.000 puts) | 33 s (+ generación 8 s) |
| Hora en vivo (7.200 × 25 ms) | 3 min 10 s |
| Verificación | 24 s |
| Preguntas P1–P6 | ~4 min |
| D.1 / D.2 (aof, sin, aof-kill) | ~30 s / ~12 min |

**Total sin el build: ~10 min (`--sin-experimentos`) o ~23 min con la Parte D.**

Cada pieza se puede correr suelta (todas aceptan `--help`), p. ej. `python -m t2.preguntas P3 P5`.

## 2. Estructura

```
docker-compose.yml        redis (AOF) + hbase (build ./hbase) con healthchecks y volúmenes con nombre
hbase/Dockerfile         el del Lab 07, sin cambios (HBase 2.5.15 standalone + Thrift)
run.sh                    orquestador de punta a punta
requirements.txt          redis, happybase, thriftpy2, pandas
t2/config.py              TODOS los parámetros (t0, semilla, MAXLEN, lotes, nombres, números de control)
t2/datos.py               adaptador del generador del Lab 06 → Lectura(estacion, tipo, valor, ts_ms...)
t2/claves.py              row keys y SPLITS (funciones puras)
t2/escritura.py           cómo se escribe UNA lectura en cada tabla (compartido por todos)
t2/hbase_util.py          conexión, CREATE con SPLITS vía hbase shell, conteo, scan reverso, JMX
t2/carga_historica.py     B.1
t2/productor.py           B.2 productor (XADD MAXLEN ~)
t2/consumidor_base.py     bucle de consumer group: propios → XAUTOCLAIM → '>' ; XACK después
t2/consumidor_ingesta.py  B.2 grupo ingesta → HBase (2 tablas)
t2/consumidor_tablero.py  B.2 grupo tablero → Redis (script Lua atómico)
t2/verificar.py           B.3
t2/preguntas.py           Parte C (P1..P6) + preguntas.ipynb
t2/exp_d1.py, exp_d2.py   Parte D
t2/generador.py           copia sin cambios de Laboratorios/06/generador.py (eventos(t0, semilla=2026))
```

## 3. Decisiones

### 3.1 Persistencia de Redis: AOF, `appendfsync everysec`, sin RDB

Escrita en `docker-compose.yml` (`--appendonly yes --appendfsync everysec --save "" --maxmemory-policy noeviction`).

* **Por qué persistir.** En este pipeline Redis no es solo un caché: el stream `sensores:t2` y la PEL de cada grupo
  son la **única copia** de los eventos que todavía no llegan a HBase, y los grupos guardan *hasta dónde leyó cada
  uno* (`last-delivered-id`). Sin persistencia, un reinicio borra el stream, los grupos (los consumidores fallan con
  `NOGROUP`) y el tablero: todo lo que no estaba confirmado en HBase se pierde (lo muestra D.2).
* **Por qué AOF y no RDB.** RDB es una foto cada N minutos: ante un crash se pierde todo lo posterior a la última
  foto (minutos de eventos). AOF registra cada escritura; con `everysec` el fsync va en un hilo aparte una vez por
  segundo, así que el costo en throughput es casi nulo y **la pérdida máxima ante un crash es ~1 s** (≈ 2 eventos al
  ritmo real, ≈ 40 a 20x). En un apagado ordenado (`docker compose restart` manda SIGTERM) Redis hace fsync antes de
  salir: pérdida 0.
* **Por qué no `always`.** Haría fsync en cada XADD/XACK: pérdida 0 incluso con crash, pero cada escritura espera al
  disco. Al ritmo real (2 ev/s) sería asumible; a 20x y con los XACK y el tablero sumados, multiplica la latencia.
  Lo que se pierde con `everysec` (1 s de lecturas de sensores) es tolerable para un tablero y una historia agregada.
* **Por qué `--save ""`.** Deja la decisión limpia (solo AOF) y hace que D.2 compare exactamente "AOF" contra "nada".
  Ojo: la imagen oficial de Redis, sin configuración, **sí** hace RDB al apagarse (`save 3600 1 300 100 60 10000`),
  así que "sin persistencia" exige `--save ""` explícito; si no, el reinicio ordenado sobreviviría igual por RDB.
* **`noeviction`.** Con cualquier política de desalojo, Redis podría borrar el stream o una ventana para hacer
  espacio. Preferimos que un XADD falle (error visible) a perder datos en silencio.

### 3.2 Row keys

El tiempo va como **epoch en milisegundos rellenado a 13 dígitos** (`ts13`): las claves se comparan como bytes, y a
ancho fijo el orden lexicográfico es el numérico; no hay zona horaria en la clave (cero riesgo de mezclar UTC con hora
de Chile) y es la misma unidad que el `ts` de `ultimo:*` y el score de `ventana:*`.

| Tabla | Row key | Rápida | Lenta |
|---|---|---|---|
| `lecturas` | `<estacion>#<ts13>` — `CC-001#1793804400000` | una estación en un rango de tiempo (P3, P4a): un seek + lectura contigua; "lo último de una estación" (scan reverso, limit 1) | un instante para **todas** las estaciones (16 rangos, P5); cualquier filtro por valor o por tipo (recorrer todo) |
| `lecturas_tipo` | `<tipo>#<ts13>#<estacion>` — `calidad_aire#1793876400000#CC-003` | un tipo en un rango de tiempo (P4b): **un** scan contiguo con todas las estaciones del tipo | una estación sola (hay que leer el tipo completo y filtrar) |

**Idempotencia.** La clave sale de la *estación* y del *timestamp de medición*, nunca de la llegada ni del ID del stream:

* un evento **repetido** (17.243 "repetidos tal cual") cae en la misma fila → la fila no se duplica (queda una versión
  más de la celda, con el mismo valor);
* un evento **reentregado** por Redis tras una caída (D.1) o reescrito tras un fallo en la segunda tabla (P4) cae en
  la misma fila;
* una lectura **tardía** cae en su lugar cronológico, no al final: los scans por rango de medición la encuentran donde
  corresponde.

Por eso el conteo esperado es **853.953 claves**, no 871.200 eventos: 871.200 − 17.243 repetidos − 4 colisiones
(misma estación y mismo ms con distinto valor: HBase conserva el último escrito; la pauta lo tolera). En
`lecturas_tipo` el conteo es el mismo porque el tipo es función de la estación.

La estación va al final de la clave de `lecturas_tipo` solo para desempatar (dos estaciones del mismo tipo pueden medir
en el mismo ms). Columnas: una sola familia `d` (nombre corto: se repite en cada celda), `VERSIONS => 3` (para poder
*mostrar* en D.1 lo que deja un reproceso) y `BLOOMFILTER => 'ROW'`.

### 3.3 Regiones (SPLITS)

Thrift 1 no permite pre-dividir, así que las tablas se crean con `hbase shell` (`t2/hbase_util.crear_tabla`), igual que
en el Lab 07. Los cortes se calculan desde el catálogo real de los datos:

* **`lecturas`: 4 regiones de 4 estaciones, cortes en bordes de estación**: `SPLITS => ['CC-005', 'HUA-001', 'SPP-002']`
  (la 5.ª, 9.ª y 13.ª de las 16 en orden). El Lab 07 cortaba por comuna (`COR`, `PEN`, `TAL`), lo que deja regiones
  de 6, 4, 3 y 3 estaciones; cortar por posición en el catálogo deja exactamente 4 por región.
  Cada estación queda **entera** en una región, así que P3/P4a tocan una sola región; las 16 estaciones emiten a ritmo
  parecido, así que cada región recibe ~¼ de las escrituras, también en vivo (las 16 escriben a la vez → no hay
  hotspot). 4 y no 16 porque es un solo RegionServer con ~100 MB: 16 regiones de pocos MB solo agregan memstores y
  flushes sin paralelismo real.
* **`lecturas_tipo`: 3 regiones, una por tipo**: `SPLITS => ['flujo_vehicular#', 'ocupacion_estacionamiento#']`.
  Un scan de P4b nunca cruza regiones. No quedan parejas (calidad_aire 4 estaciones, flujo_vehicular 8,
  ocupacion_estacionamiento 4): la región de flujo recibe el doble; se aceptó porque la pregunta que justifica la
  tabla (P4) es por tipo y así cada tipo es un único rango contiguo dentro de una región.
* HBase no va a dividir estas regiones por su cuenta: cada una queda en decenas de MB y el umbral de split es ~10 GB.
  Así el número de regiones es el que decidimos y la tabla de JMX de P5 es reproducible.

### 3.4 MAXLEN del stream: `MAXLEN ~ 10000`

* **Atraso tolerado** = MAXLEN / ritmo de llegada: a 20x (40 ev/s) son **250 s (~4 min)** de atraso del consumidor más
  lento; al ritmo real (2 ev/s) son **5.000 s (~83 min)**, es decir, "el stream de la última hora" con margen.
* **≥ 7.200**: la hora en vivo completa cabe, así que en esta tarea nunca se recorta algo que un grupo no haya leído, y
  P1 puede reconstruir el "último por llegada" desde el stream.
* **Memoria acotada**: ~10.000 entradas × ~150 B ≈ 1,5 MB.
* **`~` (aproximado)**: Redis solo recorta nodos completos del radix tree (100 entradas por nodo), lo que vuelve el
  recorte O(1) amortizado. La longitud puede quedar un poco por encima de MAXLEN, nunca por debajo.
* **¿Qué pasa con una entrada recortada que nadie leyó?** Se pierde **en silencio**: XTRIM/MAXLEN no mira a los
  grupos. Si nunca se entregó, no está en ninguna PEL y ningún consumidor sabrá que existió (solo el `lag` del grupo lo
  insinúa). Si ya se había entregado pero no confirmado, su ID sigue en la PEL pero sin contenido: `XREADGROUP … 0` la
  devuelve con campos `nil` y `XAUTOCLAIM` (Redis ≥ 7) la reporta como borrada y la saca de la PEL. Verificado con
  `redis-cli`. `consumidor_base.py` cuenta y avisa esos casos como `perdidas`. Por eso MAXLEN debe dimensionarse
  sobre el **peor atraso** esperado del grupo más lento, no sobre el promedio. (Redis 8.2 agrega `XADD … ACKED`, que
  solo recorta entradas confirmadas por todos los grupos.)

### 3.5 Consumer groups, XACK y "exactamente una vez" en efecto

* Dos grupos (`ingesta`, `tablero`) sobre el mismo stream, creados en `0` **antes** de producir (con `$` se perderían
  los eventos escritos antes de crear el grupo). Cada grupo ve el stream completo y lleva su propio avance.
* **XACK después del put**, siempre (`consumidor_base._procesar`): primero `manejador(lote)` — en ingesta, un batch a
  `lecturas` y otro a `lecturas_tipo` — y solo si ambos vuelven sin excepción, `XACK`. Si el proceso muere entremedio,
  las entradas quedan en la PEL y se reentregan → **at-least-once**. Como las escrituras son idempotentes (§3.2), el
  efecto es el de *exactamente una vez*. Si el XACK fuera antes, una muerte entre XACK y put perdería el lote: Redis
  ya lo dio por procesado y nadie lo reintentaría.
* Cada consumidor, al arrancar y periódicamente: retoma lo propio pendiente (`XREADGROUP … 0`), reclama huérfanos de
  otros con más de 30 s sin confirmar (`XAUTOCLAIM`) y luego lee lo nuevo (`>`). Termina cuando el productor marcó
  `sensores:t2:fin`, no queda nada nuevo y la PEL del grupo está en 0.
* Si falla el segundo put (a `lecturas_tipo`) tras escribir el primero, no hay XACK, el lote se reintenta y el primero
  se reescribe sobre las mismas filas: las dos tablas quedan consistentes eventualmente, sin transacciones entre
  tablas (HBase no las tiene).

### 3.6 Tablero: "último" significa último **por medición**

Cada evento se aplica con **un script Lua** (`consumidor_tablero.LUA_ACTUALIZAR`), atómico dentro de Redis:

```lua
local previo = redis.call('HGET', KEYS[1], 'ts')
if (not previo) or ts >= tonumber(previo) then   -- solo avanza, nunca retrocede
  HSET ultimo:<est> valor .. ts .. tipo ..  ;  ZADD ranking:flujo valor est
end
```

Una lectura tardía (ts menor que el guardado) **no toca** `ultimo:*` ni `ranking:flujo`, pero sí entra a la ventana si su
medición está dentro de los últimos 10 min. En Lua y no como `HGET` + `HSET` desde Python, para que no haya carrera
"leer-comparar-escribir" si se escalan varios consumidores del grupo. El empate (`>=`) deja ganar al que llega
después, igual que HBase con la misma clave, así que Redis y HBase coinciden incluso en las colisiones de ms.

`ventana:<est>` usa score = ts de medición y miembro `"<ts>:<valor>"` (si el miembro fuera solo el valor, dos lecturas
con el mismo valor colapsarían en una). Se recorta con `ZREMRANGEBYSCORE -inf (ts_max − 10 min)`, con `ts_max` = la
medición más reciente del stream (`tablero:ts_max`), que es el ancla que pide P2. Todo es idempotente ante reproceso.

### 3.7 Carga con batch

`Table.batch(batch_size=5000)`: happybase acumula 5.000 mutaciones y las manda en **un** `mutateRows`. Put por put
serían 864.000 viajes de red por tabla; en batch, ~173. Las dos tablas se cargan en pasadas separadas para medir por
separado lo que cuesta cada una (P4). El tiempo y las filas/s quedan en `resultados/carga_historica.json`.

### 3.8 Conteo eficiente

`FirstKeyOnlyFilter() AND KeyOnlyFilter()`: el RegionServer corta cada fila tras su primera celda y de esa celda no
manda el valor; por la red viaja ~una clave por fila.

## 4. Verificación (`python -m t2.verificar`)

Recalcula los números de control desde el generador y los compara con el enunciado; imprime eventos producidos,
`entries-read`, `lag`, entradas procesadas y `XPENDING` de cada grupo (debe ser 0); cuenta filas de `lecturas` y
`lecturas_tipo` (esperado 853.953 tras la hora en vivo; `--solo-historico` → 846.906) y revisa que `ultimo:*` sea el
último por medición, que las ventanas estén recortadas y que el catálogo esté completo. Si algo no calza explica la
causa probable (faltan filas → lote perdido / XACK antes del put; sobran → clave no idempotente). Sale con código 1 si
algo falla.

## 5. Parte C y D

* `python -m t2.preguntas` → `resultados/preguntas.md` (tabla de tiempos con el hardware + JSON por pregunta).
  Tiempos: 1 corrida de calentamiento reportada aparte + mediana de 5.
* `python -m t2.exp_d1` (stream y tablas aislados `*_d1`): mata a `ingesta-A` con `os._exit(1)` después del put del
  lote 4 y antes del XACK, muestra la PEL, que la fila ya está en HBase, recupera con `ingesta-B` (XAUTOCLAIM) y
  comprueba 7.047 filas y las versiones de la celda.
* `python -m t2.exp_d2 aof sin aof-kill`: reinicia Redis con ~3.000 eventos producidos y fotografía qué sobrevivió.
  Cambia la configuración con `REDIS_APPENDONLY=no` sin editar el compose y al final la restaura.

## 6. Escala: hasta dónde llegan estas mediciones

HBase standalone es **una** JVM con un RegionServer, ZooKeeper embebido y el sistema de archivos local (sin HDFS, sin
réplicas, `hbase.unsafe.stream.capability.enforce=false`: el WAL no tiene las garantías de hsync de HDFS). Lo que se
extrapola: órdenes de magnitud entre patrones de acceso (rango vs tabla completa, 1 vs 16 rangos, Redis vs HBase),
que dependen de cuántas filas se leen y no del hardware. Lo que no: el throughput absoluto de escritura, el efecto en
tiempo del hotspot (con un RegionServer, todas las regiones comparten la misma máquina: el hotspot se ve en JMX, no
en la latencia) y la durabilidad de HBase.

## 7. Qué no funcionó / pendientes

* *(Completar con lo que les pase al correrlo.)*
* La primera versión del tablero solo recortaba la ventana de la estación que recibía el evento: si el ancla
  (`tablero:ts_max`) avanzaba por otra estación, quedaban entradas de más de 10 min. Lo detectó `verificar.py`; se
  agregó un barrido de todas las ventanas de flujo al final de cada lote (`LUA_RECORTAR`).
* A 40 ev/s cada `XREADGROUP` traía ~1 entrada (~7.000 RPC a HBase). Se agregó micro-batching en ingesta
  (`--espera-lote-ms 250`): ~650 lotes, a cambio de ≤ 250 ms más de latencia hacia la historia. El tablero no espera.
* El top 5 desde HBase desempataba distinto que `ZREVRANGE` (a igual puntaje, Redis ordena lexicográfico inverso).
* El servidor Thrift de HBase cierra las conexiones inactivas por más de 60 s
  (`hbase.thrift.server.socket.read.timeout`): `TTransportException: TSocket read 0 bytes`. Le pasó a D.2, que abría
  la conexión y la usaba ~75 s después. Ahora cada medición abre su conexión, e ingesta reconecta y reintenta el lote
  (sin XACK de por medio, así que no se pierde nada).
* Puertos: si quedó corriendo el Redis del Lab 06 (`pmd-redis`) o el HBase del Lab 07 (`pmd-hbase`), ocupan 6379 /
  9090 / 16010. Apáguenlos antes (`docker compose down` en sus carpetas). La imagen `pmd-hbase` sí se reutiliza.
