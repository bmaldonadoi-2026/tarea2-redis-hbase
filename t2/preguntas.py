"""Parte C — Las seis preguntas, cada una desde el motor que corresponde.

    python -m t2.preguntas            # todas
    python -m t2.preguntas P1 P3      # algunas

Cada pregunta devuelve un dict con resultado, tiempos (calentamiento + mediana
de 5) e interpretación. Al final se escriben ``resultados/preguntas.json`` y
``resultados/preguntas.md`` (tablas listas para el informe).

Requiere haber corrido el pipeline completo (carga histórica + hora en vivo):
el tablero de Redis y las tablas de HBase deben estar poblados.
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime

import pandas as pd

from . import claves, config, datos, escritura, hbase_util, redis_util
from .consumidor_tablero import ClavesTablero
from .medicion import hardware, medir

K = ClavesTablero()
COL_V, COL_TS = escritura.COL_VALOR, escritura.COL_TS


def iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, config.TZ_CHILE).isoformat(timespec="milliseconds")


def _ts_de_clave(row: bytes, pos: int) -> int:
    return int(row.decode().split("#")[pos])


def _estaciones(r) -> dict[str, list[str]]:
    """Catálogo desde Redis: tipo -> estaciones (SMEMBERS tipo:*)."""
    cat = {}
    for k in sorted(r.scan_iter(match="tipo:*")):
        cat[k.split(":", 1)[1]] = sorted(r.smembers(k))
    return cat


# =========================================================================== P1
def p1(r, conn) -> dict:
    """Ahora — último valor de las 16 estaciones + top 5 de flujo: Redis vs HBase."""
    cat = _estaciones(r)
    todas = sorted(e for es in cat.values() for e in es)
    flujo = {e for t, es in cat.items() if datos.es_flujo(t) for e in es}
    t_lect = conn.table(config.TABLA_LECTURAS)

    def desde_redis():
        p = r.pipeline(transaction=False)
        for e in todas:
            p.hgetall(K.ultimo(e))
        p.zrevrange(K.ranking, 0, 4, withscores=True)
        res = p.execute()
        ultimos = {e: (int(h["ts"]), h["valor"]) for e, h in zip(todas, res[:-1])}
        return ultimos, [(e, v) for e, v in res[-1]]

    def desde_hbase():
        # Con clave estacion#ts el último POR MEDICIÓN es la última fila del rango
        # de la estación: un scan REVERSO con limit=1 por estación (16 seeks).
        ultimos = {}
        for e in todas:
            fila = hbase_util.ultima_fila(t_lect, e, columns=[COL_V])
            if fila:
                ultimos[e] = (_ts_de_clave(fila[0], 1), fila[1][COL_V].decode())
        # Mismo desempate que ZREVRANGE: a igual puntaje, orden lexicográfico inverso.
        top = sorted(((e, float(v)) for e, (_, v) in ultimos.items() if e in flujo),
                     key=lambda x: (x[1], x[0]), reverse=True)[:5]
        return ultimos, top

    m_redis, m_hbase = medir(desde_redis), medir(desde_hbase)
    ult_r, top_r = m_redis.resultado
    ult_h, top_h = m_hbase.resultado

    # Último por LLEGADA = primera aparición de cada estación recorriendo el
    # stream hacia atrás (el ID del stream es el reloj de llegada).
    por_llegada = {}
    for sid, c in r.xrevrange(config.STREAM, count=config.MAXLEN):
        por_llegada.setdefault(c["estacion_id"], (sid, int(c["ts"]), c["valor"]))
    distintas = [
        {"estacion": e,
         "por_medicion": {"ts": iso(ult_r[e][0]), "valor": ult_r[e][1]},
         "por_llegada": {"stream_id": sid, "ts": iso(ts), "valor": v,
                         "atraso_s": round((ult_r[e][0] - ts) / 1000, 1)}}
        for e, (sid, ts, v) in sorted(por_llegada.items()) if e in ult_r and ts != ult_r[e][0]
    ]
    tabla = pd.DataFrame(
        [{"estacion": e, "ts_medicion": iso(ts), "valor": v,
          "coincide_hbase": ult_h.get(e) == (ts, v)} for e, (ts, v) in ult_r.items()])
    return {
        "ultimos": tabla.to_dict("records"),
        "top5_redis": top_r, "top5_hbase": top_h,
        "coinciden": ult_r == ult_h and [e for e, _ in top_r] == [e for e, _ in top_h],
        "medicion_vs_llegada": distintas,
        "tiempos": {"redis": m_redis.resumen(), "hbase": m_hbase.resumen()},
        "veces_mas_lento_hbase": round(m_hbase.mediana_ms / m_redis.mediana_ms, 1),
        "interpretacion": (
            "Redis responde con 1 viaje de red (pipeline de 16 HGETALL + 1 ZREVRANGE) sobre "
            "estructuras ya calculadas; HBase necesita 16 scans reversos (un seek por estación) y "
            "ordenar en el cliente. Un dashboard que refresca cada segundo usa Redis; HBase "
            "queda para la historia. La estación cuyo último por llegada difiere del último por "
            "medición recibió una lectura tardía al final de la hora: si el tablero usara el "
            "reloj de llegada, retrocedería."),
    }


# =========================================================================== P2
def p2(r) -> dict:
    """Ventana de 10 min — promedio de flujo desde ventana:* (solo Redis)."""
    cat = _estaciones(r)
    flujo = sorted(e for t, es in cat.items() if datos.es_flujo(t) for e in es)

    def consulta():
        ancla = int(r.get(K.ts_max))
        p = r.pipeline(transaction=False)
        for e in flujo:
            # (ancla - 10 min, ancla]: '(' = extremo abierto
            p.zrangebyscore(K.ventana(e), f"({ancla - config.VENTANA_MS}", ancla)
        out = {}
        for e, miembros in zip(flujo, p.execute()):
            valores = [float(m.split(":", 1)[1]) for m in miembros]
            out[e] = {"n": len(valores), "promedio": round(statistics.fmean(valores), 2) if valores else None}
        return ancla, out

    m = medir(consulta)
    ancla, promedios = m.resultado

    # MEMORY USAGE con y sin recorte: se construye una ventana SIN recortar con
    # toda la hora en vivo (desde el stream) y se compara.
    memoria = []
    stream = r.xrange(config.STREAM, count=config.MAXLEN)
    for e in flujo:
        k_tmp = f"exp:ventana_sin_recorte:{e}"
        r.delete(k_tmp)
        p = r.pipeline(transaction=False)
        for _, c in stream:
            if c["estacion_id"] == e:
                p.zadd(k_tmp, {f"{c['ts']}:{c['valor']}": int(c["ts"])})
        p.execute()
        con, sin = r.memory_usage(K.ventana(e), samples=0), r.memory_usage(k_tmp, samples=0)
        n_con, n_sin = r.zcard(K.ventana(e)), r.zcard(k_tmp)
        memoria.append({"estacion": e, "entradas_10min": n_con, "bytes_10min": con,
                        "codificacion_10min": r.object("encoding", K.ventana(e)),
                        "entradas_1h": n_sin, "bytes_1h_sin_recorte": sin,
                        "codificacion_1h": r.object("encoding", k_tmp),
                        "bytes_por_entrada_1h": round(sin / max(n_sin, 1), 1)})
        r.delete(k_tmp)
    df = pd.DataFrame(memoria)
    b_ent = df["bytes_por_entrada_1h"].mean()
    ent_dia = df["entradas_1h"].mean() * 24
    return {
        "ancla": iso(ancla), "promedios_10min": promedios,
        "tiempo": m.resumen(), "memoria": memoria,
        "extrapolacion_sin_recorte": {
            "bytes_por_entrada": round(b_ent, 1),
            "MB_por_estacion_por_dia": round(b_ent * ent_dia / 2**20, 2),
            "MB_por_estacion_por_anio": round(b_ent * ent_dia * 365 / 2**20, 1),
        },
        "interpretacion": (
            "El score = ts de medición hace que 'los últimos 10 minutos' sea un ZRANGEBYSCORE "
            "O(log n + k) y que recortar sea un ZREMRANGEBYSCORE; las tardías se insertan en su "
            "lugar por orden de medición, no de llegada. Con ~75 entradas la ventana cabe en "
            "codificación listpack (compacta); sin recorte el set crece lineal con el tiempo, pasa "
            "a skiplist (más bytes por entrada) y Redis, que vive en RAM, se queda sin memoria."),
    }


# =========================================================================== P3
def p3(conn) -> dict:
    """Un rango — CC-001, 04-nov [08:00, 10:00): scan por rango vs tabla completa + filtro."""
    est = "CC-001"
    desde, hasta = claves.ms_chile("2026-11-04T08:00"), claves.ms_chile("2026-11-04T10:00")
    lo, hi = claves.rango_estacion(est, desde, hasta)
    t = conn.table(config.TABLA_LECTURAS)

    def agregar(filas):
        cuartos = defaultdict(list)
        n = 0
        for row, d in filas:
            ts = _ts_de_clave(row, 1)
            cuartos[(ts - desde) // 900_000].append(float(d[COL_V]))
            n += 1
        return n, {iso(desde + q * 900_000)[11:16]: round(statistics.fmean(v), 2)
                   for q, v in sorted(cuartos.items())}

    def por_rango():
        return agregar(t.scan(row_start=lo, row_stop=hi, columns=[COL_V]))

    filtro = f"RowFilter(>=, 'binary:{lo.decode()}') AND RowFilter(<, 'binary:{hi.decode()}')"

    def tabla_completa():
        return agregar(t.scan(columns=[COL_V], filter=filtro, batch_size=10_000))

    m_rango, m_full = medir(por_rango), medir(tabla_completa)
    assert m_rango.resultado == m_full.resultado, "los dos métodos deben dar lo mismo"
    n, cuartos = m_rango.resultado
    factor = m_full.mediana_ms / m_rango.mediana_ms
    return {
        "estacion": est, "rango": [iso(desde), iso(hasta)], "lecturas": n,
        "promedio_por_cuarto_de_hora": cuartos,
        "tiempos": {"scan_rango": m_rango.resumen(), "tabla_completa_filtro": m_full.resumen()},
        "veces_mas_lento": round(factor, 1),
        "filtro": filtro,
        "interpretacion": (
            f"El scan por rango salta directo (seek) a '{lo.decode()}' y lee solo ~{n} filas de una "
            f"región; el filtro obliga al RegionServer a leer las ~853 mil filas de las 4 regiones para "
            f"devolver las mismas {n}. Con 5 años (365x más datos) el rango cuesta lo mismo "
            f"(depende del resultado, no de la tabla) y el recorrido completo ~365x más: de "
            f"{m_full.mediana_ms / 1000:.1f} s a ~{m_full.mediana_ms * 365 / 60000:.0f} min."),
    }


# =========================================================================== P4
def p4(r, conn) -> dict:
    """Dos patrones de acceso — PM2.5 05-nov [07:00, 09:00) por estación de calidad del aire."""
    tipo = config.TIPO_AIRE
    desde, hasta = claves.ms_chile("2026-11-05T07:00"), claves.ms_chile("2026-11-05T09:00")
    t_lect, t_tipo = conn.table(config.TABLA_LECTURAS), conn.table(config.TABLA_TIPO)

    def resumir(acum):
        out = {}
        for e, v in sorted(acum.items()):
            maximo = max(x for _, x in v)
            out[e] = {"n": len(v), "promedio": round(statistics.fmean(x for _, x in v), 2),
                      "maximo": maximo,
                      # si el máximo se repite: la PRIMERA ocurrencia por medición
                      "hora_maximo": iso(min(ts for ts, x in v if x == maximo))}
        return out

    def a_catalogo_mas_lecturas():
        estaciones = sorted(r.smembers(K.tipo(tipo)))        # Redis decide QUÉ estaciones
        acum = defaultdict(list)
        for e in estaciones:                                   # HBase: un rango por estación
            lo, hi = claves.rango_estacion(e, desde, hasta)
            for row, d in t_lect.scan(row_start=lo, row_stop=hi, columns=[COL_V]):
                acum[e].append((_ts_de_clave(row, 1), float(d[COL_V])))
        return resumir(acum)

    def b_lecturas_tipo():
        lo, hi = claves.rango_tipo(tipo, desde, hasta)        # UN solo scan
        acum = defaultdict(list)
        for row, d in t_tipo.scan(row_start=lo, row_stop=hi, columns=[COL_V]):
            _, ts, e = row.decode().split("#")
            acum[e].append((int(ts), float(d[COL_V])))
        return resumir(acum)

    m_a, m_b = medir(a_catalogo_mas_lecturas), medir(b_lecturas_tipo)
    assert m_a.resultado == m_b.resultado, "las dos rutas deben dar lo mismo"

    meta = json.loads(conn.table(config.TABLA_META).row(b"carga_historica")[b"d:info"])
    disco = {n: hbase_util.tamano_en_disco(n) for n in (config.TABLA_LECTURAS, config.TABLA_TIPO)}
    return {
        "resultado": m_a.resultado,
        "tiempos": {"a_catalogo_redis_mas_lecturas": m_a.resumen(), "b_lecturas_tipo": m_b.resumen()},
        "costo_segunda_tabla": {
            "escrituras": "x2: cada evento son 2 puts (uno por tabla)",
            "segundos_carga_lecturas": meta["segundos_lecturas"],
            "segundos_carga_lecturas_tipo": meta["segundos_lecturas_tipo"],
            "bytes_lecturas": disco[config.TABLA_LECTURAS],
            "bytes_lecturas_tipo": disco[config.TABLA_TIPO],
            "espacio_extra_pct": round(100 * disco[config.TABLA_TIPO] / disco[config.TABLA_LECTURAS], 1),
        },
        "interpretacion": (
            "(a) son N scans cortos (N = estaciones de calidad del aire) más 1 SMEMBERS; (b) es un "
            "solo scan contiguo. La diferencia crece con el número de estaciones del tipo. La segunda "
            "tabla duplica escrituras y ~duplica disco; si el segundo put falla, el consumidor no hace "
            "XACK y el lote se reintenta: la clave idempotente reescribe la primera tabla sin duplicar "
            "(consistencia eventual entre tablas). No vale la pena si la consulta por tipo es rara, si "
            "hay pocas estaciones por tipo, o si la carga de escritura es el cuello de botella."),
    }


# =========================================================================== P5
def p5(conn, espera_jmx_s: int = int(os.getenv("T2_ESPERA_JMX_S", "25"))) -> dict:
    """Hotspotting y salting — la hora en vivo en 3 tablas con distinta clave."""
    vivo = datos.en_vivo()
    cat = datos.catalogo(vivo)
    estaciones = sorted(e for es in cat.values() for e in es)
    disenos = {
        config.TABLA_HS_EST_TS: (claves.splits_estaciones(estaciones), claves.clave_lecturas),
        config.TABLA_HS_TS_EST: (claves.splits_dias(config.T0, 6), claves.clave_ts_estacion),
        config.TABLA_HS_SALT: (claves.splits_salt(), claves.clave_salt),
    }
    escritura_s = {}
    for nombre, (splits, fclave) in disenos.items():
        hbase_util.crear_tabla(conn, nombre, splits, versiones=1)
        t = conn.table(nombre)
        t0 = time.perf_counter()
        with t.batch(batch_size=200) as b:            # lotes chicos: imita el flujo en vivo
            for l in vivo:
                b.put(fclave(l.estacion, l.ts_ms), escritura.celdas_hs(l.estacion, l.tipo, l.valor_txt))
        escritura_s[nombre] = round(time.perf_counter() - t0, 3)

    print(f"  [P5] esperando {espera_jmx_s} s a que JMX publique las métricas por región...", flush=True)
    time.sleep(espera_jmx_s)
    filas_tabla = []
    for nombre in disenos:
        t = conn.table(nombre)
        jmx = hbase_util.metricas_regiones(nombre)
        for i, reg in enumerate(hbase_util.regiones(t)):
            enc = hbase_util.encoded_name(reg["name"])
            met = jmx.get(enc, {})
            filas_tabla.append({
                "tabla": nombre, "region": i,
                "start_key": reg["start_key"].decode(errors="replace") or "(inicio)",
                "escrituras_jmx": met.get("writeRequestCount", met.get("putCount")),
                "filas_scan": hbase_util.contar_filas(t, reg["start_key"] or None, reg["end_key"] or None),
            })
    df = pd.DataFrame(filas_tabla)

    desde, hasta = claves.ms_chile("2026-11-07T00:10"), claves.ms_chile("2026-11-07T00:20")

    def leer_est_ts():
        t = conn.table(config.TABLA_HS_EST_TS)
        return sum(1 for e in estaciones for _ in t.scan(row_start=claves.clave_lecturas(e, desde),
                                                         row_stop=claves.clave_lecturas(e, hasta),
                                                         columns=[COL_V]))

    def leer_ts_est():
        t = conn.table(config.TABLA_HS_TS_EST)
        return sum(1 for _ in t.scan(row_start=claves.ts13(desde).encode(),
                                     row_stop=claves.ts13(hasta).encode(), columns=[COL_V]))

    def leer_salt():
        t = conn.table(config.TABLA_HS_SALT)
        return sum(1 for s in range(config.N_SALT)
                   for _ in t.scan(row_start=f"{s}#{claves.ts13(desde)}".encode(),
                                   row_stop=f"{s}#{claves.ts13(hasta)}".encode(), columns=[COL_V]))

    lecturas = {"estacion#ts (16 rangos)": medir(leer_est_ts),
                "ts#estacion (1 rango)": medir(leer_ts_est),
                "salt#ts#estacion (4 rangos)": medir(leer_salt)}
    conteos = {k: m.resultado for k, m in lecturas.items()}
    assert len(set(conteos.values())) == 1, f"los tres diseños deben devolver lo mismo: {conteos}"
    pivote = df.pivot(index="tabla", columns="region", values="escrituras_jmx")
    pivote_filas = df.pivot(index="tabla", columns="region", values="filas_scan")
    return {
        "escrituras_por_region": filas_tabla,
        "tabla_3xregiones_jmx": pivote.astype(object).where(pivote.notna(), "-").to_dict("index"),
        "tabla_3xregiones_filas": pivote_filas.astype(object).where(pivote_filas.notna(), "-").to_dict("index"),
        "salt_por_estacion": {e: claves.salt(e) for e in estaciones},
        "segundos_escritura": escritura_s,
        "filas_consulta": conteos,
        "tiempos_lectura": {k: m.resumen() for k, m in lecturas.items()},
        "interpretacion": (
            "ts#estacion manda TODAS las escrituras de la hora en vivo a la región del 07-nov "
            "(hotspot: una sola región/RegionServer trabaja), pero la consulta por tiempo es 1 rango. "
            "estacion#ts reparte las escrituras en las 4 regiones y la consulta 'todas las estaciones "
            "en 10 minutos' cuesta 16 rangos. salt#ts#estacion reparte en 4 cubetas (tan parejo como "
            "reparta el CRC32 a las 16 estaciones) y la consulta cuesta 4 rangos. Con un solo "
            "RegionServer standalone el hotspot casi no se nota en tiempo: se ve en el conteo de JMX; "
            "en un cluster, la región caliente limita el throughput de todo el sistema."),
    }


# =========================================================================== P6
def p6(r, conn) -> dict:
    """La propia — ¿qué tan raro es el ahora?

    Para las 5 estaciones con más flujo AHORA (Redis: ranking:flujo + ultimo:*
    deciden QUÉ estaciones y el UMBRAL), ¿cuántas veces en los 5 días de historia
    esa estación midió un flujo >= al actual, qué percentil es el ahora, y
    CUÁNDO fue la última vez antes de la hora en vivo? (HBase: un rango por
    estación sobre ``lecturas``).

    Ninguno de los dos motores puede solo: Redis no guarda historia (la ventana
    es de 10 min) y HBase no sabe cuál es "el top de ahora" sin 16 scans
    reversos + ordenar (P1). Combinados: O(log n) en Redis + 5 rangos en HBase.
    """
    inicio_vivo = claves.ms(config.T0) + 5 * 86_400_000         # 07-nov 00:00
    t_lect = conn.table(config.TABLA_LECTURAS)

    def consulta():
        top = r.zrevrange(K.ranking, 0, 4, withscores=True)            # QUÉ (Redis)
        p = r.pipeline(transaction=False)
        for e, _ in top:
            p.hget(K.ultimo(e), "ts")
        ts_ahora = p.execute()
        out = []
        for (e, v), ts in zip(top, ts_ahora):
            n_tot = n_mayor = 0
            ultima = None
            lo, hi = claves.rango_estacion(e, 0, inicio_vivo)           # CUÁNDO (HBase)
            for row, d in t_lect.scan(row_start=lo, row_stop=hi, columns=[COL_V], batch_size=5000):
                n_tot += 1
                if float(d[COL_V]) >= v:
                    n_mayor += 1
                    ultima = _ts_de_clave(row, 1)
            out.append({
                "estacion": e, "valor_ahora": v, "ts_ahora": iso(int(ts)),
                "lecturas_historicas": n_tot, "veces_mayor_o_igual": n_mayor,
                "percentil_ahora": round(100 * (1 - n_mayor / n_tot), 1) if n_tot else None,
                "ultima_vez_antes": iso(ultima) if ultima else "nunca en 5 días",
                "horas_desde_ultima_vez": round((int(ts) - ultima) / 3_600_000, 1) if ultima else None,
            })
        return out

    m = medir(consulta)
    return {
        "pregunta": ("Para las 5 estaciones con más flujo ahora: ¿cuántas veces en la historia "
                     "midieron un flujo >= al actual, qué percentil es el ahora y cuándo fue la "
                     "última vez antes de la hora en vivo?"),
        "reparto": "Redis = QUÉ (top 5 y umbral, ya calculados); HBase = CUÁNDO (historia por estación).",
        "resultado": m.resultado, "tiempo": m.resumen(),
        "interpretacion": (
            "Estar en el top 5 de ahora no implica ser excepcional: el percentil histórico separa "
            "a la estación que de verdad está en un extremo de su propia historia de la que solo "
            "es alta porque su nivel normal es alto. Sin Redis habría que reconstruir el top con "
            "16 scans reversos; sin HBase no hay con qué comparar."),
    }


# =========================================================================== main
def _md(res: dict) -> str:
    """Resumen en markdown para pegar en el informe."""
    hw = res["hardware"]
    L = [f"# Resultados Parte C\n\nMedido en: {hw.get('cpu')} · {hw.get('nucleos')} núcleos · "
         f"{hw.get('ram_gb', '?')} GB RAM · {hw.get('sistema')}\n",
         "Tiempos: mediana de 5 corridas tras 1 de calentamiento (ms).\n",
         "| Pregunta | Variante | Mediana (ms) | Calentamiento (ms) |", "|---|---|---:|---:|"]

    def fila(p, nombre, t):
        L.append(f"| {p} | {nombre} | {t['mediana_ms']:.2f} | {t['calentamiento_ms']:.2f} |")
    if "P1" in res:
        for k, t in res["P1"]["tiempos"].items():
            fila("P1", k, t)
    if "P2" in res:
        fila("P2", "redis ventana:*", res["P2"]["tiempo"])
    if "P3" in res:
        for k, t in res["P3"]["tiempos"].items():
            fila("P3", k, t)
    if "P4" in res:
        for k, t in res["P4"]["tiempos"].items():
            fila("P4", k, t)
    if "P5" in res:
        for k, t in res["P5"]["tiempos_lectura"].items():
            fila("P5", k, t)
    if "P6" in res:
        fila("P6", "redis top5 + hbase historia", res["P6"]["tiempo"])
    for p in ("P1", "P2", "P3", "P4", "P5", "P6"):
        if p in res:
            L.append(f"\n## {p}\n\n```json\n{json.dumps(res[p], indent=2, ensure_ascii=False, default=str)}\n```")
    return "\n".join(L)


def main(argv: list[str]) -> None:
    pedidas = {a.upper() for a in argv} or {"P1", "P2", "P3", "P4", "P5", "P6"}
    r, conn = redis_util.conectar(), hbase_util.conectar()
    res: dict = {"hardware": hardware()}
    fns = {"P1": lambda: p1(r, conn), "P2": lambda: p2(r), "P3": lambda: p3(conn),
           "P4": lambda: p4(r, conn), "P5": lambda: p5(conn), "P6": lambda: p6(r, conn)}
    for p in ("P1", "P2", "P3", "P4", "P5", "P6"):
        if p in pedidas:
            print(f"\n######## {p} ########", flush=True)
            res[p] = fns[p]()
            print(json.dumps(res[p], indent=2, ensure_ascii=False, default=str)[:4000])
    config.RESULTADOS.mkdir(exist_ok=True)
    (config.RESULTADOS / "preguntas.json").write_text(json.dumps(res, indent=2, ensure_ascii=False, default=str))
    (config.RESULTADOS / "preguntas.md").write_text(_md(res))
    print(f"\n[preguntas] escrito {config.RESULTADOS / 'preguntas.md'}")


if __name__ == "__main__":
    main(sys.argv[1:])
