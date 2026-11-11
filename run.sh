#!/usr/bin/env bash
# =============================================================================
# Tarea 2 — Redis + HBase: del stream a la historia
# Un solo comando, de punta a punta, en una máquina limpia:
#
#   ./run.sh                    # todo: infra + carga + vivo + verificación + preguntas + D.1 + D.2
#   ./run.sh --sin-experimentos # sin la Parte D
#   ./run.sh --desde-cero       # además borra los volúmenes (fuerza recarga del histórico)
#
# Requisitos: Docker con compose v2 (>= 4 GB de RAM para Docker), python3 >= 3.10.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

EXPERIMENTOS=1
DESDE_CERO=0
for arg in "$@"; do
  case "$arg" in
    --sin-experimentos) EXPERIMENTOS=0 ;;
    --desde-cero)       DESDE_CERO=1 ;;
    *) echo "opción desconocida: $arg"; exit 2 ;;
  esac
done

T_INICIO=$(date +%s)
paso() { printf '\n\033[1;36m==> [%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }

# ---------------------------------------------------------------- 0. chequeos
command -v docker >/dev/null || { echo "Falta docker"; exit 1; }
docker compose version >/dev/null || { echo "Falta docker compose v2"; exit 1; }
PYTHON_SISTEMA=$(command -v python3 || command -v python)
if [[ ! -f t2/generador.py ]]; then
  echo "!! Falta t2/generador.py (copia sin cambios de Laboratorios/06/generador.py)."; exit 1
fi
mkdir -p logs resultados

# ---------------------------------------------------------------- 1. infraestructura
if [[ "$DESDE_CERO" == "1" ]]; then
  paso "Borrando contenedores y volúmenes (desde cero)"
  docker compose down -v --remove-orphans
fi
# Solo se construye si la imagen no existe (la descarga de HBase es lo único que depende de la red).
# Para usar otra versión 2.5.x:  docker compose build --build-arg HBASE_VERSION=2.5.x
if docker image inspect pmd-hbase >/dev/null 2>&1; then
  paso "Imagen pmd-hbase ya construida: se reutiliza"
else
  paso "docker compose build (descarga HBase, solo la primera vez)"
  docker compose build
fi
paso "docker compose up -d --wait  (espera los healthchecks de redis y hbase)"
docker compose up -d --wait

# ---------------------------------------------------------------- 2. python
paso "Ambiente virtual .venv + requirements.txt"
if [[ ! -d .venv ]]; then
  "$PYTHON_SISTEMA" -m venv .venv
fi
# Linux/macOS/WSL: .venv/bin/python · Git Bash en Windows: .venv/Scripts/python.exe
if [[ -x .venv/bin/python ]]; then PY=.venv/bin/python; else PY=.venv/Scripts/python.exe; fi
$PY -m pip install -q --upgrade pip
$PY -m pip install -q -r requirements.txt
export PYTHONUNBUFFERED=1

$PY -m t2.esperar

# ---------------------------------------------------------------- 3. B.1 histórico
paso "B.1 Carga histórica (864.000 eventos -> lecturas, lecturas_tipo)"
$PY -m t2.carga_historica 2>&1 | tee logs/carga_historica.log

# ---------------------------------------------------------------- 4. B.2 en vivo
paso "B.2 Hora en vivo: stream sensores:t2 + grupos ingesta y tablero"
$PY -m t2.preparar_vivo
$PY -m t2.consumidor_tablero --nombre tablero-1 > logs/tablero.log 2>&1 &
PID_TABLERO=$!
$PY -m t2.consumidor_ingesta --nombre ingesta-1 > logs/ingesta.log 2>&1 &
PID_INGESTA=$!
trap 'kill $PID_TABLERO $PID_INGESTA 2>/dev/null || true' EXIT
$PY -m t2.productor 2>&1 | tee logs/productor.log
paso "Esperando a que ambos grupos vacíen el stream"
wait $PID_INGESTA; echo "ingesta terminó: $(tail -n 1 logs/ingesta.log)"
wait $PID_TABLERO; echo "tablero terminó: $(tail -n 2 logs/tablero.log | tr '\n' ' ')"
trap - EXIT

# ---------------------------------------------------------------- 5. B.3 verificación
paso "B.3 Verificación"
VERIF=0
$PY -m t2.verificar 2>&1 | tee logs/verificar.log || VERIF=$?

# ---------------------------------------------------------------- 6. Parte C
paso "Parte C — Preguntas P1..P6 (mediana de 5 corridas)"
$PY -m t2.preguntas 2>&1 | tee logs/preguntas.log

# ---------------------------------------------------------------- 7. Parte D
if [[ "$EXPERIMENTOS" == "1" ]]; then
  paso "D.1 — el consumidor muere entre el put y el XACK"
  $PY -m t2.exp_d1 2>&1 | tee logs/d1.log
  paso "D.2 — Redis se reinicia (AOF vs sin persistencia; extra: SIGKILL con AOF)"
  $PY -m t2.exp_d2 aof sin aof-kill 2>&1 | tee logs/d2.log
fi

T_FIN=$(date +%s)
paso "Listo en $(( (T_FIN - T_INICIO) / 60 )) min $(( (T_FIN - T_INICIO) % 60 )) s. Resultados en resultados/, logs en logs/"
if [[ "$VERIF" != "0" ]]; then
  echo "!! verificar.py reportó diferencias (ver logs/verificar.log)"; exit 1
fi
