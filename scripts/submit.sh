#!/usr/bin/env bash
# Submete UMA SparkApplication parametrizada — sem precisar de um YAML por combinação.
#
# Uso:
#   scripts/submit.sh <pipeline> <config-name> <table-name> <chave-pk...> [flags]
#
# Exemplos:
#   scripts/submit.sh datamart      unipac fact_200_cep andon_peso_id
#   scripts/submit.sh datamart      unipac fact_200_cep andon_peso_id nr_ordem_producao
#   scripts/submit.sh bronze_silver unipac dw_andon_peso id
#
# Flags opcionais (fallback: variáveis de ambiente DRY, INSTANCES, DRIVER_MEM,
# EXEC_MEM, EXEC_CORES — flag tem precedência sobre elas):
#   --dry                  só imprime o manifesto (não aplica)
#   --instances N          réplicas do executor (default: 2)
#   --driver-mem MEM       memória do driver, ex.: 1500m (default: 1500m)
#   --exec-mem MEM         memória do executor, ex.: 2g (default: 2g)
#   --exec-cores N         cores do executor (default: 1)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_lib.sh"

usage() {
  cat <<EOF
Uso: scripts/submit.sh <pipeline> <config-name> <table-name> <chave-pk...> [flags]

  --dry                só imprime o manifesto (não aplica)
  --instances N        réplicas do executor (default: \$INSTANCES ou 2)
  --driver-mem MEM      memória do driver, ex.: 1500m (default: \$DRIVER_MEM ou 1500m)
  --exec-mem MEM        memória do executor, ex.: 2g (default: \$EXEC_MEM ou 2g)
  --exec-cores N        cores do executor (default: \$EXEC_CORES ou 1)
  -h, --help            mostra esta ajuda
EOF
}

DRY="${DRY:-0}"
INSTANCES="${INSTANCES:-2}"
DRIVER_MEM="${DRIVER_MEM:-1500m}"
EXEC_MEM="${EXEC_MEM:-2g}"
EXEC_CORES="${EXEC_CORES:-1}"

ARGV=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry) DRY=1; shift ;;
    --instances) INSTANCES="$2"; shift 2 ;;
    --driver-mem) DRIVER_MEM="$2"; shift 2 ;;
    --exec-mem) EXEC_MEM="$2"; shift 2 ;;
    --exec-cores) EXEC_CORES="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) ARGV+=("$1"); shift ;;
  esac
done
set -- "${ARGV[@]}"

if [[ $# -lt 4 ]]; then
  echo "uso: $0 <pipeline> <config-name> <table-name> <chave-pk...>" >&2
  exit 2
fi

PIPELINE="$1"; CONFIG="$2"; TABLE="$3"; shift 3
PKS=("$@")

# Nome do recurso: só minúsculas/dígitos/'-'. Underscores viram '-'.
NAME="$(echo "${PIPELINE}-${TABLE}" | tr '_' '-' | tr '[:upper:]' '[:lower:]')"

# arguments como lista JSON: um token por item (regra do argparse).
ARGS="\"--pipeline\",\"${PIPELINE}\",\"--config-name\",\"${CONFIG}\",\"--table-name\",\"${TABLE}\",\"--chave-pk\""
for c in "${PKS[@]}"; do ARGS="${ARGS},\"${c}\""; done

MANIFEST="$(cat <<YAML
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: ${NAME}
  namespace: datamart
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: datamart-spark:poc
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/spark/app/main.py
  sparkVersion: "3.5.1"
  arguments: [${ARGS}]
  restartPolicy:
    type: Never
  # Jars já vêm assados na imagem em /opt/spark/jars (ver images/spark/Dockerfile).
  driver:
    cores: 1
    memory: "${DRIVER_MEM}"
    serviceAccount: spark
    env:
      - name: ENV
        value: "local"
    envFrom:
      - secretRef:
          name: spark-secrets
  executor:
    cores: ${EXEC_CORES}
    instances: ${INSTANCES}
    memory: "${EXEC_MEM}"
    env:
      - name: ENV
        value: "local"
    envFrom:
      - secretRef:
          name: spark-secrets
YAML
)"

if [[ "$DRY" == "1" ]]; then
  echo "$MANIFEST"
  exit 0
fi

TMP="$(mktemp)"
printf '%s\n' "$MANIFEST" > "$TMP"
submit_spark "$TMP" "$NAME"
rm -f "$TMP"
