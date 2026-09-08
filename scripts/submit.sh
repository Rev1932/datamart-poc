#!/usr/bin/env bash
# Submete UMA SparkApplication parametrizada — sem precisar de um YAML por combinação.
#
# Uso:
#   scripts/submit.sh <pipeline> <config-name> <table-name> <chave-pk...>
#
# Exemplos:
#   scripts/submit.sh datamart      unipac fact_200_cep andon_peso_id
#   scripts/submit.sh datamart      unipac fact_200_cep andon_peso_id nr_ordem_producao
#   scripts/submit.sh bronze_silver unipac dw_andon_peso id
#
# Variáveis opcionais:
#   DRY=1   -> só imprime o manifesto (não aplica)
#   INSTANCES=2  DRIVER_MEM=1500m  EXEC_MEM=2g  EXEC_CORES=1
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_lib.sh"

if [[ $# -lt 4 ]]; then
  echo "uso: $0 <pipeline> <config-name> <table-name> <chave-pk...>" >&2
  exit 2
fi

PIPELINE="$1"; CONFIG="$2"; TABLE="$3"; shift 3
PKS=("$@")

INSTANCES="${INSTANCES:-2}"
DRIVER_MEM="${DRIVER_MEM:-1500m}"
EXEC_MEM="${EXEC_MEM:-2g}"
EXEC_CORES="${EXEC_CORES:-1}"

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

if [[ "${DRY:-0}" == "1" ]]; then
  echo "$MANIFEST"
  exit 0
fi

TMP="$(mktemp)"
printf '%s\n' "$MANIFEST" > "$TMP"
submit_spark "$TMP" "$NAME"
rm -f "$TMP"
