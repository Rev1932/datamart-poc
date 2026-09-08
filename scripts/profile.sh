#!/usr/bin/env bash
# quiesce/resume: libera CPU e RAM do nó antes de medir latência de leitura.
# Sem o quiesce, o p95 do benchmark mede o scheduler do Airflow junto.
set -euo pipefail

NS_DATA=datamart
NS_AIR=airflow
STATE_FILE="${DATAMART_POC_STATE:-$HOME/.datamart-poc-profile.state}"

# ns:tipo/nome:replicas_default
WORKLOADS=(
  "$NS_AIR:statefulset/airflow-scheduler:1"
  "$NS_AIR:deployment/airflow-webserver:1"
  "$NS_DATA:statefulset/mongodb:1"
)

die() { echo "ERRO: $*" >&2; exit 1; }

check_spark_idle() {
  local pending
  pending="$(kubectl -n "$NS_DATA" get sparkapplication \
    -o jsonpath='{range .items[*]}{.metadata.name}{"="}{.status.applicationState.state}{"\n"}{end}' \
    2>/dev/null | grep -vE '=(COMPLETED|FAILED|SUBMISSION_FAILED)$' || true)"

  if [[ -n "$pending" ]]; then
    echo "SparkApplication em estado não-terminal:" >&2
    echo "$pending" >&2
    die "aguarde a conclusão antes do quiesce — derrubar o Airflow agora deixa a carga órfã."
  fi
}

current_replicas() {
  kubectl -n "$1" get "$2" -o jsonpath='{.spec.replicas}' 2>/dev/null || true
}

quiesce() {
  check_spark_idle
  : > "$STATE_FILE"
  for w in "${WORKLOADS[@]}"; do
    IFS=: read -r ns obj _ <<< "$w"
    local n; n="$(current_replicas "$ns" "$obj")"
    [[ -n "$n" ]] || { echo ">> $ns/$obj não existe — ignorando"; continue; }
    echo "$ns:$obj:$n" >> "$STATE_FILE"
    echo ">> escalando $ns/$obj para 0 (era $n)"
    kubectl -n "$ns" scale "$obj" --replicas=0
  done
  echo ">> quiesce concluído. Estado salvo em $STATE_FILE"
}

resume() {
  local src=()
  if [[ -s "$STATE_FILE" ]]; then
    mapfile -t src < "$STATE_FILE"
  else
    echo ">> $STATE_FILE ausente — restaurando com as réplicas padrão."
    src=("${WORKLOADS[@]}")
  fi

  for w in "${src[@]}"; do
    IFS=: read -r ns obj n <<< "$w"
    [[ -n "${n:-}" ]] || n=1
    kubectl -n "$ns" get "$obj" >/dev/null 2>&1 || { echo ">> $ns/$obj não existe — ignorando"; continue; }
    echo ">> escalando $ns/$obj para $n"
    kubectl -n "$ns" scale "$obj" --replicas="$n"
  done
  echo ">> resume concluído."
}

case "${1:-}" in
  quiesce) quiesce ;;
  resume)  resume ;;
  *) echo "uso: $0 quiesce|resume" >&2; exit 1 ;;
esac
