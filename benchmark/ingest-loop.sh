#!/usr/bin/env bash
# B2 — dispara N execuções do Job C (datamart) para simular ingestões em batch com
# chaves sobrepostas e observar o comportamento de merge do ReplacingMergeTree.
# Entre cada batch, imprime as métricas de merge (benchmark/merge-metrics.sql).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_lib.sh"

N="${N:-5}"
SLEEP="${SLEEP:-15}"
NS="datamart"
CH_POD="$(kubectl -n "$NS" get pod -l clickhouse.altinity.com/chi=datamart -o jsonpath='{.items[0].metadata.name}')"

ch() { kubectl -n "$NS" exec -i "$CH_POD" -- clickhouse-client --user datamart --password datamart123 "$@"; }

for i in $(seq 1 "$N"); do
  echo "==================== BATCH $i/$N ===================="
  submit_spark "$ROOT/infra/spark/sparkapplication-ingest.yaml" "datamart-fact-200-cep"
  echo "---- métricas após batch $i ----"
  ch --multiquery < "$ROOT/benchmark/merge-metrics.sql" || true
  sleep "$SLEEP"
done

echo "==================== CONSOLIDAÇÃO (OPTIMIZE FINAL) ===================="
time ch --query "OPTIMIZE TABLE datamart.fact_200_cep FINAL"
ch --query "SELECT count() AS total, uniqExact(hk_business_id) AS chaves_unicas FROM datamart.fact_200_cep"
ch --query "SELECT count() AS total_final FROM datamart.fact_200_cep FINAL"
