#!/usr/bin/env bash
# Sobe toda a stack da POC no minikube: cluster -> imagem Spark -> operators ->
# MinIO -> ClickHouse -> RBAC -> PostgreSQL -> MongoDB -> Airflow -> tabelas de fato.
# Rode a partir da RAIZ do repositório.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# submit_spark: (re)submete uma SparkApplication e aguarda estado terminal.
source "$ROOT/scripts/_lib.sh"

echo "==> 1/12 cluster local"
bash cluster/minikube-up.sh

echo "==> 2/12 imagens (honeycomb + jars do ClickHouse, e Airflow)"
# Nao reconstroi o honeycomb: a imagem publicada ja tem Spark, Delta, hadoop-aws e o app.
# O Dockerfile so acrescenta os jars do connector.
docker image inspect hub.datawake.cloud/dw-dados/honeycomb:latest >/dev/null 2>&1 \
  || docker pull hub.datawake.cloud/dw-dados/honeycomb:latest
docker build -f images/spark/Dockerfile -t honeycomb:poc images/spark/
minikube image load honeycomb:poc

# Airflow: a imagem de producao ja tem 2.11.2 e pymongo; nada a construir.
minikube image ls 2>/dev/null | grep -q datawake-airflow \
  || minikube image load dw-dados/datawake-airflow:0.1.0

echo "==> 3/12 namespaces"
kubectl apply -f infra/00-namespaces.yaml

echo "==> 4/12 MinIO"
kubectl apply -f infra/minio/minio-standalone.yaml
kubectl -n datamart rollout status statefulset/minio --timeout=300s
kubectl apply -f infra/minio/bucket-provision-job.yaml
kubectl -n datamart wait --for=condition=complete job/minio-provision --timeout=180s

echo "==> 5/12 Spark Operator + RBAC/secrets"
helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator \
  -n spark-operator --create-namespace -f infra/spark/operator-values.yaml
kubectl apply -f infra/spark/spark-rbac.yaml
kubectl apply -f infra/spark/spark-secrets.yaml

echo "==> 6/12 ClickHouse operator + instância"
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/release-0.24.0/deploy/operator/clickhouse-operator-install-bundle.yaml
kubectl apply -f infra/clickhouse/chi-datamart.yaml
echo "aguardando CHI (status=Completed)..."
for _ in $(seq 1 60); do
  st="$(kubectl -n datamart get chi datamart -o jsonpath='{.status.status}' 2>/dev/null || true)"
  [[ "$st" == "Completed" ]] && break
  sleep 10
done
kubectl -n datamart get chi datamart

echo "==> 7/12 RBAC multi-tenant (database/role/user/profile/quota por tenant)"
kubectl -n datamart create configmap ch-rbac-tpl \
  --from-file=ddl/rbac/10_tenant.sql.tpl --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/clickhouse/job-rbac.yaml
kubectl -n datamart wait --for=condition=complete job/ch-rbac --timeout=300s
bash scripts/verify-rbac.sh

echo "==> 8/12 PostgreSQL (braço de comparação)"
kubectl apply -f infra/postgres/postgres-statefulset.yaml
kubectl -n datamart rollout status statefulset/postgres --timeout=300s

echo "==> 9/12 MongoDB (control plane das DAGs)"
kubectl apply -f infra/mongodb/mongodb-statefulset.yaml
kubectl -n datamart rollout status statefulset/mongodb --timeout=300s
kubectl -n datamart create configmap mongo-seed-docs \
  --from-file=airflow/mongo-seed/k8s_acme.json \
  --from-file=airflow/mongo-seed/k8s_acme_gold.json \
  --from-file=airflow/mongo-seed/k8s_globex.json \
  --from-file=airflow/mongo-seed/k8s_globex_gold.json \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/mongodb/job-seed.yaml
kubectl -n datamart wait --for=condition=complete job/mongo-seed --timeout=300s

echo "==> 10/12 Airflow"
kubectl apply -f infra/airflow/postgres-metadata.yaml
kubectl -n airflow rollout status statefulset/airflow-postgres --timeout=300s
kubectl -n airflow create configmap airflow-dags \
  --from-file=airflow/dags/ --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/airflow/rbac-spark.yaml
helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install airflow apache-airflow/airflow --version 1.16.0 \
  -n airflow -f infra/airflow/values.yaml --timeout 15m
kubectl -n airflow rollout status statefulset/airflow-scheduler --timeout=420s
kubectl -n airflow rollout status deploy/airflow-webserver --timeout=300s
# O scheduler e StatefulSet neste chart, nao Deployment.
kubectl -n airflow exec statefulset/airflow-scheduler -c scheduler -- \
  airflow dags list-import-errors

echo "==> 11/12 smoke do connector ClickHouse"
kubectl -n datamart create configmap ch-smoke \
  --from-file=infra/spark/smoke/smoke_clickhouse.py --dry-run=client -o yaml | kubectl apply -f -
# submit_spark e nao `kubectl apply`: um CR em estado terminal de uma execucao
# anterior faz o apply virar no-op, e a espera leria o estado velho.
submit_spark infra/spark/sparkapplication-smoke-clickhouse.yaml smoke-clickhouse \
  || { echo "ERRO: smoke do connector falhou." >&2; exit 1; }

echo "==> 12/12 tabela de fato por tenant"
DDL_FATO=ddl/clickhouse/01_fact_200_cep.sql
if [[ -f "$DDL_FATO" ]]; then
  POD="$(kubectl -n datamart get pod -l clickhouse.altinity.com/chi=datamart -o jsonpath='{.items[0].metadata.name}')"
  ADMIN_PWD="$(kubectl -n datamart get secret ch-creds -o jsonpath='{.data.dm_admin}' | base64 -d)"
  for t in acme globex; do
    sed "s/{{TENANT}}/${t}/g" "$DDL_FATO" | \
      kubectl -n datamart exec -i "$POD" -- \
        clickhouse-client --user dm_admin --password "$ADMIN_PWD" --multiquery
  done
else
  echo "    $DDL_FATO ainda não existe (entregue em E2/T2.3) — pulando."
fi

echo "==> stack pronta. Próximos: scripts/load-bronze.sh --validate, DAGs no Airflow"
