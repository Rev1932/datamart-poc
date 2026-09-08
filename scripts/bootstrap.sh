#!/usr/bin/env bash
# Sobe toda a stack da POC no minikube: cluster -> imagem Spark -> operators ->
# MinIO -> ClickHouse -> RBAC -> PostgreSQL -> MongoDB -> tabelas de fato.
# Rode a partir da RAIZ do repositório.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> 1/11 cluster local"
bash cluster/minikube-up.sh

echo "==> 2/11 imagem honeycomb + jars do ClickHouse"
# Nao reconstroi o honeycomb: a imagem publicada ja tem Spark, Delta, hadoop-aws e o app.
# O Dockerfile so acrescenta os jars do connector.
docker image inspect hub.datawake.cloud/dw-dados/honeycomb:latest >/dev/null 2>&1 \
  || docker pull hub.datawake.cloud/dw-dados/honeycomb:latest
docker build -f images/spark/Dockerfile -t honeycomb:poc images/spark/
minikube image load honeycomb:poc

echo "==> 3/11 namespaces"
kubectl apply -f infra/00-namespaces.yaml

echo "==> 4/11 MinIO"
kubectl apply -f infra/minio/minio-standalone.yaml
kubectl -n datamart rollout status statefulset/minio --timeout=300s
kubectl apply -f infra/minio/bucket-provision-job.yaml
kubectl -n datamart wait --for=condition=complete job/minio-provision --timeout=180s

echo "==> 5/11 Spark Operator + RBAC/secrets"
helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator \
  -n spark-operator --create-namespace -f infra/spark/operator-values.yaml
kubectl apply -f infra/spark/spark-rbac.yaml
kubectl apply -f infra/spark/spark-secrets.yaml

echo "==> 6/11 ClickHouse operator + instância"
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/release-0.24.0/deploy/operator/clickhouse-operator-install-bundle.yaml
kubectl apply -f infra/clickhouse/chi-datamart.yaml
echo "aguardando CHI (status=Completed)..."
for _ in $(seq 1 60); do
  st="$(kubectl -n datamart get chi datamart -o jsonpath='{.status.status}' 2>/dev/null || true)"
  [[ "$st" == "Completed" ]] && break
  sleep 10
done
kubectl -n datamart get chi datamart

echo "==> 7/11 RBAC multi-tenant (database/role/user/profile/quota por tenant)"
kubectl -n datamart create configmap ch-rbac-tpl \
  --from-file=ddl/rbac/10_tenant.sql.tpl --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/clickhouse/job-rbac.yaml
kubectl -n datamart wait --for=condition=complete job/ch-rbac --timeout=300s
bash scripts/verify-rbac.sh

echo "==> 8/11 PostgreSQL (braço de comparação)"
kubectl apply -f infra/postgres/postgres-statefulset.yaml
kubectl -n datamart rollout status statefulset/postgres --timeout=300s

echo "==> 9/11 MongoDB (control plane das DAGs)"
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

echo "==> 10/11 smoke do connector ClickHouse"
kubectl -n datamart create configmap ch-smoke \
  --from-file=infra/spark/smoke/smoke_clickhouse.py --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/spark/sparkapplication-smoke-clickhouse.yaml
for _ in $(seq 1 40); do
  st="$(kubectl -n datamart get sparkapplication smoke-clickhouse -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)"
  case "$st" in
    COMPLETED) echo "    smoke OK"; break ;;
    FAILED|SUBMISSION_FAILED)
      kubectl -n datamart logs smoke-clickhouse-driver | tail -30
      echo "ERRO: smoke do connector falhou." >&2; exit 1 ;;
  esac
  sleep 15
done

echo "==> 11/11 tabela de fato por tenant"
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
