#!/usr/bin/env bash
# Sobe toda a stack da POC no minikube: cluster -> imagem Spark -> operators ->
# MinIO -> ClickHouse -> RBAC/secrets -> tabela ClickHouse.
# Rode a partir da RAIZ do repositório.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> 1/7 cluster local"
bash cluster/minikube-up.sh

echo "==> 2/7 build da imagem Spark (dentro do minikube)"
minikube image build -t datamart-spark:poc -f images/spark/Dockerfile .

echo "==> 3/7 namespaces"
kubectl apply -f infra/00-namespaces.yaml

echo "==> 4/7 MinIO"
kubectl apply -f infra/minio/minio-standalone.yaml
kubectl -n datamart rollout status statefulset/minio --timeout=300s
kubectl apply -f infra/minio/bucket-provision-job.yaml
kubectl -n datamart wait --for=condition=complete job/minio-provision --timeout=180s

echo "==> 5/7 Spark Operator + RBAC/secrets"
helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator \
  -n spark-operator --create-namespace -f infra/spark/operator-values.yaml
kubectl apply -f infra/spark/spark-rbac.yaml
kubectl apply -f infra/spark/spark-secrets.yaml

echo "==> 6/7 ClickHouse operator + instância"
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/release-0.24.0/deploy/operator/clickhouse-operator-install-bundle.yaml
kubectl apply -f infra/clickhouse/chi-datamart.yaml
echo "aguardando CHI (status=Completed)..."
for _ in $(seq 1 60); do
  st="$(kubectl -n datamart get chi datamart -o jsonpath='{.status.status}' 2>/dev/null || true)"
  [[ "$st" == "Completed" ]] && break
  sleep 10
done
kubectl -n datamart get chi datamart

echo "==> 7/7 tabela ClickHouse (ddl/01)"
POD="$(kubectl -n datamart get pod -l clickhouse.altinity.com/chi=datamart -o jsonpath='{.items[0].metadata.name}')"
kubectl -n datamart exec -i "$POD" -- clickhouse-client --user datamart --password datamart123 --multiquery < ddl/01_create_datamart_table.sql

echo "==> stack pronta. Próximos: scripts/seed-bronze.sh, run-normalize.sh, run-ingest.sh"
