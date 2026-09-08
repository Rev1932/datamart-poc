#!/usr/bin/env bash
# Popula a camada bronze no MinIO com Parquet de exemplo.
#
# PLACEHOLDER: a POC não inclui dados reais. Aponte SRC_DIR para uma pasta local
# com os .parquet da(s) tabela(s) de origem e este script os envia para o prefixo
# bronze esperado pelo pipeline bronze_silver:
#   s3a://datamart/data-bee_replication/data-bee_<config-name>/<table>/
set -euo pipefail

CONFIG_NAME="${CONFIG_NAME:-unipac}"
TABLE="${TABLE:-dw_andon_peso}"
SRC_DIR="${SRC_DIR:-}"

if [[ -z "$SRC_DIR" ]]; then
  echo "Defina SRC_DIR=/caminho/para/parquets (contendo os .parquet de $TABLE)." >&2
  echo "Ex.: SRC_DIR=./sample/$TABLE CONFIG_NAME=$CONFIG_NAME TABLE=$TABLE $0" >&2
  exit 1
fi

DEST="local/datamart/data-bee_replication/data-bee_${CONFIG_NAME}/${TABLE}"

# Usa um pod efêmero com mc dentro do cluster para enxergar o MinIO.
kubectl -n datamart run mc-seed --rm -i --restart=Never \
  --image=quay.io/minio/mc:RELEASE.2024-06-12T14-34-03Z \
  --env=MINIO_ROOT_USER=minioadmin --env=MINIO_ROOT_PASSWORD=minioadmin123 \
  --command -- /bin/sh -c "mc alias set local http://minio.datamart.svc.cluster.local:9000 \$MINIO_ROOT_USER \$MINIO_ROOT_PASSWORD && echo 'mc pronto (envie os arquivos via port-forward ou mc mirror a partir do host)'"

echo "Para enviar do host, faça port-forward do MinIO e use o mc local:"
echo "  kubectl -n datamart port-forward svc/minio 9000:9000 &"
echo "  mc alias set poc http://127.0.0.1:9000 minioadmin minioadmin123"
echo "  mc mirror '$SRC_DIR' 'poc/datamart/data-bee_replication/data-bee_${CONFIG_NAME}/${TABLE}'"
