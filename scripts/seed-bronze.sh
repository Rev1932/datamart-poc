#!/usr/bin/env bash
# Popula a camada bronze no MinIO com Parquet de exemplo.
#
# PLACEHOLDER: a POC não inclui dados reais. Aponte --src-dir para uma pasta local
# com os .parquet da(s) tabela(s) de origem e este script os envia para o prefixo
# bronze esperado pelo pipeline bronze_silver:
#   s3a://datamart/data-bee_replication/data-bee_<config-name>/<table>/
set -euo pipefail

usage() {
  cat <<EOF
Uso: bash scripts/seed-bronze.sh --src-dir /caminho/para/parquets [--config-name unipac] [--table dw_andon_peso]

  --src-dir CAMINHO      pasta local com os .parquet da tabela (obrigatório)
  --config-name NOME     nome da config/filial (default: \$CONFIG_NAME ou unipac)
  --table NOME           nome da tabela (default: \$TABLE ou dw_andon_peso)
  -h, --help             mostra esta ajuda

Fallback: as variáveis de ambiente SRC_DIR, CONFIG_NAME e TABLE continuam
aceitas (flag tem precedência sobre elas).
EOF
}

CONFIG_NAME="${CONFIG_NAME:-unipac}"
TABLE="${TABLE:-dw_andon_peso}"
SRC_DIR="${SRC_DIR:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --src-dir) SRC_DIR="$2"; shift 2 ;;
    --config-name) CONFIG_NAME="$2"; shift 2 ;;
    --table) TABLE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERRO: opção desconhecida '$1'." >&2; usage >&2; exit 1 ;;
  esac
done

if [[ -z "$SRC_DIR" ]]; then
  echo "Defina --src-dir /caminho/para/parquets (contendo os .parquet de $TABLE)." >&2
  echo "Ex.: bash $0 --src-dir ./sample/$TABLE --config-name $CONFIG_NAME --table $TABLE" >&2
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
