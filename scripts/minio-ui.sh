#!/usr/bin/env bash
# Credenciais e contrato de layout do MinIO, para a carga manual dos Parquet.
#
#   bash scripts/minio-ui.sh            # credenciais e o layout esperado
#   bash scripts/minio-ui.sh --abrir    # tambem abre a porta do console
#
# O acesso em si vive no scripts/ports.sh — este script nao encaminha porta nenhuma.
set -euo pipefail

NS=datamart
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usuario="$(kubectl -n "$NS" get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)"
senha="$(kubectl -n "$NS" get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)"

echo "usuario : $usuario"
echo "senha   : $senha"
echo

if [[ "${1:-}" == "--abrir" ]]; then
  bash "$ROOT/scripts/ports.sh" --only console
  echo
else
  echo "Console em http://localhost:9001 — abra a porta antes, se ainda nao abriu:"
  echo "  bash scripts/ports.sh --only console"
  echo
fi

echo "Layout esperado pelo honeycomb, dentro do bucket 'datamart':"
echo "  data-bee_replication/data-bee_<filial>/<tabela>/*.parquet"
echo
echo "  <filial> precisa bater com 'filiais' em Data_Catalog.k8s_<tenant> (Mongo)."
echo "  <tabela> precisa bater com 'tables[].name' na mesma colecao."
kubectl -n "$NS" get secret mongo-creds >/dev/null 2>&1 && {
  echo
  echo "Contrato semeado hoje:"
  kubectl -n "$NS" exec mongodb-0 -- mongosh \
    "mongodb://$(kubectl -n "$NS" get secret mongo-creds -o jsonpath='{.data.MONGO_INITDB_ROOT_USERNAME}' | base64 -d):$(kubectl -n "$NS" get secret mongo-creds -o jsonpath='{.data.MONGO_INITDB_ROOT_PASSWORD}' | base64 -d)@localhost:27017/?authSource=admin" \
    --quiet --eval '
      ["acme","globex"].forEach(t => {
        const c = db.getSiblingDB("Data_Catalog").getCollection("k8s_"+t).find().sort({_id:-1}).limit(1).toArray()[0];
        if (c) c.filiais.forEach(f =>
          c.tables.forEach(tb => print("  data-bee_replication/data-bee_" + f + "/" + tb.name + "/")));
      });' 2>/dev/null | sort -u
}
