#!/usr/bin/env bash
# Abre o console do MinIO para a carga manual dos Parquet.
#
#   bash scripts/minio-ui.sh            # imprime a URL e as credenciais
#   bash scripts/minio-ui.sh --forward  # encaminha por localhost (necessario no Windows)
set -euo pipefail

NS=datamart
PORTA_CONSOLE=9001

usuario="$(kubectl -n "$NS" get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)"
senha="$(kubectl -n "$NS" get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)"
ip="$(minikube ip)"
nodeport="$(kubectl -n "$NS" get svc minio-console -o jsonpath='{.spec.ports[?(@.name=="console")].nodePort}')"

echo "usuario : $usuario"
echo "senha   : $senha"
echo

if [[ "${1:-}" == "--forward" ]]; then
  echo ">> encaminhando localhost:${PORTA_CONSOLE} -> console do MinIO"
  echo ">> abra http://localhost:${PORTA_CONSOLE} e deixe este terminal aberto (Ctrl+C encerra)"
  exec kubectl -n "$NS" port-forward --address 127.0.0.1 svc/minio-console "${PORTA_CONSOLE}:9001"
fi

echo "Do WSL2, direto pelo NodePort:"
echo "  http://${ip}:${nodeport}"
echo
echo "Do Windows, a rede do minikube nao e roteavel: use o encaminhamento."
echo "  bash scripts/minio-ui.sh --forward   ->  http://localhost:${PORTA_CONSOLE}"
echo
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
