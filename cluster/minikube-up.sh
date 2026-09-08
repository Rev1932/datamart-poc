#!/usr/bin/env bash
# Sobe o cluster local (minikube) para a POC. Roda dentro do WSL2 (driver docker).
set -euo pipefail

CPUS="${CPUS:-4}"
MEMORY="${MEMORY:-8g}"
DISK="${DISK:-40g}"

if ! command -v minikube >/dev/null 2>&1; then
  echo "ERRO: minikube não encontrado no PATH." >&2
  exit 1
fi

if ! minikube status >/dev/null 2>&1; then
  echo ">> minikube start (cpus=$CPUS memory=$MEMORY disk=$DISK)"
  minikube start --driver=docker --cpus="$CPUS" --memory="$MEMORY" --disk-size="$DISK"
else
  echo ">> minikube já está rodando."
fi

echo ">> habilitando addons"
minikube addons enable storage-provisioner
minikube addons enable default-storageclass
minikube addons enable metrics-server || true

kubectl get nodes
echo ">> cluster pronto."
