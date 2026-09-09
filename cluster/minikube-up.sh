#!/usr/bin/env bash
# Sobe o cluster local (minikube) para a POC. Roda dentro do WSL2 (driver docker).
set -euo pipefail

usage() {
  cat <<EOF
Uso: bash cluster/minikube-up.sh [--profile small|full] [--disk 90g]

  --profile small|full   perfil de recursos do nó (default: \$PROFILE ou small)
  --disk TAMANHO         tamanho de disco do minikube, ex.: 90g (default: \$DISK ou 90g)
  -h, --help             mostra esta ajuda

Fallback: as variáveis de ambiente PROFILE e DISK continuam aceitas (flag tem
precedência sobre elas).
EOF
}

PROFILE="${PROFILE:-small}"
DISK="${DISK:-90g}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --disk) DISK="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERRO: opção desconhecida '$1'." >&2; usage >&2; exit 1 ;;
  esac
done

case "$PROFILE" in
  full)  CPUS=6; MEMORY_GIB=10 ;;
  small) CPUS=4; MEMORY_GIB=8 ;;
  *)
    echo "ERRO: PROFILE inválido '$PROFILE'. Use full ou small." >&2
    exit 1
    ;;
esac

RAM_MIN_GIB=$(( MEMORY_GIB + 5 ))
DISK_MIN_GIB=82

if ! command -v minikube >/dev/null 2>&1; then
  echo "ERRO: minikube não encontrado no PATH." >&2
  exit 1
fi

ram_total_gib="$(awk '/^MemTotal:/ {printf "%d", $2/1048576}' /proc/meminfo)"
if (( ram_total_gib < RAM_MIN_GIB )); then
  cat >&2 <<EOF
ERRO: RAM insuficiente para PROFILE=$PROFILE.
  disponível no WSL2 : ${ram_total_gib} GiB
  exigido            : ${RAM_MIN_GIB} GiB (${MEMORY_GIB} para o nó + 5 para o WSL2 e suas ferramentas)

Rode o perfil reduzido: bash cluster/minikube-up.sh --profile small
Ou ajuste %USERPROFILE%\\.wslconfig no Windows e rode 'wsl --shutdown':
  [wsl2]
  memory=$(( RAM_MIN_GIB + 1 ))GB
EOF
  exit 1
fi

docker_root="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)"
[[ -d "${docker_root:-}" ]] || docker_root=/var/lib/docker
[[ -d "$docker_root" ]] || docker_root=/

disk_free_gib="$(df -BG --output=avail "$docker_root" | tail -1 | tr -dc '0-9')"
if (( disk_free_gib < DISK_MIN_GIB )); then
  cat >&2 <<EOF
ERRO: espaço em disco insuficiente em $docker_root.
  livre   : ${disk_free_gib} GiB
  exigido : ${DISK_MIN_GIB} GiB (76 de PersistentVolumeClaim + ~6 de imagens)

Sem isso o nó ganha o taint node.kubernetes.io/disk-pressure no meio da carga:
pods são evictados e o ClickHouse falha com Code 243 (NOT_ENOUGH_SPACE).
Libere espaço com 'docker system prune -a' ou expanda o vhdx do WSL2.
EOF
  exit 1
fi

if ! minikube status >/dev/null 2>&1; then
  echo ">> minikube start (perfil=$PROFILE cpus=$CPUS memory=${MEMORY_GIB}g disk=$DISK)"
  minikube start --driver=docker --cpus="$CPUS" --memory="${MEMORY_GIB}g" --disk-size="$DISK"
else
  echo ">> minikube já está rodando — perfil NÃO é reaplicado em cluster existente."
  echo "   Para trocar de perfil: minikube delete && bash cluster/minikube-up.sh --profile $PROFILE"
fi

echo ">> habilitando addons"
minikube addons enable storage-provisioner
minikube addons enable default-storageclass
minikube addons enable metrics-server || true

kubectl get node -o jsonpath='{.items[0].status.capacity}'; echo
echo ">> cluster pronto (perfil=$PROFILE)."
