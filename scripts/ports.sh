#!/usr/bin/env bash
# Abre (e mantem abertas) as portas dos servicos da POC em localhost.
#
#   bash scripts/ports.sh              # sobe tudo em segundo plano e devolve o prompt
#   bash scripts/ports.sh --status     # o que esta no ar
#   bash scripts/ports.sh --stop       # derruba tudo
#   bash scripts/ports.sh --help       # demais opcoes
#
# Cada porta roda sob um supervisor que reabre o encaminhamento sozinho quando a
# conexao cai — que e o que acontece a cada restart de pod.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ESTADO="${HOME}/.cache/datamart-poc/ports"

# chave|namespace|alvo|porta local|porta no pod|descricao|como usar
SERVICOS=(
  "console|datamart|svc/minio|9001|9001|Console do MinIO|http://localhost:9001"
  "s3|datamart|svc/minio|9000|9000|Endpoint S3 do MinIO|http://localhost:9000"
  "airflow|airflow|svc/airflow-webserver|8080|8080|Webserver do Airflow|http://localhost:8080"
  "clickhouse|datamart|svc/clickhouse-datamart|8123|8123|ClickHouse HTTP|http://localhost:8123/ping"
  "ch-native|datamart|svc/clickhouse-datamart|9010|9000|ClickHouse protocolo nativo|clickhouse-client --port 9010"
  "postgres|datamart|svc/postgres|5432|5432|PostgreSQL do braco de comparacao|psql -h localhost -p 5432 -U dm_app"
  "mongo|datamart|svc/mongodb|27017|27017|MongoDB do control plane|mongosh mongodb://localhost:27017"
)

ajuda() {
  cat <<'TXT'
Uso: bash scripts/ports.sh [acao] [--only chave1,chave2]

Acoes (sem nenhuma, equivale a --start):
  --start        sobe os encaminhamentos que ainda nao estao no ar
  --stop         derruba os encaminhamentos
  --restart      derruba e sobe de novo
  --status       mostra chave, porta, PID e se a porta responde
  --logs CHAVE   acompanha o log de um encaminhamento (Ctrl+C sai)
  --creds        imprime as credenciais dos servicos
  --list         lista as chaves disponiveis
  --help         esta mensagem

Filtro:
  --only console,airflow    age so nessas chaves

Exemplos:
  bash scripts/ports.sh
  bash scripts/ports.sh --only console,s3
  bash scripts/ports.sh --status
  bash scripts/ports.sh --stop
TXT
}

campo() { printf '%s' "$1" | cut -d'|' -f"$2"; }

chaves_selecionadas() {
  local filtro="$1" s chave
  for s in "${SERVICOS[@]}"; do
    chave="$(campo "$s" 1)"
    if [[ -z "$filtro" ]] || [[ ",$filtro," == *",$chave,"* ]]; then
      printf '%s\n' "$s"
    fi
  done
}

porta_em_uso() {
  ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${1}\$"
}

pid_vivo() {
  local arq="$ESTADO/$1.pid" pid
  [[ -f "$arq" ]] || return 1
  pid="$(cat "$arq" 2>/dev/null || true)"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

subir_um() {
  local s="$1"
  local chave ns alvo lp rp desc
  chave="$(campo "$s" 1)"; ns="$(campo "$s" 2)"; alvo="$(campo "$s" 3)"
  lp="$(campo "$s" 4)";    rp="$(campo "$s" 5)"; desc="$(campo "$s" 6)"

  if pid_vivo "$chave"; then
    printf '  %-11s ja no ar em :%s\n' "$chave" "$lp"
    return 0
  fi
  # Supervisor morto deixa o kubectl orfao segurando a porta; reapa o grupo antes
  # de julgar a porta ocupada, senao o --start seguinte recusa para sempre.
  if [[ -f "$ESTADO/$chave.pid" ]]; then
    local velho
    velho="$(cat "$ESTADO/$chave.pid" 2>/dev/null || true)"
    if [[ -n "$velho" ]]; then
      kill -TERM -- "-$velho" 2>/dev/null || true
    fi
    rm -f "$ESTADO/$chave.pid"
    sleep 1
  fi
  if porta_em_uso "$lp"; then
    printf '  %-11s IGNORADO: a porta %s ja esta ocupada por outro processo\n' "$chave" "$lp" >&2
    return 0
  fi

  local log="$ESTADO/$chave.log" cmd
  printf -- '--- %s: abrindo %s -> %s %s\n' "$(date '+%F %T')" "$lp" "$ns" "$alvo" >>"$log"
  printf -v cmd 'while true; do kubectl -n %q port-forward --address 127.0.0.1 %q %q >>%q 2>&1; printf "[%%s] conexao caiu, reabrindo\\n" "$(date +%%H:%%M:%%S)" >>%q; sleep 2; done' \
    "$ns" "$alvo" "${lp}:${rp}" "$log" "$log"
  setsid bash -c "$cmd" </dev/null >/dev/null 2>&1 &
  echo "$!" >"$ESTADO/$chave.pid"
  printf '  %-11s :%-5s  %s\n' "$chave" "$lp" "$desc"
}

derrubar_um() {
  local chave="$1"
  local arq="$ESTADO/$chave.pid"
  local pid
  if [[ ! -f "$arq" ]]; then
    printf '  %-11s nao estava no ar\n' "$chave"
    return 0
  fi
  pid="$(cat "$arq" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    # O supervisor e lider da sessao: o PID negativo derruba ele e o kubectl filho.
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    printf '  %-11s derrubado (pid %s)\n' "$chave" "$pid"
  else
    printf '  %-11s processo ja tinha morrido\n' "$chave"
  fi
  rm -f "$arq"
}

acao_start() {
  local filtro="$1" s
  mkdir -p "$ESTADO"
  echo "==> abrindo portas (segundo plano; o terminal fica livre)"
  while read -r s; do subir_um "$s"; done < <(chaves_selecionadas "$filtro")
  sleep 2
  echo
  acao_status "$filtro"
  echo
  echo "Para derrubar:   bash scripts/ports.sh --stop"
  echo "Credenciais:     bash scripts/ports.sh --creds"
}

acao_stop() {
  local filtro="$1" s
  echo "==> derrubando encaminhamentos"
  while read -r s; do derrubar_um "$(campo "$s" 1)"; done < <(chaves_selecionadas "$filtro")
}

acao_status() {
  local filtro="$1" s chave lp uso estado desc
  printf '%-12s %-7s %-9s %-9s %s\n' CHAVE PORTA PID PORTA_ABERTA COMO_USAR
  while read -r s; do
    chave="$(campo "$s" 1)"; lp="$(campo "$s" 4)"; desc="$(campo "$s" 7)"
    if pid_vivo "$chave"; then estado="$(cat "$ESTADO/$chave.pid")"; else estado="-"; fi
    if porta_em_uso "$lp"; then uso="sim"; else uso="NAO"; fi
    printf '%-12s %-7s %-9s %-9s %s\n' "$chave" "$lp" "$estado" "$uso" "$desc"
  done < <(chaves_selecionadas "$filtro")
}

acao_creds() {
  local u p
  echo "MinIO   (console e S3)"
  u="$(kubectl -n datamart get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_USER}' 2>/dev/null | base64 -d || true)"
  p="$(kubectl -n datamart get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' 2>/dev/null | base64 -d || true)"
  printf '  usuario %s / senha %s\n' "${u:-?}" "${p:-?}"
  echo "ClickHouse (administrativo)"
  p="$(kubectl -n datamart get secret ch-creds -o jsonpath='{.data.dm_admin}' 2>/dev/null | base64 -d || true)"
  printf '  usuario dm_admin / senha %s\n' "${p:-?}"
  echo "PostgreSQL"
  p="$(kubectl -n datamart get secret spark-secrets -o jsonpath='{.data.POSTGRES_PASSWORD}' 2>/dev/null | base64 -d || true)"
  printf '  usuario dm_app / senha %s / bancos dm_acme e dm_globex\n' "${p:-?}"
  echo "MongoDB"
  u="$(kubectl -n datamart get secret mongo-creds -o jsonpath='{.data.MONGO_INITDB_ROOT_USERNAME}' 2>/dev/null | base64 -d || true)"
  p="$(kubectl -n datamart get secret mongo-creds -o jsonpath='{.data.MONGO_INITDB_ROOT_PASSWORD}' 2>/dev/null | base64 -d || true)"
  printf '  usuario %s / senha %s / authSource admin\n' "${u:-?}" "${p:-?}"
  echo "Airflow"
  echo "  usuario admin / senha admin"
}

acao=start
filtro=""
alvo_log=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --start)   acao=start ;;
    --stop)    acao=stop ;;
    --restart) acao=restart ;;
    --status)  acao=status ;;
    --creds)   acao=creds ;;
    --list)    acao=list ;;
    --logs)    acao=logs; alvo_log="${2:-}"; shift ;;
    --only)    filtro="${2:-}"; shift ;;
    -h|--help) ajuda; exit 0 ;;
    *) echo "opcao desconhecida: $1" >&2; echo >&2; ajuda >&2; exit 2 ;;
  esac
  shift
done

if [[ -n "$filtro" ]]; then
  for k in ${filtro//,/ }; do
    chaves_selecionadas "$k" | grep -q . || { echo "chave desconhecida: $k" >&2; exit 2; }
  done
fi

case "$acao" in
  start)   acao_start "$filtro" ;;
  stop)    acao_stop "$filtro" ;;
  restart) acao_stop "$filtro"; sleep 1; acao_start "$filtro" ;;
  status)  mkdir -p "$ESTADO"; acao_status "$filtro" ;;
  creds)   acao_creds ;;
  list)    while read -r s; do printf '%-12s %s\n' "$(campo "$s" 1)" "$(campo "$s" 6)"; done < <(chaves_selecionadas "") ;;
  logs)
    [[ -n "$alvo_log" ]] || { echo "informe a chave: bash scripts/ports.sh --logs console" >&2; exit 2; }
    arq="$ESTADO/$alvo_log.log"
    [[ -f "$arq" ]] || { echo "sem log para '$alvo_log' (ja subiu essa porta?)" >&2; exit 1; }
    exec tail -f "$arq" ;;
esac
