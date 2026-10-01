#!/bin/bash
# Carga do vizinho: <laços> consultas pesadas em paralelo até <segundos>. Uso: vizinho.sh <usuário> <segundos> <laços>
# Imprime "OK <n>" por laço e "ERRO <código>" por consulta rejeitada. CH_PW no ambiente.
usuario="$1"; segundos="$2"; lacos="$3"
fim=$(( $(date +%s) + segundos ))

laco() {
  local ok=0 saida codigo
  while [ "$(date +%s)" -lt "$fim" ]; do
    if saida="$(clickhouse-client --host "$CH_HOST" --user "$usuario" --password "$CH_PW" \
        --database dm_globex --format Null --queries-file /tmp/bench/queries/vizinho.ch.sql 2>&1)"; then
      ok=$((ok + 1))
    else
      codigo="$(grep -oE 'Code: [0-9]+' <<< "$saida" | head -1 | cut -d' ' -f2)"
      echo "ERRO ${codigo:-?}"
      # Um cliente real espera antes de tentar de novo; sem isso o laço martela o servidor com rejeições.
      sleep 1
    fi
  done
  echo "OK $ok"
}

for _ in $(seq "$lacos"); do laco & done
wait
