#!/bin/bash
# Mede q04 e q01 do acme, alternadas, até <segundos>. Uso: medir_por_tempo.sh <segundos> <rodada> <fase>
segundos="$1"; rodada="$2"; fase="$3"
fim=$(( $(date +%s) + segundos ))
dir=/tmp/bench/queries
i=0
while [ "$(date +%s)" -lt "$fim" ]; do
  for q in q04 q01; do
    i=$((i + 1))
    r="$(bash /tmp/bench/cliente/ch_exec.sh "$(ls $dir/${q}_*.ch.sql)" "t33-$RUN_ID-$rodada-$fase-$q-$i")" \
      || { echo "ERRO na execução $q" >&2; exit 1; }
    printf '%s,%s,%s,%s,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$rodada" "$fase" "$q" "${r%% *}"
  done
done
