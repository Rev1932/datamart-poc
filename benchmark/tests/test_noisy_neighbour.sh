#!/usr/bin/env bash
# Comportamento do noisy-neighbour.sh: avaliação do aceite sobre CSV sintético e smoke curto no cluster.
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NN="$RAIZ/benchmark/noisy-neighbour.sh"
falhas=0
ok()  { echo "PASS  $1"; }
nok() { echo "FAIL  $1"; falhas=$((falhas + 1)); }

dir="$(mktemp -d)"
trap 'echo "artefatos do teste em $dir"' EXIT

# 20 amostras por fase: rank mais próximo dá p95 = 19º valor = base × 19.
sintetico() {
  local arq="$1"; shift
  echo "ts,rodada,fase,query,duracao_ms" > "$arq"
  local fase base
  for par in "$@"; do
    fase="${par%%:*}"; base="${par#*:}"
    for i in $(seq 1 20); do
      echo "x,1,$fase,q04,$((base * i))" >> "$arq"
      echo "x,1,$fase,q01,$((base * i))" >> "$arq"
    done
  done
}

sintetico "$dir/protege_f4.csv" 1:10 2:30 3:28 4:12
saida="$(bash "$NN" --avaliar "$dir/protege_f4.csv" 2>&1)" && rc=0 || rc=$?
[ "$rc" = 0 ] && grep -q "ACEITE" <<< "$saida" && grep -q "fase 4" <<< "$(grep ACEITE <<< "$saida")" \
  && ok "vizinho degrada 3×, fase 4 volta a 1,2×: aceite pela fase 4" || { nok "deveria aceitar pela fase 4 (rc=$rc)"; echo "$saida"; }
grep -Eq '^\| 1 +\| q04 +\|.*\| 190 +\|' <<< "$saida" && ok "p95 da fase 1 = 190 (rank mais próximo)" || nok "p95 da fase 1 errado"

sintetico "$dir/nao_protege.csv" 1:10 2:30 3:28 4:20
bash "$NN" --avaliar "$dir/nao_protege.csv" >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" = 1 ] && ok "nenhum mecanismo ≤ 1,3×: reprova com 1" || nok "deveria reprovar (rc=$rc)"

sintetico "$dir/sem_degradar.csv" 1:10 2:11 3:10 4:10
saida="$(bash "$NN" --avaliar "$dir/sem_degradar.csv" 2>&1)" && rc=0 || rc=$?
[ "$rc" = 1 ] && grep -qi "inconclusiv" <<< "$saida" \
  && ok "vizinho que não degrada: inconclusivo, sai com 1" || nok "sem degradação deveria ser inconclusivo (rc=$rc)"

quiesce_ativo() {
  [ "$(kubectl -n airflow get statefulset airflow-scheduler -o jsonpath='{.spec.replicas}')" = 0 ]
}
if ! quiesce_ativo; then
  saida="$(bash "$NN" --duracao 5 --rodadas 1 --saida "$dir" 2>&1)" && rc=0 || rc=$?
  [ "$rc" = 2 ] && grep -qi quiesce <<< "$saida" \
    && ok "sem quiesce e sem --permitir-ruido: recusa com 2" || nok "deveria recusar sem quiesce (rc=$rc)"
fi

saida="$(bash "$NN" --duracao 15 --pausa 5 --rodadas 1 --permitir-ruido --saida "$dir" 2>&1)" && rc=0 || rc=$?
[ "$rc" != 2 ] && ok "smoke de 15 s por fase termina sem erro de execução (rc=$rc)" || { nok "smoke falhou (rc=$rc)"; echo "$saida" | tail -20; }
csv="$(ls -t "$dir"/vizinho_2*.csv 2>/dev/null | head -1)"
carga="$(ls -t "$dir"/vizinho_carga_*.csv 2>/dev/null | head -1)"
[ -n "$csv" ] && [ "$(tail -n +2 "$csv" | cut -d, -f3 | sort -u | paste -sd,)" = "1,2,3,4" ] \
  && ok "medição nas 4 fases" || nok "faltam fases na medição"
[ -n "$csv" ] && [ "$(tail -n +2 "$csv" | cut -d, -f4 | sort -u | paste -sd,)" = "q01,q04" ] \
  && ok "q04 e q01 medidas" || nok "faltam consultas"
[ -n "$carga" ] && awk -F, 'NR > 1 && $2 == 2 && $4 > 0 { achou = 1 } END { exit !achou }' "$carga" \
  && ok "vizinho da fase 2 executou consultas" || nok "vizinho da fase 2 não executou"
[ -n "$carga" ] && [ "$(tail -n +2 "$carga" | cut -d, -f2 | sort -u | paste -sd,)" = "2,3,4" ] \
  && ok "carga registrada nas fases 2, 3 e 4" || nok "carga do vizinho incompleta"

echo "falhas: $falhas"
[ "$falhas" = 0 ]
