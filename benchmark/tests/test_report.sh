#!/usr/bin/env bash
# Agregação do report.sh sobre CSVs sintéticos com durações conhecidas.
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="$RAIZ/benchmark/report.sh"
falhas=0
ok()  { echo "PASS  $1"; }
nok() { echo "FAIL  $1"; falhas=$((falhas + 1)); }

cabecalho="ts,perfil,engine,tenant,query,repeticao,modo,concorrencia,duracao_ms,read_rows,read_bytes,result_rows"

gerar() {
  local arquivo="$1" fator="$2"
  echo "$cabecalho" > "$arquivo"
  for engine in pg ch; do
    for i in $(seq 1 20); do
      awk -v i="$i" -v f="$fator" -v e="$engine" 'BEGIN {
        printf "2026-09-11T00:00:00,small,%s,acme,q01,%d,quente,1,%.1f,,,5\n", e, i, (e == "pg" ? 10 : 1) * i * f }'
    done >> "$arquivo"
  done
}

dir="$(mktemp -d)"
r1="$dir/r1_quente_c1.csv"
r2="$dir/r2_quente_c1.csv"
vazio="$dir/vazio_quente_c1.csv"
c8a="$dir/r1_quente_c8.csv"
c8b="$dir/r2_quente_c8.csv"
trap 'rm -f -- "$r1" "$r2" "$vazio" "$c8a" "$c8b"; rmdir -- "$dir"' EXIT

gerar "$r1" 1
gerar "$r2" 1.1
saida="$(bash "$REPORT" --dir "$dir" --aceite 2>&1)" && rc=0 || rc=$?

# Rank mais próximo: de 20 valores, o p50 é o 10º e o p95 é o 19º.
grep -Eq '^\| q01 +\| quente +\| 1 +\| pg +\| 100\.0 +\| 190\.0 +\| 20 +\| r1_quente_c1 +\|' <<< "$saida" \
  && ok "p50/p95 por rank mais próximo na rodada (pg: 100 / 190)" || nok "p50/p95 por rank mais próximo"
grep -Eq '^\| q01 +\| quente +\| 1 +\|.*\| 10\.0 +\|' <<< "$saida" \
  && ok "fator p95 pg/ch = 10 no consolidado" || nok "fator p95 pg/ch = 10"
[ "$rc" = 0 ] && ok "desvio de p50 de 10 % passa no aceite" || { nok "desvio de 10 % deveria passar (rc=$rc)"; echo "$saida"; }

estr="$dir/throttle_r1_quente_c1.csv"
printf 'ts,engine,query,modo,concorrencia,periodos,estrangulados,estrangulado_ms\nx,pg,q01,quente,1,200,50,4000\n' > "$estr"
saida="$(bash "$REPORT" --dir "$dir" --aceite 2>&1)" && rc=0 || rc=$?
[ "$rc" = 0 ] && ok "arquivo de estrangulamento não entra na latência" || nok "arquivo de estrangulamento quebrou o relatório (rc=$rc)"
grep -Eq '^\| r1_quente_c1 +\| pg +\| 200 +\| 50 +\| 25\.0 +\| 4\.0 +\|' <<< "$saida" \
  && ok "seção de estrangulamento: 25 % e 4 s" || nok "seção de estrangulamento ausente ou errada"
rm -f -- "$estr"

gerar "$r2" 1.5
saida="$(bash "$REPORT" --dir "$dir" --aceite 2>&1)" && rc=0 || rc=$?
[ "$rc" = 1 ] && ok "desvio de p50 de 50 % com -c 1 reprova o aceite (rc=1)" || nok "desvio de 50 % deveria reprovar (rc=$rc)"
grep -Eq 'FALHA.*q01.*pg|q01.*pg.*FALHA' <<< "$saida" && ok "reprovação nomeia a query e o braço" || nok "reprovação não nomeia a linha"

sed 's/,quente,1,/,quente,8,/' "$r1" > "$c8a"
sed 's/,quente,1,/,quente,8,/' "$r2" > "$c8b"
rm -f -- "$r1" "$r2"
saida="$(bash "$REPORT" --dir "$dir" --aceite 2>&1)" && rc=0 || rc=$?
[ "$rc" = 0 ] && grep -q 'indicativo' <<< "$saida" && ok "com -c 8 o desvio é indicativo e não reprova" \
  || nok "desvio com -c 8 não deveria reprovar (rc=$rc)"
rm -f -- "$c8a" "$c8b"

echo "$cabecalho" > "$vazio"
bash "$REPORT" --dir "$dir" --aceite >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" = 2 ] && ok "CSV sem medições sai com 2" || nok "CSV sem medições deveria sair com 2 (rc=$rc)"

echo "falhas: $falhas"
[ "$falhas" = 0 ]
