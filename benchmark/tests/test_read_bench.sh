#!/usr/bin/env bash
# Comportamento do read-bench.sh contra o cluster: recusa sem quiesce, formato do CSV e paridade.
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH="$RAIZ/benchmark/read-bench.sh"
falhas=0
ok()  { echo "PASS  $1"; }
nok() { echo "FAIL  $1"; falhas=$((falhas + 1)); }

dir="$(mktemp -d)"
trap 'echo "artefatos do teste em $dir"' EXIT

quiesce_ativo() {
  [ "$(kubectl -n airflow get statefulset airflow-scheduler -o jsonpath='{.spec.replicas}')" = 0 ]
}

if ! quiesce_ativo; then
  saida="$(bash "$BENCH" -r 1 --queries q01 --saida "$dir" 2>&1)" && rc=0 || rc=$?
  [ "$rc" = 2 ] && grep -qi quiesce <<< "$saida" \
    && ok "sem quiesce e sem --permitir-ruido: recusa com 2 e cita o quiesce" \
    || nok "deveria recusar sem quiesce (rc=$rc)"
else
  echo "SKIP  recusa sem quiesce — o cluster está em quiesce agora"
fi

contar() { tail -n +2 "$1" | wc -l | tr -d ' '; }

bash "$BENCH" -r 2 -c 1 --queries q01 --permitir-ruido --saida "$dir" >/dev/null
csv="$(ls -t "$dir"/2*_quente_c1.csv | head -1)"
[ "$(contar "$csv")" = 6 ] && ok "-r 2 -c 1, 3 braços: 6 linhas" || nok "-r 2 -c 1 deveria gerar 6 linhas ($(contar "$csv"))"
[ "$(head -1 "$csv")" = "ts,perfil,engine,tenant,query,repeticao,modo,concorrencia,duracao_ms,read_rows,read_bytes,result_rows" ] \
  && ok "cabeçalho no formato da especificação" || nok "cabeçalho fora do formato"
awk -F, 'NR > 1 && !($9 + 0 > 0) { exit 1 }' "$csv" && ok "toda duração é positiva" || nok "há duração vazia ou zero"
[ "$(tail -n +2 "$csv" | cut -d, -f3 | sort -u | paste -sd,)" = "ch,pg,pgt" ] \
  && ok "os três braços presentes" || nok "braços ausentes"
awk -F, 'NR > 1 && $3 == "pg" { r = $12 } NR > 1 && $3 == "ch" { c = $12 } END { exit !(r != "" && r == c) }' "$csv" \
  && ok "result_rows igual entre pg e ch" || nok "result_rows diverge ou vazio"
estr="$(ls -t "$dir"/throttle_*_quente_c1.csv | head -1)"
[ "$(contar "$estr")" = 3 ] && awk -F, 'NR > 1 && !($6 + 0 > 0) { exit 1 }' "$estr" \
  && ok "estrangulamento registrado por braço, com períodos > 0" || nok "arquivo de estrangulamento ausente ou vazio"

bash "$BENCH" -r 2 -c 2 --queries q01 --bracos ch --permitir-ruido --saida "$dir" >/dev/null
csv="$(ls -t "$dir"/2*_quente_c2.csv | head -1)"
[ "$(contar "$csv")" = 4 ] && ok "-r 2 -c 2, 1 braço: 4 linhas" || nok "-r 2 -c 2 deveria gerar 4 linhas ($(contar "$csv"))"
awk -F, 'NR > 1 && $8 != 2 { exit 1 }' "$csv" && ok "concorrencia = 2 registrada" || nok "concorrencia errada no CSV"

for pod in bench-cliente-pg bench-cliente-ch; do
  printf 'SELECT coluna_que_nao_existe FROM fact_200_cep\n' | kubectl -n datamart exec -i "$pod" -- sh -c 'cat > /tmp/bench/erro.sql'
done
kubectl -n datamart exec bench-cliente-pg -- env TENANT=acme sh /tmp/bench/cliente/pg_exec.sh public /tmp/bench/erro.sql t32-erro \
  >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" != 0 ] && ok "pg_exec.sh falha com SQL inválido" || nok "pg_exec.sh engoliu o erro do SQL"
pw="$(kubectl -n datamart get secret ch-bench -o jsonpath='{.data.acme}' | base64 -d)"
kubectl -n datamart exec bench-cliente-ch -- env TENANT=acme CH_USER=u_acme_bench "CH_PW=$pw" \
  bash /tmp/bench/cliente/ch_exec.sh /tmp/bench/erro.sql t32-erro >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" != 0 ] && ok "ch_exec.sh falha com SQL inválido" || nok "ch_exec.sh engoliu o erro do SQL"

bash "$BENCH" --paridade --salvar-paridade "$dir/par" && rc=0 || rc=$?
[ "$rc" = 0 ] && ok "paridade das 5 queries nos 3 braços" || nok "paridade falhou (rc=$rc)"

primeiro="$(ls "$dir/par" | grep '_ch\.tsv$' | head -1)"
awk 'NR == 1 { $NF = $NF + 1 } { print }' OFS='\t' "$dir/par/$primeiro" > "$dir/par/tmp" && mv -- "$dir/par/tmp" "$dir/par/$primeiro"
bash "$BENCH" --comparar-paridade "$dir/par" >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" = 1 ] && ok "paridade adulterada ($primeiro) reprova com 1" || nok "adulteração deveria reprovar (rc=$rc)"

echo "falhas: $falhas"
[ "$falhas" = 0 ]
