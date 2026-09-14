#!/bin/sh
# Um fluxo de concorrência: <reps> execuções seguidas, uma linha de CSV por execução.
# Uso: fluxo.sh <braço> <query> <reps> <fluxo>. O xargs -P passa o número do fluxo por último.
braco="$1"; q="$2"; reps="$3"; fluxo="$4"
dir=/tmp/bench/queries
i=1
while [ "$i" -le "$reps" ]; do
  qid="t32-$RUN_ID-$braco-$q-$fluxo-$i"
  case "$braco" in
    pg)  r="$(sh /tmp/bench/cliente/pg_exec.sh public "$(ls $dir/${q}_*.pg.sql)" "$qid")" ;;
    pgt) r="$(sh /tmp/bench/cliente/pg_exec.sh gold_tuned "$(ls $dir/${q}_*.pg.sql)" "$qid")" ;;
    ch)  r="$(bash /tmp/bench/cliente/ch_exec.sh "$(ls $dir/${q}_*.ch.sql)" "$qid")" ;;
  esac || { echo "ERRO na execução $qid" >&2; exit 1; }
  rep=$(( (fluxo - 1) * reps + i ))
  printf '%s,%s,%s,%s,%s,%d,%s,%s,%s,,,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$PERFIL" "$braco" \
    "$TENANT" "$q" "$rep" "$MODO" "$CONC" "${r%% *}" "$RESULT_ROWS"
  i=$((i + 1))
done
