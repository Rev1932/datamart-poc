#!/bin/bash
# Uma execução no ClickHouse. Imprime "<ms> <linhas>". Uso: ch_exec.sh <arquivo.sql> <query_id> [saida] [--eventos]
sql="$1"; qid="$2"; saida="${3:-/dev/null}"; eventos="${4:-}"
[ -n "${CH_USER:-}" ] && [ -n "${CH_PW:-}" ] || { echo "CH_USER e CH_PW são obrigatórios" >&2; exit 1; }
formato=Null
[ "$saida" != /dev/null ] && formato=TabSeparated
extra=()
[ "$eventos" = --eventos ] && extra=(--print-profile-events --profile-events-delay-ms=-1)

err="$(clickhouse-client --host "$CH_HOST" --user "$CH_USER" --password "$CH_PW" \
  --database "dm_$TENANT" --query_id "$qid" --time --format "$formato" "${extra[@]}" \
  --param_filial="$FILIAL" --param_banco="$BANCO" --param_mes_ini="$MES_INI" --param_mes_fim="$MES_FIM" \
  --param_tri_ini="$TRI_INI" --param_tri_fim="$TRI_FIM" --param_sem_ini="$SEM_INI" --param_sem_fim="$SEM_FIM" \
  --queries-file "$sql" 2>&1 >"$saida")"
rc=$?

# O cliente imprime o tempo mesmo quando a query falha no meio do resultado: o código de saída decide.
t="$(grep -E '^[0-9]+\.[0-9]+$' <<< "$err" | tail -1)"
[ "$rc" = 0 ] && [ -n "$t" ] || { echo "$err" >&2; exit 1; }
linhas=""
[ "$saida" != /dev/null ] && linhas="$(wc -l < "$saida")"
awk -v t="$t" -v l="$linhas" 'BEGIN { printf "%.3f %s\n", t * 1000, l }'
[ "$eventos" = --eventos ] && grep -E '\] (SelectedRows|SelectedBytes|ReadCompressedBytes|MemoryTrackerPeakUsage):' <<< "$err" |
  awk '{ print "EVENTO", $(NF-2), $(NF-1) }'
exit 0
