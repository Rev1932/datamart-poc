#!/bin/sh
# Uma execução no Postgres. Imprime "<ms> <linhas>". Uso: pg_exec.sh <schema> <arquivo.sql> <query_id> [saida]
schema="$1"; sql="$2"; qid="$3"; saida="${4:-/dev/null}"

# cursor_tuple_fraction = 1 mantém o plano de um SELECT comum: o FETCH_COUNT usa cursor, e sem
# isso o planner otimiza para a primeira linha.
{
  printf 'SET search_path TO %s;\nSET cursor_tuple_fraction = 1.0;\n' "$schema"
  printf '\\set FETCH_COUNT 50000\n\\timing on\n\\o %s\n/* %s */\n' "$saida" "$qid"
  cat "$sql"
  printf '\\o\n\\echo LINHAS :ROW_COUNT\n'
} | psql -X -A -t -F "$(printf '\t')" -v ON_ERROR_STOP=1 -d "dm_$TENANT" \
    -v filial="$FILIAL" -v banco="$BANCO" -v mes_ini="$MES_INI" -v mes_fim="$MES_FIM" \
    -v tri_ini="$TRI_INI" -v tri_fim="$TRI_FIM" -v sem_ini="$SEM_INI" -v sem_fim="$SEM_FIM" 2>&1 |
  awk '/^Time:/ { t = $2 } /^LINHAS/ { r = $2 } /ERROR|FATAL/ { e = 1; print > "/dev/stderr" }
       END { if (e || t == "") exit 1; print t, r }'
