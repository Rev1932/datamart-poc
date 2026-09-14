#!/usr/bin/env bash
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS=datamart
POD_PG=bench-cliente-pg
POD_CH=bench-cliente-ch
POD_CH_SRV=chi-datamart-datamart-0-0-0

R=10
C=1
MODO=quente
QUERIES="q01,q02,q03,q04,q05"
BRACOS="pg,pgt,ch"
TENANT=acme
SAIDA="$RAIZ/benchmark/results"
RUIDO=0
ACAO=medir
DIR_PARIDADE=""

# Recorte fixo do benchmark: LIMEIRA, pico de agosto de 2026 e os três meses carregados.
PARAMS=(
  "FILIAL=LIMEIRA" "BANCO=dw_limeira"
  "MES_INI=2026-08-01 00:00:00" "MES_FIM=2026-09-01 00:00:00"
  "TRI_INI=2026-07-01 00:00:00" "TRI_FIM=2026-10-01 00:00:00"
  "SEM_INI=2026-08-24 00:00:00" "SEM_FIM=2026-08-31 00:00:00"
)

usage() {
  cat <<'EOF'
Uso: bash benchmark/read-bench.sh [opções]

Suíte de leitura da T3.2: 5 consultas nos braços pg (public), pgt (gold_tuned) e ch (ClickHouse).

Medição:
  -r N                   execuções medidas por fluxo (padrão 10)
  -c N                   fluxos simultâneos (padrão 1)
  --modo quente|frio     quente descarta 2 execuções de aquecimento; frio derruba os caches antes de
                         cada execução e reinicia o pod do Postgres (só com -c 1)
  --queries q01,...      subconjunto das consultas (padrão: todas)
  --bracos pg,pgt,ch     subconjunto dos braços (padrão: todos)
  --tenant acme|globex   padrão acme
  --saida DIR            diretório do CSV (padrão benchmark/results)
  --permitir-ruido       mede sem quiesce; o CSV sai com perfil "small-ruidoso"

Outros passos:
  --paridade             confere que os braços devolvem o mesmo resultado; não cronometra
  --salvar-paridade DIR  como --paridade, guardando as saídas normalizadas em DIR
  --comparar-paridade DIR  compara saídas já guardadas, sem consultar os bancos
  --instrumentar         uma execução por consulta e braço com EXPLAIN (ANALYZE, BUFFERS) no Postgres e
                         eventos de leitura no ClickHouse; grava instr_<ts>.csv
  -h, --help

Saída: 0 ok; 1 paridade divergente; 2 erro de uso, de pré-condição ou de execução.
EOF
}

die() { echo "ERRO: $*" >&2; exit 2; }

while (( $# )); do
  case "$1" in
    -r) R="$2"; shift 2 ;;
    -c) C="$2"; shift 2 ;;
    --modo) MODO="$2"; shift 2 ;;
    --queries) QUERIES="$2"; shift 2 ;;
    --bracos) BRACOS="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --saida) SAIDA="$2"; shift 2 ;;
    --permitir-ruido) RUIDO=1; shift ;;
    --paridade) ACAO=paridade; shift ;;
    --salvar-paridade) ACAO=paridade; DIR_PARIDADE="$2"; shift 2 ;;
    --comparar-paridade) ACAO=comparar; DIR_PARIDADE="$2"; shift 2 ;;
    --instrumentar) ACAO=instrumentar; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "opção desconhecida: $1 (veja --help)" ;;
  esac
done

[[ "$R" =~ ^[1-9][0-9]*$ && "$C" =~ ^[1-9][0-9]*$ ]] || die "-r e -c precisam ser inteiros positivos"
case "$MODO" in quente|frio) ;; *) die "modo inválido: $MODO" ;; esac
case "$TENANT" in acme|globex) ;; *) die "tenant inválido: $TENANT" ;; esac
[[ "$MODO" == frio && "$C" != 1 ]] && die "o modo frio só faz sentido com -c 1"
IFS=, read -r -a QS <<< "$QUERIES"
IFS=, read -r -a BS <<< "$BRACOS"
for b in "${BS[@]}"; do case "$b" in pg|pgt|ch) ;; *) die "braço inválido: $b" ;; esac; done

pod_do() { [[ "$1" == ch ]] && echo "$POD_CH" || echo "$POD_PG"; }
schema_do() { [[ "$1" == pgt ]] && echo gold_tuned || echo public; }
arquivo_sql() {
  local f
  f="$(ls "$RAIZ"/benchmark/queries/"$1"_*."$2".sql 2>/dev/null | head -1)"
  [[ -n "$f" ]] || die "consulta $1 não encontrada para $2"
  echo "/tmp/bench/queries/$(basename "$f")"
}
CH_PW=""
no_pod() {
  local pod="$1"; shift
  kubectl -n "$NS" exec -i "$pod" -- env "TENANT=$TENANT" "CH_USER=u_${TENANT}_bench" "CH_PW=$CH_PW" "${PARAMS[@]}" "$@"
}

secret_de() { kubectl -n "$NS" get secret "$1" -o jsonpath="{.data.$2}" | base64 -d; }

garantir_usuario_bench() {
  if ! kubectl -n "$NS" get secret ch-bench >/dev/null 2>&1; then
    kubectl -n "$NS" create secret generic ch-bench \
      --from-literal=acme="$(head -c 48 /dev/urandom | base64 | tr -dc A-Za-z0-9 | head -c 24)" \
      --from-literal=globex="$(head -c 48 /dev/urandom | base64 | tr -dc A-Za-z0-9 | head -c 24)" >/dev/null
  fi
  CH_PW="$(secret_de ch-bench "$TENANT")"
  sed -e "s/{{TENANT}}/$TENANT/g" -e "s/{{DB}}/dm_$TENANT/g" -e "s/{{SENHA}}/$CH_PW/g" \
      "$RAIZ/benchmark/views/01_usuario_bench.ch.sql" |
    kubectl -n "$NS" exec -i "$POD_CH_SRV" -- clickhouse-client --user dm_admin \
      --password "$(secret_de ch-creds dm_admin)" --multiquery >/dev/null
}

executar() {
  local b="$1" q="$2" qid="$3" saida="${4:-/dev/null}" eventos="${5:-}"
  if [[ "$b" == ch ]]; then
    no_pod "$POD_CH" bash /tmp/bench/cliente/ch_exec.sh "$(arquivo_sql "$q" ch)" "$qid" "$saida" "$eventos"
  else
    no_pod "$POD_PG" sh /tmp/bench/cliente/pg_exec.sh "$(schema_do "$b")" "$(arquivo_sql "$q" pg)" "$qid" "$saida"
  fi
}

preparar() {
  if ! kubectl -n "$NS" get pod "$POD_PG" "$POD_CH" >/dev/null 2>&1; then
    kubectl apply -f "$RAIZ/benchmark/clientes.yaml" >/dev/null
  fi
  kubectl -n "$NS" wait --for=condition=Ready pod "$POD_PG" "$POD_CH" --timeout=180s >/dev/null
  local pod
  for pod in "$POD_PG" "$POD_CH"; do
    tar -C "$RAIZ/benchmark" -c queries cliente | kubectl -n "$NS" exec -i "$pod" -- sh -c 'mkdir -p /tmp/bench && tar -x -C /tmp/bench'
  done
  kubectl -n "$NS" exec -i "$POD_PG" -- psql -X -q -v ON_ERROR_STOP=1 -d "dm_$TENANT" \
    < "$RAIZ/benchmark/views/00_views.pg.sql" >/dev/null
  sed "s/{{DB}}/dm_$TENANT/g" "$RAIZ/benchmark/views/00_views.ch.sql" |
    kubectl -n "$NS" exec -i "$POD_CH_SRV" -- clickhouse-client --multiquery >/dev/null
  garantir_usuario_bench
}

quiesce_ativo() {
  [[ "$(kubectl -n airflow get statefulset airflow-scheduler -o jsonpath='{.spec.replicas}')" == 0 &&
     "$(kubectl -n "$NS" get statefulset mongodb -o jsonpath='{.spec.replicas}')" == 0 ]]
}

# O Postgres imprime numeric com as casas da coluna e o ClickHouse corta zeros; timestamps idem.
normalizar() {
  gawk -F'\t' -v OFS='\t' '
    function n(v,   p, f) {
      if (v == "\\N") return ""
      if (v ~ /^-?[0-9]+(\.[0-9]+)?$/) return sprintf("%.3f", v)
      if (v ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?$/) {
        p = index(v, "."); f = p ? substr(v, p + 1) : ""
        return substr(v, 1, 19) "." substr(f "000", 1, 3)
      }
      return v
    }
    { for (i = 1; i <= NF; i++) $i = n($i); print }' | LC_ALL=C sort
}

# Números com tolerância de 0,0011: avg e stddev são Float64 no ClickHouse e numeric no Postgres,
# e o arredondamento na terceira casa pode divergir em uma unidade.
comparar_arquivos() {
  gawk -F'\t' -v rotulo="$3" '
    NR == FNR { a[FNR] = $0; na = FNR; next }
    {
      nb = FNR
      if (!(FNR in a)) { print rotulo ": linha " FNR " só existe em um lado"; ruim = 1; exit }
      split(a[FNR], x, "\t"); m = split($0, y, "\t")
      for (i = 1; i <= m; i++) {
        num = x[i] ~ /^-?[0-9]+\.[0-9]+$/ && y[i] ~ /^-?[0-9]+\.[0-9]+$/
        if ((num && (x[i] - y[i] > 0.0011 || y[i] - x[i] > 0.0011)) || (!num && x[i] != y[i])) {
          print rotulo ": linha " FNR ", coluna " i ": " x[i] " x " y[i]; ruim = 1; exit
        }
      }
    }
    END { if (!ruim && na != nb) { print rotulo ": " na " x " nb " linhas"; ruim = 1 } exit ruim }' "$1" "$2"
}

comparar_dir() {
  local dir="$1" q ruim=0 ref b
  for q in "${QS[@]}"; do
    ref="$dir/${q}_pg.tsv"
    [[ -f "$ref" ]] || continue
    for b in pgt ch; do
      [[ -f "$dir/${q}_$b.tsv" ]] || continue
      if comparar_arquivos "$ref" "$dir/${q}_$b.tsv" "$q pg x $b"; then
        echo "OK     $q pg x $b ($(wc -l < "$ref") linhas)"
      else
        ruim=1
      fi
    done
  done
  return "$ruim"
}

paridade() {
  local dir="${DIR_PARIDADE:-$(mktemp -d)}" q b remoto
  mkdir -p -- "$dir"
  for q in "${QS[@]}"; do
    for b in "${BS[@]}"; do
      remoto="/tmp/bench/par_${q}_$b.tsv"
      executar "$b" "$q" "t32-paridade-$b-$q" "$remoto" >/dev/null || die "execução de $q em $b falhou"
      # O stream do kubectl exec cai em transferências grandes (q05 tem ~80 MB): comprime e tenta de novo.
      for tentativa in 1 2 3; do
        kubectl -n "$NS" exec "$(pod_do "$b")" -- gzip -c "$remoto" | gunzip | normalizar > "$dir/${q}_$b.tsv" && break
        (( tentativa < 3 )) || die "transferência da saída de $q em $b falhou 3 vezes"
      done
    done
  done
  echo "saídas normalizadas em $dir"
  comparar_dir "$dir" || { echo "RESULTADO: paridade DIVERGENTE" >&2; exit 1; }
  echo "RESULTADO: paridade OK"
}

reiniciar_pg() {
  kubectl -n "$NS" delete pod postgres-0 --wait=true >/dev/null
  local i
  for i in $(seq 1 60); do
    kubectl -n "$NS" get pod postgres-0 >/dev/null 2>&1 && break
    sleep 2
  done
  kubectl -n "$NS" wait --for=condition=Ready pod/postgres-0 --timeout=300s >/dev/null
  for i in $(seq 1 30); do
    kubectl -n "$NS" exec "$POD_PG" -- pg_isready -q && return 0
    sleep 2
  done
  die "Postgres não voltou depois do reinício"
}

# "<períodos> <estrangulados> <ms estrangulado>" do cgroup do pod cliente, v1 ou v2.
cpu_stat() {
  kubectl -n "$NS" exec "$1" -- sh -c 'cat /sys/fs/cgroup/cpu/cpu.stat 2>/dev/null || cat /sys/fs/cgroup/cpu.stat' |
    awk '/^nr_periods/ { p = $2 } /^nr_throttled/ { t = $2 } /^throttled_time/ { ms = $2 / 1e6 }
         /^throttled_usec/ { ms = $2 / 1e3 } END { printf "%d %d %.0f\n", p, t, ms }'
}

derrubar_cache_ch() {
  kubectl -n "$NS" exec "$POD_CH_SRV" -- clickhouse-client --multiquery \
    -q "SYSTEM DROP MARK CACHE; SYSTEM DROP UNCOMPRESSED CACHE; SYSTEM DROP MMAP CACHE" >/dev/null
}

medir() {
  local perfil=small ts run csv q b linhas r i
  if ! quiesce_ativo; then
    (( RUIDO )) || die "o cluster não está em quiesce. Rode 'bash scripts/profile.sh quiesce' ou use --permitir-ruido."
    perfil=small-ruidoso
  fi
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p -- "$SAIDA"
  csv="$SAIDA/${ts}_${MODO}_c${C}.csv"
  echo "ts,perfil,engine,tenant,query,repeticao,modo,concorrencia,duracao_ms,read_rows,read_bytes,result_rows" > "$csv"
  estr="$SAIDA/throttle_${ts}_${MODO}_c${C}.csv"
  echo "ts,engine,query,modo,concorrencia,periodos,estrangulados,estrangulado_ms" > "$estr"
  # Um braço inteiro por vez: pg e pgt dividem um cache menor que o dado dos dois, e alternar os
  # braços a cada consulta faz um expulsar o cache do outro.
  for b in "${BS[@]}"; do
    for q in "${QS[@]}"; do
      if [[ "$MODO" == quente ]]; then
        r="$(executar "$b" "$q" "t32-$ts-aquece1-$b-$q" "/tmp/bench/aquece_${q}_$b.tsv")" || die "aquecimento de $q em $b falhou"
        linhas="${r#* }"
        executar "$b" "$q" "t32-$ts-aquece2-$b-$q" >/dev/null || die "aquecimento de $q em $b falhou"
        read -r p0 t0 ms0 <<< "$(cpu_stat "$(pod_do "$b")")"
        no_pod "$(pod_do "$b")" "RUN_ID=$ts" "PERFIL=$perfil" "MODO=quente" "CONC=$C" "RESULT_ROWS=$linhas" \
          sh -c "seq $C | xargs -P $C -n 1 sh /tmp/bench/cliente/fluxo.sh $b $q $R" >> "$csv" \
          || die "medição de $q em $b falhou"
        read -r p1 t1 ms1 <<< "$(cpu_stat "$(pod_do "$b")")"
        echo "$ts,$b,$q,quente,$C,$((p1 - p0)),$((t1 - t0)),$((ms1 - ms0))" >> "$estr"
      else
        for i in $(seq 1 "$R"); do
          if [[ "$b" == ch ]]; then derrubar_cache_ch; else reiniciar_pg; fi
          r="$(executar "$b" "$q" "t32-$ts-frio-$b-$q-$i")" || die "execução fria de $q em $b falhou"
          printf '%s,%s,%s,%s,%s,%d,frio,1,%s,,,\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$perfil" "$b" \
            "$TENANT" "$q" "$i" "${r%% *}" >> "$csv"
        done
      fi
      echo "$q $b: $(awk -F, -v b="$b" -v q="$q" '$3 == b && $5 == q' "$csv" | wc -l) execuções" >&2
    done
  done
  echo "$csv"
}

instrumentar() {
  local ts csv q b schema json saida
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p -- "$SAIDA"
  csv="$SAIDA/instr_${ts}.csv"
  echo "ts,engine,tenant,query,exec_ms,read_rows,read_bytes,mem_bytes,shared_hit,shared_read,result_rows" > "$csv"
  for q in "${QS[@]}"; do
    for b in "${BS[@]}"; do
      if [[ "$b" == ch ]]; then
        saida="$(executar ch "$q" "t32-$ts-instr-ch-$q" /tmp/bench/instr.tsv --eventos)" || die "instrumentação de $q em ch falhou"
        gawk -v ts="$ts" -v t="$TENANT" -v q="$q" '
          NR == 1 { ms = $1; rows = $2 }
          $1 == "EVENTO" { e[$2] = $3 }
          END { printf "%s,ch,%s,%s,%s,%s,%s,%s,,,%s\n", ts, t, q, ms, e["SelectedRows:"], e["SelectedBytes:"],
                e["MemoryTrackerPeakUsage:"], rows }' <<< "$saida" >> "$csv"
      else
        schema="$(schema_do "$b")"
        json="$( { printf 'SET search_path TO %s;\nEXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)\n' "$schema"
                   cat "$RAIZ/benchmark/queries/$(basename "$(arquivo_sql "$q" pg)")"; } |
                 no_pod "$POD_PG" psql -X -A -t -q -v ON_ERROR_STOP=1 -d "dm_$TENANT" \
                   -v filial=LIMEIRA -v banco=dw_limeira \
                   -v "mes_ini=2026-08-01 00:00:00" -v "mes_fim=2026-09-01 00:00:00" \
                   -v "tri_ini=2026-07-01 00:00:00" -v "tri_fim=2026-10-01 00:00:00" \
                   -v "sem_ini=2026-08-24 00:00:00" -v "sem_fim=2026-08-31 00:00:00")" || die "EXPLAIN de $q em $b falhou"
        jq -r --arg ts "$ts" --arg b "$b" --arg t "$TENANT" --arg q "$q" '
          .[0] as $e | $e.Plan as $p |
          ([$p | .. | objects | select(has("Relation Name")) |
            ((."Actual Rows" + (."Rows Removed by Filter" // 0) + (."Rows Removed by Index Recheck" // 0))
             * ."Actual Loops")] | add // 0) as $lidas |
          [$ts, $b, $t, $q, $e."Execution Time", $lidas,
           (($p."Shared Hit Blocks" + $p."Shared Read Blocks") * 8192), "",
           $p."Shared Hit Blocks", $p."Shared Read Blocks", $p."Actual Rows"] | map(tostring) | join(",")' \
          <<< "$json" >> "$csv"
      fi
    done
  done
  echo "$csv"
}

case "$ACAO" in
  comparar)
    [[ -d "$DIR_PARIDADE" ]] || die "diretório inexistente: $DIR_PARIDADE"
    comparar_dir "$DIR_PARIDADE" || { echo "RESULTADO: paridade DIVERGENTE" >&2; exit 1; }
    echo "RESULTADO: paridade OK" ;;
  paridade) preparar; paridade ;;
  instrumentar) preparar; instrumentar ;;
  medir) preparar; medir ;;
esac
