#!/usr/bin/env bash
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS=datamart
POD=bench-cliente-ch
DIR="$RAIZ/benchmark/results"
ACEITE=0

usage() {
  cat <<'EOF'
Uso: bash benchmark/report.sh [--dir DIR] [--aceite]

Agrega os CSVs do read-bench.sh (os instr_*.csv ficam de fora) com clickhouse local, no pod
bench-cliente-ch. Cada arquivo é uma rodada. Percentis pelo rank mais próximo.

  --dir DIR   diretório dos CSVs (padrão benchmark/results)
  --aceite    sai com 1 se o p50 de alguma consulta e braço, no modo quente com -c 1, variar 30 % ou
              mais entre rodadas. O p95 aparece como faixa; -c > 1 é indicativo e não reprova
  -h, --help

Saída: 0 ok; 1 aceite reprovado; 2 erro ou nenhuma medição.
EOF
}

die() { echo "ERRO: $*" >&2; exit 2; }

while (( $# )); do
  case "$1" in
    --dir) DIR="$2"; shift 2 ;;
    --aceite) ACEITE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "opção desconhecida: $1 (veja --help)" ;;
  esac
done

[[ -d "$DIR" ]] || die "diretório inexistente: $DIR"
mapfile -t ARQS < <(cd "$DIR" && find . -maxdepth 1 -name '*.csv' ! -name 'instr_*' ! -name 'throttle_*' -printf '%f\n' | sort)
mapfile -t ESTR < <(cd "$DIR" && find . -maxdepth 1 -name 'throttle_*.csv' -printf '%f\n' | sort)
(( ${#ARQS[@]} )) || die "nenhum CSV de medição em $DIR"

if ! kubectl -n "$NS" get pod "$POD" >/dev/null 2>&1; then
  kubectl apply -f "$RAIZ/benchmark/clientes.yaml" >/dev/null
fi
kubectl -n "$NS" wait --for=condition=Ready pod "$POD" --timeout=180s >/dev/null

REMOTO="/tmp/rep-$(date +%s%N)"
tar -C "$DIR" -c "${ARQS[@]}" | kubectl -n "$NS" exec -i "$POD" -- sh -c "mkdir -p $REMOTO && tar -x -C $REMOTO"
if (( ${#ESTR[@]} )); then
  tar -C "$DIR" -c "${ESTR[@]}" | kubectl -n "$NS" exec -i "$POD" -- sh -c "mkdir -p $REMOTO/estr && tar -x -C $REMOTO/estr"
fi

ESQUEMA="ts String, perfil String, engine String, tenant String, query String, repeticao UInt32, modo String, concorrencia UInt32, duracao_ms Float64, read_rows Nullable(UInt64), read_bytes Nullable(UInt64), result_rows Nullable(UInt64)"
FONTE="file('$REMOTO/*.csv', CSVWithNames, '$ESQUEMA')"

local_q() {
  kubectl -n "$NS" exec "$POD" -- clickhouse local --output_format_decimal_trailing_zeros=1 -q "$1"
}

n="$(local_q "SELECT count() FROM $FONTE")"
[[ "$n" != 0 ]] || die "os CSVs em $DIR não têm nenhuma medição"

# Rank mais próximo: o p-ésimo percentil é o valor na posição ceil(p·n) da lista ordenada.
POR_RODADA="
  SELECT query, modo, concorrencia AS c, engine, replaceRegexpOne(_file, '\\\\.csv\$', '') AS rodada,
         arraySort(groupArray(duracao_ms)) AS a, length(a) AS n,
         a[toUInt64(ceil(0.5 * n))] AS p50, a[toUInt64(ceil(0.95 * n))] AS p95
  FROM $FONTE GROUP BY query, modo, c, engine, rodada"

POR_BRACO="
  SELECT query, modo, concorrencia AS c, engine,
         arraySort(groupArray(duracao_ms)) AS a, length(a) AS n,
         a[toUInt64(ceil(0.5 * n))] AS p50, a[toUInt64(ceil(0.95 * n))] AS p95
  FROM $FONTE GROUP BY query, modo, c, engine"

d1() { echo "ifNull(toString(toDecimal64($1, 1)), '-')"; }

echo "## Latência por rodada (ms)"
echo
local_q "SELECT query, modo, c, engine, toDecimal64(p50, 1) AS p50, toDecimal64(p95, 1) AS p95, n, rodada
         FROM ($POR_RODADA) ORDER BY query, modo, c, engine, rodada FORMAT Markdown"
echo
echo "## Consolidado entre rodadas (ms) — fator p95 pg/ch e pgt/ch"
echo
local_q "SELECT query, modo, c,
           $(d1 "anyIfOrNull(p50, engine = 'pg')") AS \`p50 pg\`, $(d1 "anyIfOrNull(p95, engine = 'pg')") AS \`p95 pg\`,
           $(d1 "anyIfOrNull(p50, engine = 'pgt')") AS \`p50 pgt\`, $(d1 "anyIfOrNull(p95, engine = 'pgt')") AS \`p95 pgt\`,
           $(d1 "anyIfOrNull(p50, engine = 'ch')") AS \`p50 ch\`, $(d1 "anyIfOrNull(p95, engine = 'ch')") AS \`p95 ch\`,
           $(d1 "anyIfOrNull(p95, engine = 'pg') / nullIf(anyIfOrNull(p95, engine = 'ch'), 0)") AS \`fator p95 pg/ch\`,
           $(d1 "anyIfOrNull(p95, engine = 'pgt') / nullIf(anyIfOrNull(p95, engine = 'ch'), 0)") AS \`fator p95 pgt/ch\`
         FROM ($POR_BRACO) GROUP BY query, modo, c ORDER BY query, modo, c FORMAT Markdown"
echo
echo "## Barras de p95 (ms)"
echo
local_q "SELECT query, modo, c, engine,
           concat(repeat('█', toUInt64(greatest(1, round(p95 / max(p95) OVER (PARTITION BY query, modo, c) * 40)))),
                  ' ', toString(toDecimal64(p95, 1))) AS p95
         FROM ($POR_BRACO) ORDER BY query, modo, c, engine FORMAT Markdown"
echo
echo "## Estabilidade entre rodadas (aceite: desvio do p50 < 30 % no modo quente com -c 1; -c > 1 é indicativo)"
echo
ESTAB="
  SELECT query, modo, c, engine, count() AS rodadas,
         toDecimal64(min(p50), 1) AS \`p50 mín\`, toDecimal64(max(p50), 1) AS \`p50 máx\`,
         toDecimal64((max(p50) - min(p50)) / min(p50) * 100, 1) AS \`desvio p50 %\`,
         toDecimal64(min(p95), 1) AS \`p95 mín\`, toDecimal64(max(p95), 1) AS \`p95 máx\`,
         toDecimal64((max(p95) - min(p95)) / min(p95) * 100, 1) AS \`desvio p95 %\`,
         multiIf(count() < 2, 'n/a', c > 1, 'indicativo',
                 (max(p50) - min(p50)) / min(p50) * 100 < 30, 'OK', 'FALHA') AS status
  FROM ($POR_RODADA) GROUP BY query, modo, c, engine"
local_q "SELECT * FROM ($ESTAB) ORDER BY query, modo, c, engine FORMAT Markdown"

if (( ${#ESTR[@]} )); then
  echo
  echo "## Estrangulamento de CPU dos pods clientes (validade da medição)"
  echo
  local_q "SELECT replaceRegexpOne(_file, '^throttle_(.*)\\\\.csv\$', '\\\\1') AS rodada, engine,
             sum(periodos) AS \`nº períodos\`, sum(estrangulados) AS \`nº estrangulados\`,
             toDecimal64(100 * sum(estrangulados) / greatest(sum(periodos), 1), 1) AS \`% estrangulado\`,
             toDecimal64(sum(estrangulado_ms) / 1000, 1) AS \`s parado\`
           FROM file('$REMOTO/estr/*.csv', CSVWithNames,
                     'ts String, engine String, query String, modo String, concorrencia UInt32, periodos UInt64, estrangulados UInt64, estrangulado_ms UInt64')
           GROUP BY rodada, engine ORDER BY rodada, engine FORMAT Markdown"
fi

if (( ACEITE )); then
  falhas="$(local_q "SELECT query, modo, c, engine, \`desvio p50 %\` FROM ($ESTAB) WHERE modo = 'quente' AND status = 'FALHA'
                     ORDER BY query, c, engine FORMAT TSV")"
  echo
  if [[ -n "$falhas" ]]; then
    while IFS=$'\t' read -r q m c b d; do echo "FALHA de estabilidade: $q $m c=$c $b — desvio de p50 $d %"; done <<< "$falhas"
    exit 1
  fi
  echo "ACEITE: desvio de p50 < 30 % em toda consulta e braço do modo quente com -c 1"
fi
