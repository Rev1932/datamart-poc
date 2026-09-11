#!/usr/bin/env bash
set -euo pipefail

NS=datamart
TENANT=acme
SEM_PGT=0
SAVE_DIR=""
FROM_DIR=""

usage() {
  cat <<'EOF'
Uso: bash benchmark/compare-counts.sh [opções]

Portão de corretude da T3.1. Compara count(*) e sum(valor) por (filial, mês)
entre os três braços do tenant:

  pg   Postgres   dm_<tenant>  public.fact_200_cep
  pgt  Postgres   dm_<tenant>  gold_tuned.fact_200_cep
  ch   ClickHouse dm_<tenant>.fact_200_cep, lido com o usuário u_<tenant>_ro

As diferenças são sempre em relação ao pg (pgt - pg, ch - pg).

Opções:
  --tenant acme|globex  tenant a comparar (padrão: acme)
  --sem-pgt             compara só pg e ch, para tenant sem o schema gold_tuned
  --save-dir DIR        grava os extratos brutos em DIR/<braço>.tsv; não sobrescreve
  --from-dir DIR        compara extratos já gravados em DIR, sem consultar os bancos
  -h, --help            mostra esta ajuda

Códigos de saída:
  0  todas as chaves presentes nos braços e delta = 0 em count e sum(valor)
  1  divergência: diferença não nula ou chave ausente em algum braço
  2  erro de uso, de extração ou de formato do extrato
EOF
}

die() { echo "ERRO: $*" >&2; exit 2; }

while (( $# )); do
  case "$1" in
    --tenant)     (( $# >= 2 )) || die "--tenant exige um valor"; TENANT="$2"; shift 2 ;;
    --tenant=*)   TENANT="${1#*=}"; shift ;;
    --sem-pgt)    SEM_PGT=1; shift ;;
    --save-dir)   (( $# >= 2 )) || die "--save-dir exige um diretório"; SAVE_DIR="$2"; shift 2 ;;
    --save-dir=*) SAVE_DIR="${1#*=}"; shift ;;
    --from-dir)   (( $# >= 2 )) || die "--from-dir exige um diretório"; FROM_DIR="$2"; shift 2 ;;
    --from-dir=*) FROM_DIR="${1#*=}"; shift ;;
    -h|--help)    usage; exit 0 ;;
    *)            die "opção desconhecida: $1 (veja --help)" ;;
  esac
done

case "$TENANT" in acme|globex) ;; *) die "tenant inválido: '$TENANT' (use acme ou globex)" ;; esac
[[ -n "$SAVE_DIR" && -n "$FROM_DIR" ]] && die "--save-dir e --from-dir não podem ser usados juntos"

BRACOS=(pg pgt ch)
(( SEM_PGT )) && BRACOS=(pg ch)

psql_q() {
  kubectl -n "$NS" exec -i postgres-0 -- \
    sh -c 'psql -U "$POSTGRES_USER" -d "$1" -XAtq -F "|" -v ON_ERROR_STOP=1' sh "dm_$TENANT"
}

extrair_pg() {
  printf 'SELECT filial, to_char("timestamp", %s), count(*), sum(valor) FROM %s.fact_200_cep GROUP BY 1, 2;\n' \
    "'YYYYMM'" "$1" | psql_q | tr '|' '\t'
}

extrair_ch() {
  local pod pwd
  pod="$(kubectl -n "$NS" get pod -l clickhouse.altinity.com/chi=datamart \
          -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "$pod" ]] || { echo "pod do ClickHouse não encontrado em $NS" >&2; return 1; }
  pwd="$(kubectl -n "$NS" get secret ch-creds -o jsonpath="{.data.${TENANT}_ro}" | base64 -d)"
  [[ -n "$pwd" ]] || { echo "chave ${TENANT}_ro ausente no Secret ch-creds" >&2; return 1; }
  kubectl -n "$NS" exec -i "$pod" -- clickhouse-client --user "u_${TENANT}_ro" --password "$pwd" -q "
    SELECT filial, toYYYYMM(timestamp) AS mes, count(), sum(valor)
    FROM dm_${TENANT}.fact_200_cep
    GROUP BY filial, mes
    FORMAT TabSeparated"
}

declare -A EXT
if [[ -n "$FROM_DIR" ]]; then
  ORIGEM="extratos em $FROM_DIR"
  for b in "${BRACOS[@]}"; do
    [[ -f "$FROM_DIR/$b.tsv" ]] || die "extrato ausente: $FROM_DIR/$b.tsv"
    EXT[$b]="$(<"$FROM_DIR/$b.tsv")"
  done
else
  ORIGEM="bancos ao vivo, tenant $TENANT"
  if (( ! SEM_PGT )); then
    existe="$(psql_q <<< "SELECT to_regclass('gold_tuned.fact_200_cep') IS NOT NULL;")" \
      || die "consulta ao Postgres falhou"
    [[ "$existe" == t ]] \
      || die "gold_tuned.fact_200_cep não existe em dm_$TENANT; aplique ddl/postgres/03_pg_tuned.sql ou use --sem-pgt"
    EXT[pgt]="$(extrair_pg gold_tuned)" || die "extração do braço pgt falhou"
  fi
  EXT[pg]="$(extrair_pg public)" || die "extração do braço pg falhou"
  EXT[ch]="$(extrair_ch)" || die "extração do braço ch falhou"
fi

for b in "${BRACOS[@]}"; do
  [[ -n "${EXT[$b]}" ]] || die "o braço $b não devolveu nenhuma linha"
done

if [[ -n "$SAVE_DIR" ]]; then
  mkdir -p -- "$SAVE_DIR"
  for b in "${BRACOS[@]}"; do
    ( set -o noclobber; printf '%s\n' "${EXT[$b]}" > "$SAVE_DIR/$b.tsv" ) \
      || die "não sobrescrevo $SAVE_DIR/$b.tsv"
  done
fi

echo "T3.1 — portão de corretude · origem: $ORIGEM · braços: ${BRACOS[*]}"
echo "diferenças em relação ao pg; '-' marca chave ausente no braço"

for b in "${BRACOS[@]}"; do
  printf '%s\n' "${EXT[$b]}" | awk -v b="$b" '{ print b "\t" $0 }'
done | awk -v bracos="${BRACOS[*]}" '
BEGIN { FS = OFS = "\t"; nb = split(bracos, B, " ") }

# O Postgres imprime numeric com 3 casas (285638509.000) e o ClickHouse corta os zeros
# (285638509). Texto nunca bate; a comparação é em milésimos inteiros.
function milesimos(v,   sinal, p, fr) {
  sinal = 1
  if (substr(v, 1, 1) == "-") { sinal = -1; v = substr(v, 2) }
  p = index(v, ".")
  fr = p ? substr(v, p + 1) : ""
  if (p) v = substr(v, 1, p - 1)
  return sinal * ((v fr substr("000", 1, 3 - length(fr))) + 0)
}

function fmt(m,   s, ip) {
  s = m < 0 ? "-" : ""
  if (m < 0) m = -m
  ip = int(m / 1000)
  return sprintf("%s%.0f.%03d", s, ip, m - ip * 1000)
}

function dif(d, ehsoma) {
  if (d == 0) return "0"
  return (d > 0 ? "+" : "") (ehsoma ? fmt(d) : sprintf("%.0f", d))
}

function formato(msg) { print "ERRO de formato: " msg > "/dev/stderr"; ruim = 1 }

{
  if (NF != 5 || $2 == "" || $3 !~ /^[0-9][0-9][0-9][0-9][0-9][0-9]$/ || $4 !~ /^[0-9]+$/ \
      || $5 !~ /^-?[0-9]+(\.[0-9][0-9]?[0-9]?)?$/) {
    formato("braço " $1 ", linha inesperada: " substr($0, length($1) + 2))
    next
  }
  k = $3 "\t" $2
  if ((k, $1) in N) { formato("braço " $1 ", chave repetida: " $3 " " $2); next }
  N[k, $1] = $4 + 0
  S[k, $1] = milesimos($5)
  if (!(k in vista)) { vista[k] = 1; K[++nk] = k }
}

function tabela(titulo, pre, ehsoma,   i, j, k, b, lin, st, v, d, w, ref, tot, totok, parte) {
  w = ehsoma ? 16 : 10
  print ""
  print titulo
  lin = sprintf("%-7s %-10s", "mes", "filial")
  for (j = 1; j <= nb; j++) lin = lin sprintf(" %" w "s", pre "_" B[j])
  for (j = 2; j <= nb; j++) lin = lin sprintf(" %" (ehsoma ? 11 : 9) "s", (ehsoma ? "dif_" : "delta_") B[j])
  print lin "  status"
  for (j = 1; j <= nb; j++) { tot[B[j]] = 0; totok[B[j]] = 1 }
  for (i = 1; i <= nk; i++) {
    k = K[i]; split(k, parte, "\t")
    lin = sprintf("%-7s %-10s", parte[1], parte[2]); st = "ok"
    for (j = 1; j <= nb; j++) {
      b = B[j]
      if ((k, b) in N) {
        v = ehsoma ? S[k, b] : N[k, b]
        tot[b] += v
        lin = lin sprintf(" %" w "s", ehsoma ? fmt(v) : sprintf("%.0f", v))
      } else {
        totok[b] = 0; st = "AUSENTE"
        lin = lin sprintf(" %" w "s", "-")
      }
    }
    for (j = 2; j <= nb; j++) {
      b = B[j]
      if ((k, "pg") in N && (k, b) in N) {
        d = ehsoma ? S[k, b] - S[k, "pg"] : N[k, b] - N[k, "pg"]
        if (d != 0) {
          if (st == "ok") st = "DIVERGE"
          falhas[++nf] = parte[1] " " parte[2] ": " (ehsoma ? "sum(valor) " : "count(*) ") b " - pg = " dif(d, ehsoma)
        }
        lin = lin sprintf(" %" (ehsoma ? 11 : 9) "s", dif(d, ehsoma))
      } else {
        lin = lin sprintf(" %" (ehsoma ? 11 : 9) "s", "-")
      }
    }
    print lin "  " st
  }
  lin = sprintf("%-7s %-10s", "total", "")
  for (j = 1; j <= nb; j++) lin = lin sprintf(" %" w "s", ehsoma ? fmt(tot[B[j]]) : sprintf("%.0f", tot[B[j]]))
  for (j = 2; j <= nb; j++) {
    d = tot[B[j]] - tot["pg"]
    lin = lin sprintf(" %" (ehsoma ? 11 : 9) "s", totok[B[j]] && totok["pg"] ? dif(d, ehsoma) : "-")
  }
  print lin
}

END {
  if (ruim) exit 2
  for (i = 2; i <= nk; i++) {
    v = K[i]
    for (j = i - 1; j > 0 && K[j] > v; j--) K[j + 1] = K[j]
    K[j + 1] = v
  }
  for (i = 1; i <= nk; i++) {
    split(K[i], parte, "\t")
    for (j = 1; j <= nb; j++)
      if (!((K[i], B[j]) in N)) falhas[++nf] = parte[1] " " parte[2] ": chave ausente no braço " B[j]
  }
  tabela("count(*) por (filial, mês)", "n", 0)
  tabela("sum(valor) por (filial, mês)", "sum", 1)
  print ""
  if (nf > 0) {
    for (i = 1; i <= nf; i++) print "FALHA: " falhas[i] > "/dev/stderr"
    print "RESULTADO: FALHA — " nf " divergência(s) em " nk " chave(s) (filial, mês)." > "/dev/stderr"
    exit 1
  }
  print "RESULTADO: OK — " nk " chaves (filial, mês) presentes nos " nb " braços; delta = 0 em count(*) e sum(valor) em toda linha."
}'
