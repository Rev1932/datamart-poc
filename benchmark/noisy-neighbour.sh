#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS=datamart
POD_MED=bench-cliente-ch
POD_VIZ=bench-vizinho-ch
POD_CH_SRV=chi-datamart-datamart-0-0-0

DURACAO=180
PAUSA=60
AQUECIMENTO=10
RODADAS=2
LACOS=4
SAIDA="$RAIZ/benchmark/results"
RUIDO=0
AVALIAR=""
LIMIAR=1.3

PARAMS=(
  "FILIAL=LIMEIRA" "BANCO=dw_limeira"
  "MES_INI=2026-08-01 00:00:00" "MES_FIM=2026-09-01 00:00:00"
  "SEM_INI=2026-08-24 00:00:00" "SEM_FIM=2026-08-31 00:00:00"
)

usage() {
  cat <<'EOF'
Uso: bash benchmark/noisy-neighbour.sh [opções]

T3.3 — vizinho barulhento. Mede q04 e q01 do acme (u_acme_bench, uma consulta por vez) em 4 fases:
  1  sem vizinho
  2  vizinho u_globex_bench, sem cota nem limite
  3  vizinho u_globex_ro, com a cota q_globex e o perfil da T1.5
  4  vizinho u_globex_limitado, com max_threads = 1 e max_concurrent_queries_for_user = 1, sem cota
O vizinho roda <laços> consultas pesadas sem filtro em dm_globex, em paralelo, no pod bench-vizinho-ch.

Aceite: o vizinho da fase 2 degrada o p95 da q04 em 1,3× ou mais, e a fase 3 ou a 4 o traz de volta a
até 1,3× o isolado.

  --duracao S        segundos de medição por fase (padrão 180)
  --pausa S          segundos entre fases, para a janela da cota zerar (padrão 60)
  --rodadas N        repetições das 4 fases (padrão 2)
  --lacos N          consultas simultâneas do vizinho (padrão 4)
  --saida DIR        diretório dos CSVs (padrão benchmark/results)
  --permitir-ruido   roda sem quiesce
  --avaliar CSV      só avalia um vizinho_<ts>.csv já gravado, sem consultar o cluster
  -h, --help

Saída: 0 aceite; 1 reprovado ou inconclusivo; 2 erro de uso, de pré-condição ou de execução.
EOF
}

die() { echo "ERRO: $*" >&2; exit 2; }

while (( $# )); do
  case "$1" in
    --duracao) DURACAO="$2"; shift 2 ;;
    --pausa) PAUSA="$2"; shift 2 ;;
    --rodadas) RODADAS="$2"; shift 2 ;;
    --lacos) LACOS="$2"; shift 2 ;;
    --saida) SAIDA="$2"; shift 2 ;;
    --permitir-ruido) RUIDO=1; shift ;;
    --avaliar) AVALIAR="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "opção desconhecida: $1 (veja --help)" ;;
  esac
done

for v in "$DURACAO" "$PAUSA" "$RODADAS" "$LACOS"; do
  [[ "$v" =~ ^[0-9]+$ ]] || die "--duracao, --pausa, --rodadas e --lacos precisam ser inteiros"
done

rotulo() {
  case "$1" in
    1) echo "nenhum" ;;
    2) echo "u_globex_bench, sem limite" ;;
    3) echo "u_globex_ro, cota da T1.5" ;;
    4) echo "u_globex_limitado, CPU e concorrência" ;;
  esac
}

# Rank mais próximo: o p-ésimo percentil é o valor na posição ceil(p·n) da lista ordenada.
avaliar() {
  local csv="$1"
  [[ -f "$csv" ]] || die "arquivo inexistente: $csv"
  (( $(tail -n +2 "$csv" | wc -l) > 0 )) || die "$csv não tem medições"
  local tabela
  tabela="$(tail -n +2 "$csv" | sort -t, -k3,3n -k4,4 -k5,5n | gawk -F, '
    function fecha() {
      if (n == 0) return
      p50 = v[int(n * 0.5 + 0.999999)]; p95 = v[int(n * 0.95 + 0.999999)]
      print chave, n, p50, p95
    }
    { k = $3 " " $4; if (k != chave) { fecha(); chave = k; n = 0 } v[++n] = $5 }
    END { fecha() }')"

  echo "| fase | query | vizinho | n | p50 ms | p95 ms | p95 × isolado |"
  echo "|---|---|---|---:|---:|---:|---:|"
  local fase q n p50 p95 base
  while read -r fase q n p50 p95; do
    base="$(awk -v q="$q" '$1 == 1 && $2 == q { print $5 }' <<< "$tabela")"
    printf '| %s | %s | %s | %s | %.0f | %.0f | %s |\n' "$fase" "$q" "$(rotulo "$fase")" "$n" "$p50" "$p95" \
      "$(awk -v a="$p95" -v b="$base" 'BEGIN { if (b > 0) printf "%.2f×", a / b; else print "-" }')"
  done <<< "$tabela"
  echo

  local f1 f2 f3 f4
  f1="$(awk '$1 == 1 && $2 == "q04" { print $5 }' <<< "$tabela")"
  f2="$(awk '$1 == 2 && $2 == "q04" { print $5 }' <<< "$tabela")"
  f3="$(awk '$1 == 3 && $2 == "q04" { print $5 }' <<< "$tabela")"
  f4="$(awk '$1 == 4 && $2 == "q04" { print $5 }' <<< "$tabela")"
  [[ -n "$f1" && -n "$f2" && -n "$f3" && -n "$f4" ]] || die "a q04 precisa ter medição nas 4 fases"
  awk -v f1="$f1" -v f2="$f2" -v f3="$f3" -v f4="$f4" -v l="$LIMIAR" 'BEGIN {
    r2 = f2 / f1; r3 = f3 / f1; r4 = f4 / f1
    if (r2 < l) { printf "INCONCLUSIVO: o vizinho da fase 2 só levou o p95 da q04 a %.2f× o isolado; sem degradação, não há o que proteger\n", r2; exit 1 }
    p = ""
    if (r3 <= l) p = p sprintf(" fase 3 (cota da T1.5, %.2f×)", r3)
    if (r4 <= l) p = p sprintf(" fase 4 (CPU e concorrência, %.2f×)", r4)
    if (p == "") { printf "REPROVADO: o vizinho degrada a q04 a %.2f× e nenhum mecanismo a traz a %.1f× (fase 3: %.2f×, fase 4: %.2f×)\n", r2, l, r3, r4; exit 1 }
    printf "ACEITE: o vizinho degrada a q04 a %.2f× o isolado; protege a%s\n", r2, p
  }'
}

if [[ -n "$AVALIAR" ]]; then
  avaliar "$AVALIAR"
  exit $?
fi

secret_de() { kubectl -n "$NS" get secret "$1" -o jsonpath="{.data.$2}" | base64 -d; }

cpu_stat() {
  kubectl -n "$NS" exec "$1" -- sh -c 'cat /sys/fs/cgroup/cpu/cpu.stat 2>/dev/null || cat /sys/fs/cgroup/cpu.stat' |
    awk '/^nr_periods/ { p = $2 } /^nr_throttled/ { t = $2 } END { printf "%d %d\n", p, t }'
}

quiesce_ativo() {
  [[ "$(kubectl -n airflow get statefulset airflow-scheduler -o jsonpath='{.spec.replicas}')" == 0 &&
     "$(kubectl -n "$NS" get statefulset mongodb -o jsonpath='{.spec.replicas}')" == 0 ]]
}

aplicar_ch() {
  kubectl -n "$NS" exec -i "$POD_CH_SRV" -- clickhouse-client --user dm_admin \
    --password "$(secret_de ch-creds dm_admin)" --multiquery >/dev/null
}

preparar() {
  if ! kubectl -n "$NS" get pod "$POD_MED" "$POD_VIZ" bench-cliente-pg >/dev/null 2>&1; then
    kubectl apply -f "$RAIZ/benchmark/clientes.yaml" >/dev/null
  fi
  kubectl -n "$NS" wait --for=condition=Ready pod "$POD_MED" "$POD_VIZ" --timeout=180s >/dev/null
  local pod
  for pod in "$POD_MED" "$POD_VIZ"; do
    tar -C "$RAIZ/benchmark" -c queries cliente | kubectl -n "$NS" exec -i "$pod" -- sh -c 'mkdir -p /tmp/bench && tar -x -C /tmp/bench'
  done
  kubectl -n "$NS" get secret ch-bench >/dev/null 2>&1 || die "Secret ch-bench ausente: rode antes o read-bench.sh"
  sed "s/{{DB}}/dm_acme/g" "$RAIZ/benchmark/views/00_views.ch.sql" | aplicar_ch
  local t
  for t in acme globex; do
    sed -e "s/{{TENANT}}/$t/g" -e "s/{{DB}}/dm_$t/g" -e "s/{{SENHA}}/$(secret_de ch-bench "$t")/g" \
      "$RAIZ/benchmark/views/01_usuario_bench.ch.sql" | aplicar_ch
  done
  sed -e "s/{{TENANT}}/globex/g" -e "s/{{DB}}/dm_globex/g" -e "s/{{SENHA}}/$(secret_de ch-bench globex)/g" \
    "$RAIZ/benchmark/views/02_usuario_limitado.ch.sql" | aplicar_ch
}

usuario_da_fase() {
  case "$1" in
    2) echo "u_globex_bench $(secret_de ch-bench globex)" ;;
    3) echo "u_globex_ro $(secret_de ch-creds globex_ro)" ;;
    4) echo "u_globex_limitado $(secret_de ch-bench globex)" ;;
  esac
}

executar() {
  local perfil=small ts csv carga estr rodada fase usuario pw tmp p0 t0 p1 t1 pid pw_acme
  if ! quiesce_ativo; then
    (( RUIDO )) || die "o cluster não está em quiesce. Rode 'bash scripts/profile.sh quiesce' ou use --permitir-ruido."
    perfil=small-ruidoso
  fi
  preparar
  pw_acme="$(secret_de ch-bench acme)"
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p -- "$SAIDA"
  csv="$SAIDA/vizinho_${ts}.csv"
  carga="$SAIDA/vizinho_carga_${ts}.csv"
  estr="$SAIDA/vizinho_throttle_${ts}.csv"
  echo "ts,rodada,fase,query,duracao_ms" > "$csv"
  echo "rodada,fase,usuario,ok,erros,codigos" > "$carga"
  echo "rodada,fase,periodos,estrangulados,perfil" > "$estr"
  tmp="$(mktemp)"

  for rodada in $(seq 1 "$RODADAS"); do
    for fase in 1 2 3 4; do
      echo "rodada $rodada, fase $fase: $(rotulo "$fase")" >&2
      pid=""
      if (( fase > 1 )); then
        read -r usuario pw <<< "$(usuario_da_fase "$fase")"
        kubectl -n "$NS" exec "$POD_VIZ" -- env "CH_PW=$pw" \
          bash /tmp/bench/cliente/vizinho.sh "$usuario" $((AQUECIMENTO + DURACAO + 5)) "$LACOS" > "$tmp" &
        pid=$!
        sleep "$AQUECIMENTO"
      fi
      read -r p0 t0 <<< "$(cpu_stat "$POD_MED")"
      kubectl -n "$NS" exec "$POD_MED" -- env TENANT=acme CH_USER=u_acme_bench "CH_PW=$pw_acme" "RUN_ID=$ts" "${PARAMS[@]}" \
        bash /tmp/bench/cliente/medir_por_tempo.sh "$DURACAO" "$rodada" "$fase" >> "$csv" \
        || die "medição da fase $fase falhou"
      read -r p1 t1 <<< "$(cpu_stat "$POD_MED")"
      echo "$rodada,$fase,$((p1 - p0)),$((t1 - t0)),$perfil" >> "$estr"
      if [[ -n "$pid" ]]; then
        wait "$pid" || die "o vizinho da fase $fase falhou"
        gawk -v r="$rodada" -v f="$fase" -v u="$usuario" '
          $1 == "OK" { ok += $2 } $1 == "ERRO" { n++; c[$2]++ }
          END { s = ""; for (k in c) s = s (s == "" ? "" : " ") k ":" c[k]; printf "%s,%s,%s,%d,%d,%s\n", r, f, u, ok, n, s }' \
          "$tmp" >> "$carga"
      fi
      (( rodada == RODADAS && fase == 4 )) || sleep "$PAUSA"
    done
  done
  rm -f -- "$tmp"

  echo
  echo "Carga do vizinho (códigos: 201 cota excedida, 202 concorrência, 159 tempo, 241 memória):"
  column -s, -t "$carga"
  echo
  echo "Estrangulamento de CPU do pod que mede:"
  column -s, -t "$estr"
  echo
  avaliar "$csv"
}

executar
