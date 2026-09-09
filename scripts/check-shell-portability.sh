#!/usr/bin/env bash
# Varre docs (.md) e mensagens/comentarios dos scripts (.sh) por comandos que
# só funcionam em bash/zsh e quebram no fish (prefixo VAR=valor, loop `until`
# manual de espera de status, e `eval "$(minikube docker-env)"` sem a forma fish).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FAIL=0

VAR_PREFIX_RE='^[[:space:]]*[A-Z_][A-Z0-9_]*=[^[:space:]]+[[:space:]]+[^[:space:]]'
UNTIL_DO_DONE_RE='until[[:space:]].*;[[:space:]]*do.*done'

report() {
  echo "$1:$2: $3"
  FAIL=1
}

# Zera (sem alterar a contagem de linhas) o conteúdo de blocos ```sql, ```xml,
# ```dockerfile e ```yaml, para não gerar falso positivo nessas linguagens.
strip_excluded_fences() {
  gawk '
    BEGIN { fence = 0; skip = 0 }
    /^```/ {
      if (fence == 0) {
        fence = 1
        lang = $0
        sub(/^```/, "", lang)
        lang = tolower(lang)
        skip = (lang == "sql" || lang == "xml" || lang == "dockerfile" || lang == "yaml")
      } else {
        fence = 0
        skip = 0
      }
      print ""
      next
    }
    { print (fence == 1 && skip == 1) ? "" : $0 }
  ' "$1"
}

check_md_file() {
  local f="$1"
  local transformed
  transformed="$(strip_excluded_fences "$f")"

  while IFS=: read -r ln content; do
    [[ -z "$ln" ]] && continue
    report "$f" "$ln" "prefixo de variável antes de comando -> ${content#*:}"
  done < <(printf '%s\n' "$transformed" | grep -nE "$VAR_PREFIX_RE" | sed 's/:/:/')

  while IFS=: read -r ln content; do
    [[ -z "$ln" ]] && continue
    report "$f" "$ln" "loop until/do/done manual (use 'kubectl wait --for=jsonpath=...') -> ${content#*:}"
  done < <(printf '%s\n' "$transformed" | grep -nE "$UNTIL_DO_DONE_RE")

  if grep -qF 'eval "$(minikube docker-env)"' "$f" && ! grep -q 'docker-env --shell fish' "$f"; then
    local ln
    ln="$(grep -nF 'eval "$(minikube docker-env)"' "$f" | head -1 | cut -d: -f1)"
    report "$f" "$ln" "eval \"\$(minikube docker-env)\" sem a forma fish ao lado"
  fi
}

# Extrai só o que um humano lê nos .sh: comentários, echo/printf e corpo de heredocs.
# Ignora o resto (código real: atribuições, chamadas kubectl, etc.) para não
# confundir 'VAR="$(comando ...)"' com o padrão VAR=valor-que-o-usuário-digita.
extract_sh_messages() {
  gawk '
    BEGIN { inhd = 0; term = "" }
    {
      line = $0
      if (inhd == 1) {
        check = line
        gsub(/^\t+/, "", check)
        if (check == term) { inhd = 0; print ""; next }
        print line
        next
      }
      if (match(line, /<<-?[[:space:]]*["'"'"']?[A-Za-z_][A-Za-z0-9_]*["'"'"']?/)) {
        token = substr(line, RSTART, RLENGTH)
        gsub(/[^A-Za-z0-9_]/, "", token)
        term = token
        inhd = 1
        print ""
        next
      }
      if (line ~ /^[[:space:]]*#/ && line !~ /^#!/) { print line; next }
      if (line ~ /^[[:space:]]*(echo|printf)[[:space:](]/) { print line; next }
      print ""
    }
  ' "$1"
}

check_sh_file() {
  local f="$1"
  local transformed
  transformed="$(extract_sh_messages "$f")"

  while IFS=: read -r ln content; do
    [[ -z "$ln" ]] && continue
    report "$f" "$ln" "prefixo de variável antes de comando -> ${content#*:}"
  done < <(printf '%s\n' "$transformed" | grep -nE "$VAR_PREFIX_RE")
}

mapfile -t MD_FILES < <(
  find . -maxdepth 1 -name '*.md'
  find docs docs/epicos docs/decisoes infra -maxdepth 6 -name '*.md' 2>/dev/null
)
mapfile -t SH_FILES < <(find . -name '*.sh' -not -path './.git/*')

for f in "${MD_FILES[@]}"; do
  [[ -f "$f" ]] || continue
  check_md_file "$f"
done

for f in "${SH_FILES[@]}"; do
  check_sh_file "$f"
done

if [[ "$FAIL" -eq 0 ]]; then
  echo "OK: nenhum comando não portável para fish encontrado."
fi
exit "$FAIL"
