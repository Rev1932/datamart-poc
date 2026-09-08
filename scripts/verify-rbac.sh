#!/usr/bin/env bash
# Prova o isolamento multi-tenant por asserções NEGATIVAS. Um teste que só afirma o
# caminho feliz passa mesmo quando o tenant tem privilégio a mais.
set -uo pipefail

NS=datamart
FAILURES=0

ok()   { echo "  [OK]    $*"; }
fail() { echo "  [FALHA] $*" >&2; FAILURES=$((FAILURES + 1)); }

POD="$(kubectl -n "$NS" get pod -l clickhouse.altinity.com/chi=datamart \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
[[ -n "$POD" ]] || { echo "ERRO: pod do ClickHouse não encontrado no namespace $NS." >&2; exit 1; }

secret_get() {
  kubectl -n "$NS" get secret ch-creds -o jsonpath="{.data.$1}" | base64 -d
}

ADMIN_PWD="$(secret_get dm_admin)"
ACME_RO_PWD="$(secret_get acme_ro)"

ch() {
  local user="$1" pwd="$2" query="$3"
  kubectl -n "$NS" exec -i "$POD" -- \
    clickhouse-client --user "$user" --password "$pwd" -q "$query" 2>&1
}

assert_denied() {
  local desc="$1" user="$2" pwd="$3" query="$4" out
  if out="$(ch "$user" "$pwd" "$query")"; then
    fail "$desc — o comando teve SUCESSO e não deveria"
    return
  fi
  if grep -q "ACCESS_DENIED" <<< "$out"; then
    ok "$desc — negado com ACCESS_DENIED (497)"
  else
    fail "$desc — falhou, mas não por ACCESS_DENIED: $(head -2 <<< "$out")"
  fi
}

assert_setting() {
  local desc="$1" user="$2" pwd="$3" name="$4" expected="$5" out
  out="$(ch "$user" "$pwd" "SELECT value FROM system.settings WHERE name = '$name'" | tr -d '[:space:]')"
  if [[ "$out" == "$expected" ]]; then
    ok "$desc — $name = $expected"
  else
    fail "$desc — $name = '$out', esperado '$expected'"
  fi
}

echo ">> preparando tabelas-sonda (como dm_admin)"
for db in dm_acme dm_globex; do
  ch dm_admin "$ADMIN_PWD" \
    "CREATE TABLE IF NOT EXISTS ${db}.__rbac_probe (x UInt8) ENGINE = MergeTree ORDER BY x" >/dev/null \
    || { echo "ERRO: não foi possível criar a sonda em $db — o Job ch-rbac rodou?" >&2; exit 1; }
done

echo ">> asserções"
assert_denied "1. u_acme_ro não escreve no próprio database" \
  u_acme_ro "$ACME_RO_PWD" "INSERT INTO dm_acme.__rbac_probe VALUES (1)"

assert_denied "2. u_acme_ro não lê o database de outro tenant" \
  u_acme_ro "$ACME_RO_PWD" "SELECT count() FROM dm_globex.__rbac_probe"

assert_setting "3. perfil do leitor" u_acme_ro "$ACME_RO_PWD" readonly 2
assert_setting "4. perfil do leitor" u_acme_ro "$ACME_RO_PWD" join_use_nulls 1

echo
if (( FAILURES > 0 )); then
  echo "RBAC: $FAILURES asserção(ões) falharam." >&2
  exit 1
fi
echo "RBAC: 4/4 asserções passaram."
