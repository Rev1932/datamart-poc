#!/usr/bin/env bash
# Funções compartilhadas pelos scripts run-*.sh
set -euo pipefail

# submit_spark <manifest> <sparkapplication-name>
# (Re)submete uma SparkApplication e aguarda COMPLETED/FAILED, imprimindo logs do driver.
submit_spark() {
  local manifest="$1" name="$2" ns="datamart"

  kubectl -n "$ns" delete sparkapplication "$name" --ignore-not-found
  kubectl apply -f "$manifest"

  echo ">> aguardando $name ..."
  local state=""
  for _ in $(seq 1 120); do
    state="$(kubectl -n "$ns" get sparkapplication "$name" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)"
    case "$state" in
      COMPLETED) echo ">> $name COMPLETED"; return 0 ;;
      FAILED|SUBMISSION_FAILED|FAILING|INVALIDATING)
        echo ">> $name estado=$state — logs do driver:" >&2
        kubectl -n "$ns" logs "${name}-driver" --tail=100 || true
        return 1 ;;
    esac
    sleep 10
  done
  echo ">> timeout aguardando $name (último estado=$state)" >&2
  return 1
}
