#!/usr/bin/env bash
# Job B — gold (usa Trino no final; opcional para a POC de ClickHouse).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_lib.sh"
submit_spark "$ROOT/infra/spark/sparkapplication-gold.yaml" "gold-fact-200-cep"
