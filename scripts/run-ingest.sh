#!/usr/bin/env bash
# Job C — datamart (gold -> ClickHouse). Job medido no benchmark de merge.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_lib.sh"
submit_spark "$ROOT/infra/spark/sparkapplication-ingest.yaml" "datamart-fact-200-cep"
