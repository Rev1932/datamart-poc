#!/usr/bin/env bash
# Job A — bronze_silver.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_lib.sh"
submit_spark "$ROOT/infra/spark/sparkapplication-normalize.yaml" "normalize-dw-andon-peso"
