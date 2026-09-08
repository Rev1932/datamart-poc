#!/usr/bin/env bash
# Job B — gold (usa Trino no final; opcional para a POC de ClickHouse).
set -euo pipefail

echo "AVISO: script legado da V1. Ele submete a imagem datamart-spark:poc, que nao" >&2
echo "       existe mais (a V2 usa honeycomb:poc), e argumentos do fork antigo." >&2
echo "       Superado pelas DAGs de E2/T2.6. Abortando para nao falhar em silencio." >&2
exit 1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_lib.sh"
submit_spark "$ROOT/infra/spark/sparkapplication-gold.yaml" "gold-fact-200-cep"
