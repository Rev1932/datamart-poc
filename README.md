# datamart-poc

POC para validar a eficiência do **ClickHouse** como datamart de leitura rápida, com foco
no **gargalo de merge/update** durante ingestões em batch. Stack toda em **Kubernetes**
(minikube/WSL2): **MinIO** (storage), **Spark** (processamento + ingestão) e **ClickHouse**
(datamart). Especificação completa em [`.claude/specs/PLAN.md`](.claude/specs/PLAN.md).

## Fluxo

```
bronze (MinIO, Parquet)
   └─ Job A  bronze_silver  (Spark)            → silver (MinIO, Delta Lake)
        └─ Job C  datamart   (Spark)           → ClickHouse (ReplacingMergeTree)
             reusa a leitura da gold: query sobre a silver + hk_business_id (sha2-256) + load_dts,
             e grava o DataFrame no ClickHouse (append). O merge/dedup por hk_business_id
             acontece no ClickHouse — é o que a POC mede.
```

> O Job B (`gold`) grava Delta gold + registra no Trino e **não** é necessário para a POC de
> ClickHouse (o Job C lê a silver direto via `spark.sql`). Incluído por completude.

## Pré-requisitos (WSL2)

- Docker acessível (`docker ps`), `minikube`, `kubectl`, `helm` no PATH.
- Recursos: `--cpus=4 --memory=8g` (ajustável em `cluster/minikube-up.sh`).

## Subir tudo

```bash
bash scripts/bootstrap.sh
```
Sobe cluster → builda a imagem `datamart-spark:poc` no minikube → instala os operators
(Spark, ClickHouse) → sobe MinIO e a instância ClickHouse → cria a tabela `ddl/01`.

## Executar o pipeline

```bash
# 1) popular a bronze (aponte SRC_DIR para seus .parquet — ver script)
SRC_DIR=./sample/dw_andon_peso bash scripts/seed-bronze.sh

# 2) bronze -> silver (Delta)
bash scripts/run-normalize.sh

# 3) silver -> ClickHouse (datamart)
bash scripts/run-ingest.sh
```

## Benchmark de merge

```bash
N=5 SLEEP=15 bash benchmark/ingest-loop.sh
```
Dispara N ingestões (chaves sobrepostas), imprime `benchmark/merge-metrics.sql` a cada
batch e faz `OPTIMIZE ... FINAL` no fim, comparando total físico × chaves únicas.

## Layout

- `cluster/`, `infra/{minio,spark,clickhouse}/` — cluster e manifests K8s.
- `images/spark/` — Dockerfile (Spark 3.5.1 + Python 3.12 + deps).
- `spark-source-code/` — código PySpark (novo tipo `datamart` no `PipelineFactory`).
- `ddl/` — tabela ClickHouse (`ReplacingMergeTree(load_dts) ORDER BY (hk_business_id)`).
- `scripts/`, `benchmark/` — automação e medição.

## Notas / pontos de atenção

- O código exige **Python 3.12** (f-strings com aspas aninhadas) — a imagem instala 3.12.
- Jars (delta/hadoop-aws/connector ClickHouse) são resolvidos em **submit-time** via
  `spec.deps.packages` nas `SparkApplication` (requer internet no 1º run).
- A tabela em `ddl/01` é um **template** para `fact_200_cep`; ajuste colunas/tipos para
  casar com o schema real do DataFrame da gold antes do `append`.
- Segredos são placeholders de POC (`infra/minio`, `infra/spark/spark-secrets.yaml`).
