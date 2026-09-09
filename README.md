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

## Acessar os serviços

```bash
bash scripts/ports.sh              # abre as 7 portas em segundo plano; o terminal fica livre
bash scripts/ports.sh --status     # o que está no ar
bash scripts/ports.sh --creds      # usuários e senhas
bash scripts/ports.sh --stop       # derruba tudo
```

| Chave | Endereço | Serviço |
|---|---|---|
| `console` | http://localhost:9001 | Console do MinIO |
| `s3` | http://localhost:9000 | Endpoint S3 do MinIO |
| `airflow` | http://localhost:8080 | Webserver do Airflow |
| `clickhouse` | http://localhost:8123 | ClickHouse HTTP |
| `ch-native` | `localhost:9010` | ClickHouse, protocolo nativo |
| `postgres` | `localhost:5432` | PostgreSQL do braço de comparação |
| `mongo` | `localhost:27017` | MongoDB do control plane |

Cada porta roda sob um supervisor que reabre o encaminhamento sozinho quando a conexão cai — o que
acontece a cada restart de pod. Use `--only console,s3` para agir sobre um subconjunto.

Não há acesso por nome (`console.dtm.test` e afins): o caminho foi pesquisado e descartado — ver
[docs/pesquisa-ingress-dns-wsl2.md](docs/pesquisa-ingress-dns-wsl2.md).

## Executar o pipeline

```bash
# 1) popular a bronze (aponte SRC_DIR para seus .parquet — ver script)
bash scripts/seed-bronze.sh --src-dir ./sample/dw_andon_peso

# 2) bronze -> silver (Delta)
bash scripts/run-normalize.sh

# 3) silver -> ClickHouse (datamart)
bash scripts/run-ingest.sh
```

## Benchmark de merge

```bash
env N=5 SLEEP=15 bash benchmark/ingest-loop.sh
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
