# Deploy K8s — `spark_into_postgres`

Artefatos para rodar o job via **Spark Operator (SparkApplication)** no namespace `dw-dados`,
orquestrado pelo **`SparkKubernetesOperator` do Airflow**. Visão completa em
[`../docs/deploy-kubernetes.md`](../docs/deploy-kubernetes.md).

## Arquivos

| Arquivo | Papel |
| :-- | :-- |
| `../Dockerfile` + `../requirements.txt` + `../docker/spark-defaults.conf` | Imagem do job: `FROM apache/spark:3.5.8-java17-python3` + Delta/S3A/HMS/JDBC Postgres + deps pip + código/resources. Auto-contida. |
| `configmap.yaml` | Env **não-secreto** (`_ENV_SCHEMA`). Espelha `resources/.env.example`. |
| `secret.example.yaml` | Contrato dos **segredos do job** (não versionar valores reais). |
| `sparkapplication.yaml` | Template do job (campos `<...>` por execução). |
| `dag_spark_into_postgres_example.py` | DAG config-driven de exemplo (copiar p/ `repo/dags/`). |

> **RBAC não vive aqui.** O `ServiceAccount spark` e seu Role/RoleBinding em `dw-dados` são
> criados pelo chart `spark-operator/spark-operator` (`spark.jobNamespaces` no
> `dtlk-spark-operator/values.yaml`). Manter uma cópia neste repo criaria conflito de ownership.

## Ordem de aplicação

```bash
kubectl apply -f configmap.yaml
kubectl apply -f secret.example.yaml     # substitua por Secret real (OpenBao/External Secrets)
# SparkApplication é aplicado pela DAG (SparkKubernetesOperator), não manualmente.
# Para um teste pontual, renderize os <...> e:
kubectl apply -f sparkapplication.yaml
```

## Pré-requisitos de infra

- **Spark Operator** instalado em `dw-dados` — ✅ já provisionado
  (`dtlk-k8s/dtlk-spark-operator/`), junto com o `ServiceAccount spark` e o secret
  `spark-secrets` (credenciais S3A do usuário `dw-app-spark-k8s`).
- **Imagem publicada no Harbor** como `hub.datawake.cloud/dw-dados/honeycomb:<TAG>`
  (`publish.yml` builda na release). O `imagePullSecrets: datawake-registry-secret` já existe
  no namespace.
- ⚠️ **Política de bucket no MinIO — bloqueia a execução real.** O `configmap.yaml` aponta
  `MINIO_BASE_PATH=s3a://datawake-tenant-uat`, mas o usuário `dw-app-spark-k8s` do
  `spark-secrets` só tem RW em `hive-warehouse`. É preciso **ou** ampliar a política MinIO desse
  usuário para o bucket do tenant, **ou** criar um secret de credenciais próprio do job (e trocar
  o `secretRef: spark-secrets` no manifesto). Decisão de infra.
- Imagem do **Airflow rebuildada** com `apache-airflow-providers-cncf-kubernetes`
  (já adicionado em `build/airflow/requirements.txt`).
- **NetworkPolicies** liberando egress p/ MinIO, Postgres, Trino e ClickHouse.

## Env por pipeline

- `gold_datamart` (foco): `MINIO_BASE_PATH`, `POSTGRES_*`, `CLICKHOUSE_*`, e creds S3A
  (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, vindas do `spark-secrets`). **Não** usa `TRINO_*`.
- `bronze_silver`/`gold`: adicionam `TRINO_*` e `SPARK_S3_*`.

## ⚠️ Decisões em aberto (ver docs/deploy-kubernetes.md)

- **Registro no CI (`.github/config/clients.yml`) NÃO foi feito.** O `deploy.yml` roda
  `rsync --delete` com `template` **hardcoded** = `projetos/datawake-pipeline` para qualquer
  cliente registrado — registrar este projeto ali **sobrescreveria o `src/` dele** no merge de
  uma PR. Precisa de ajuste no `deploy.yml`/`clients.yml` (ou build de imagem por outro caminho)
  antes de plugar no pipeline de imagens.
- **Template vs. cliente**: definir se imagem/manifesto vivem neste template ou no projeto-cliente
  (`datamart`).
- **Pin do provider `cncf-kubernetes`**: validar contra as constraints do Airflow 2.10.2.
- **`CLICKHOUSE_VERIFY_SSL`** aponta para `wildcard_datadriven_cloud.crt`, removido de
  `resources/` no commit `fcbb178`. Decidir: reintroduzir o cert, mudar para `true`, ou montar
  via Secret.
