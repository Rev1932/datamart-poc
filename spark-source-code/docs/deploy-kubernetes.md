# Deploy em Kubernetes — `spark_into_postgres`

> **Escopo:** análise e sugestões de implementação dos artefatos de deploy/manifesto K8s,
> segregadas por tema. Cobre os impeditivos de infra I3–I11 de [`pendencias-kubernetes.md`](./pendencias-kubernetes.md).
> **Última atualização:** 2026-07-20 · branch `feat/spark_postgres`.
>
> **Modelo definido:** execução via **Spark Operator (`SparkApplication`)** em cluster mode,
> orquestrada pelo **`SparkKubernetesOperator` do Airflow**.

O código já está pronto para K8s: config 100% via variáveis de ambiente
(`_ENV_SCHEMA` em `src/main/utils/config_manager.py`), logger degradável (I1) e sem drift de
campos (B2/I2). O que falta são os **artefatos de deploy**, inexistentes no repositório.

## Ponto de partida: a imagem auto-contida

A antiga base `datawake-spark:3.5` (bitnami, build local) **deixou de existir** e quebrava o
build. A imagem foi reconstruída a partir do `apache/spark` oficial, reaproveitando a stack já
validada em produção pela POC `dtlk-k8s/dtlk-spark-operator/docker/Dockerfile`:

| Recurso | Origem | Observação |
| :-- | :-- | :-- |
| Spark 3.5.8 + Python 3.10 | `apache/spark:3.5.8-java17-python3` | roda como uid 185 (`runAsNonRoot`) |
| JARs Delta 3.3.2 (`delta-spark_2.12`, `delta-storage`) | `ADD` do Maven Central | combo estável p/ Spark 3.5.x (Scala 2.12) |
| JARs S3A (`hadoop-aws 3.3.4` + `aws-java-sdk-bundle 1.12.262`) | `ADD` do Maven Central | versões casam com o Hadoop do Spark 3.5.x |
| JAR JDBC Postgres 42.7.4 | `ADD` do Maven Central | **necessário** p/ `repo/repository.py:168` (`.write.format("jdbc")`) |
| Cliente Hive Metastore 3.1.3 | estágio `hive-client-jars` (ivy) | contorna `Invalid method name: 'get_table'` contra o metastore Hive 4.2 |
| Pacotes pip (`pyhocon`, `requests`, `minio`, `trino`, `psycopg2-binary`) | `requirements.txt` | `delta-spark` vai a parte com `--no-deps` (senão arrasta um pyspark duplicado) |
| Credenciais S3A | secret `spark-secrets` | **nunca** na imagem — `EnvironmentVariableCredentialsProvider` |

O honeycomb acessa Delta **100% por caminho** (`delta.\`s3a://…\``), nunca via catálogo Hive —
o cliente HMS entra por paridade com a plataforma e uso futuro pelo Trino.

> ⚠️ **Python 3.10.** A imagem oficial do Spark traz 3.10, enquanto o ambiente de dev é 3.12.
> Aspas duplas aninhadas em f-string (PEP 701) compilam em 3.12 e estouram `SyntaxError` no pod
> — foi o que aconteceu em `core/pipeline_orchestrator.py:63` e `utils/trino_connection.py:155`.
> O `ci.yml` agora fixa `python-version: "3.10"` no `py_compile` para pegar isso na PR.

Fonte da verdade do contrato de env: `_ENV_SCHEMA` + [`resources/.env.example`](../resources/.env.example).

---

## Tema 1 — Imagem Docker do job

**Situação:** sem `Dockerfile`/`requirements.txt`/`jars/`. O padrão dos jobs simples irmãos
(`engrecon_cep`/`bruning`: `openjdk:11-slim` + pip, local mode) não serve para cluster mode.

**Implementado — `Dockerfile` multi-stage e auto-contido na raiz** (ver tabela acima):
- **Estágio `hive-client-jars`**: resolve o cliente HMS 3.1.3 via ivy → `/opt/hive-metastore-jars`.
- **Estágio de runtime**: `ADD` dos 5 JARs em `/opt/spark/jars/`, `docker/spark-defaults.conf`,
  `pip install -r requirements.txt` + `delta-spark==${DELTA_VERSION}` com `--no-deps`.
- **Código + assets**: `COPY src/` e `COPY resources/` para `/opt/spark/app/` — `resources/`
  **irmão** de `src/` (auto-discovery do `config_base_path`).
  `ENV PYTHONPATH=/opt/spark/app/src/main:/opt/spark/python:/opt/spark/python/lib/py4j-*.zip`
  (a base **não** instala pyspark em `site-packages`; sem os dois últimos, um `python3 main.py`
  fora do `spark-submit` quebra em `import pyspark`).
- `USER spark` (uid 185) no final, e `.dockerignore` barrando `__pycache__/` do host.
- **Verificado** (imagem 1.73 GB): build OK; os 5 JARs presentes; 209 jars do cliente HMS;
  `uid=185(spark)`; `import delta, pyspark(3.5.8), pyhocon, minio, trino, psycopg2` OK;
  `ConfigManager().config_base_path` → `/opt/spark/app/resources`.

**⚠️ Constraint de CI — resolvido.** Como a imagem não depende mais de nenhuma base interna, o
`publish.yml` builda direto no runner do GitHub. O `build-args: SPARK_BASE_IMAGE=…` dos workflows
foi removido (era órfão: o Dockerfile declarava `ARG BASE_IMAGE`, então o build-arg era ignorado).

**Resolve:** I3, I4, I10.

## Tema 2 — Build & release no CI/CD existente

**Situação:** `publish.yml` já builda/pusha imagem por cliente ao achar `Dockerfile` na raiz +
registro em `.github/config/clients.yml`; release por `.releaserc.json`.

**Sugestão:** registrar o projeto em `clients.yml` (`project_path`, `label`, `group`,
`environments`) → `publish.yml` gera `{REGISTRY}/{NS}/<img>:{versão}`. Como este é um **template**
(o irmão `datamart` é a instância), decidir se a imagem/manifesto vivem no **template** (herdados
via `deploy.yml`/rsync) ou **por cliente**. Recomendação: no template, com manifesto parametrizável.

**Resolve:** I4.

## Tema 3 — Manifesto `SparkApplication` (artefato central)

**Implementado — `k8s/sparkapplication.yaml`**, alinhado ao contrato da plataforma `dw-dados`
(referência: `dtlk-spark-operator/USER_SPARK_K8S.md` e `examples/delta-roundtrip.yaml`):
- `namespace: dw-dados`, `image: hub.datawake.cloud/dw-dados/honeycomb:<TAG>`,
  `imagePullSecrets: [datawake-registry-secret]`, `sparkVersion: 3.5.8`,
  `mainApplicationFile: local:///opt/spark/app/src/main/main.py`.
- **Placement obrigatório**: `nodeSelector` do `nodepool-highperformance` (nível da app, aplica a
  driver e executors) + toleration `service-type=high-memory:NoSchedule` em ambos. Pods ficam
  `Pending` por 2-5 min enquanto o autoscaler sobe um nó — **não é falha**.
- `arguments`: `--pipeline gold_datamart --tenant_name … --filial_name … --table_name … --primary_key …` (I9).
- `sparkConf`: `EnvironmentVariableCredentialsProvider` (alinhado a `session.py:31-33`) e
  `spark.local.dir=/tmp/spark-local`.
- `driver` **e** `executor`: `envFrom` com **três** refs — `spark-secrets` (creds S3A da
  plataforma) + ConfigMap + Secret do job — em ambos (executores também leem Delta do S3);
  `env PYTHONPATH=/opt/spark/app/src/main`; `emptyDir` de 10Gi para spill (I11).
- `restartPolicy: { type: Never }` (efêmero; falha → `Failed`) e `timeToLiveSeconds: 86400`.

> ⚠️ A config de MinIO/Delta/Metastore **não** vem do `spark-defaults.conf` da imagem quando o job
> roda sob este operator: o feature gate `LoadSparkDefaults` injeta o ConfigMap
> `spark-operator-defaults` e sombreia o arquivo da imagem. Config específica deste job vai no
> `sparkConf` do manifesto.

**Resolve:** I5, I8, I11.

## Tema 4 — Config & Secrets (ConfigMap + Secret)

**Sugestão:** espelhar o `.env.example`:
- **ConfigMap** (não-secreto): `MINIO_BASE_PATH`, `SPARK_S3_ENDPOINT`,
  `POSTGRES_HOST/PORT/DATABASE/SCHEMA`, `TRINO_*`, `CLICKHOUSE_URL/SERVICE_NAME/VERIFY_SSL`.
- **Secret do job**: `POSTGRES_PASSWORD`, `TRINO_PASSWORD`/`TRINO_CLIENT_SECRET`,
  `CLICKHOUSE_PASSWORD`, `SPARK_S3_ACCESS_KEY`/`SPARK_S3_SECRET_KEY` (cliente minio-py).
- **`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`** (S3A do Spark — I7, distintas das anteriores)
  **não** ficam no Secret deste projeto: vêm do `spark-secrets` da plataforma, já existente em
  `dw-dados`.
- `envFrom: [secretRef spark-secrets, configMapRef, secretRef]` no driver e executor.
- **Backend de segredo:** testes citam OpenBao (`senha-do-openbao`) → sugerir **External Secrets
  Operator + OpenBao** materializando o Secret (sem segredo versionado, coerente com `resources/.gitignore`).

**Resolve:** I6, I7.

## Tema 5 — RBAC / ServiceAccount

**Resolvido fora deste repositório.** O `ServiceAccount spark` + Role/RoleBinding em `dw-dados`
são criados pelo chart `spark-operator/spark-operator` (`spark.jobNamespaces` em
`dtlk-spark-operator/values.yaml`). O `k8s/rbac.yaml` que existia aqui foi **removido**: uma
cópia divergente no repo do job criaria conflito de ownership com o Helm. O manifesto apenas
referencia `driver.serviceAccount: spark`.

**Resolve:** parte de I5.

## Tema 6 — Orquestração no Airflow (`SparkKubernetesOperator`)

**Situação:** as DAGs hoje fazem `docker exec spark-submit` (Compose). No K8s:
`SparkKubernetesOperator` (aplica o `SparkApplication`) + `SparkKubernetesSensor` (aguarda
conclusão), com params por entidade vindos do config Mongo (config-driven, como as DAGs atuais).

**Gaps:**
- Imagem do Airflow (`build/airflow/requirements.txt`) **não tem**
  `apache-airflow-providers-cncf-kubernetes` → adicionar + rebuild.
- ✅ **Spark Operator já instalado** em `dw-dados` (chart 2.5.0 — `dtlk-k8s/dtlk-spark-operator/`).
  Existe DAG de referência funcionando: `pipeline_spark_demo_bronze` em `dtlk-airflow-pipeline`.
- Template do `SparkApplication` versionado com placeholders preenchidos pela DAG (nome único por
  run, `arguments`, `envFrom`).

**Resolve:** I9.

## Tema 7 — Pré-requisitos de infra (fora do código)

✅ Spark Operator, namespace `dw-dados`, `ServiceAccount spark`, secret `spark-secrets` e
`imagePullSecrets` do Harbor — todos já provisionados. A imagem do job passou a ser auto-contida,
então não há mais dependência de base image interna (Tema 1).

Em aberto:
- ⚠️ **Política de bucket no MinIO** — `MINIO_BASE_PATH=s3a://datawake-tenant-uat`, mas o usuário
  `dw-app-spark-k8s` do `spark-secrets` só tem RW em `hive-warehouse`. Ampliar a política do
  usuário **ou** criar um secret de credenciais próprio do job. **Bloqueia a execução real.**
- NetworkPolicies liberando egress p/ MinIO/Postgres/Trino/ClickHouse.
- Disco p/ spill — coberto pelo `emptyDir` do manifesto.

---

## Roadmap (fatias verificáveis)

1. ✅ **Imagem** — `Dockerfile` (multi-stage), `requirements.txt`, `docker/spark-defaults.conf`,
   `.dockerignore`.
2. ✅ **Config** — `k8s/configmap.yaml`, `k8s/secret.example.yaml` (RBAC pertence ao chart).
3. ✅ **Manifesto** — `k8s/sparkapplication.yaml` (template).
4. **Orquestração** — provider no `build/airflow/requirements.txt` + DAG de exemplo com
   `SparkKubernetesOperator`.
5. **CI** — registro em `.github/config/clients.yml` (ver ressalva do `rsync --delete` no
   `k8s/README.md`).

## Verificação

**Imagem (executado — tudo passou):**
```bash
docker build -t honeycomb:dev .
docker run --rm --entrypoint sh honeycomb:dev -c 'ls /opt/spark/jars | grep -E "delta|hadoop-aws|aws-java-sdk|postgresql"'
docker run --rm --entrypoint sh honeycomb:dev -c 'ls /opt/hive-metastore-jars | wc -l'   # 209
docker run --rm --entrypoint id honeycomb:dev                                            # uid=185(spark)
docker run --rm --entrypoint python3 honeycomb:dev -c "import delta, pyspark, pyhocon, minio, trino, psycopg2"
docker run --rm --entrypoint python3 honeycomb:dev /opt/spark/app/src/main/main.py --help
docker run --rm --entrypoint python3 honeycomb:dev -c "from utils.config_manager import ConfigManager; print(ConfigManager().config_base_path)"
# Regressão de sintaxe contra o Python da imagem (3.10):
docker run --rm --user 0 --entrypoint python3 honeycomb:dev -m compileall -q /opt/spark/app/src
```

**Restante:**
1. **Contrato env**: `docker run` com envFrom simulado + `import main` sem crash (reusa repros de B1/I1).
2. **SparkApplication**: aplicar em `dw-dados` com o operator; 1 execução `gold_datamart`
   contra Postgres efêmero (reaproveitar `tests/integration/test_repository_gold_datamart.py`).
   Depende da política de bucket no MinIO (Tema 7).
3. **DAG**: dry-run com `SparkKubernetesOperator` apontando ao template; conferir substituição de
   `arguments`/`envFrom` por entidade.
