# Épico 1 — Infraestrutura

| Campo | Valor |
|---|---|
| Versão | 1.0 |
| Data | 2026-09-04 |
| Status | Em andamento — estado por task em [../TODO.md](../TODO.md) |
| Objetivo | `bash scripts/bootstrap.sh` sobe a stack completa, verificável serviço a serviço |
| Depende de | Nada. É o primeiro épico |
| Bloqueia | E2 inteiro |
| Progresso | [../TODO.md](../TODO.md) |

---

## Objetivo

Sete serviços de pé no minikube, cada um com um critério de aceite que roda por comando. Nenhuma task
depende de dado real — este épico termina antes de o usuário carregar qualquer Parquet.

## Premissas

1. minikube com driver `docker` sobre WSL2.
2. O usuário tem `.wslconfig` ajustado, ou aceita rodar no perfil `small`.
3. Imagens públicas são acessíveis (a POC não usa o Harbor da Datawake).

## Fora de escopo

TLS, SSO, NetworkPolicy, alta disponibilidade, replicação. A POC roda com uma réplica por serviço.

---

## Tasks

| Task | Serviço | Bloqueia | Paralelizável com |
|---|---|---|---|
| [T1.1](#t11--repositório-e-cluster-base) | Repositório e cluster base | tudo | — |
| [T1.2](#t12--minio) | MinIO | T2.5 | T1.3 |
| [T1.3](#t13--spark-operator-e-imagem-honeycomb) | Spark Operator + imagem | T2.1 | T1.2 |
| [T1.4](#t14--postgresql--braço-de-comparação) | PostgreSQL | T2.4 | T1.5, T1.6 |
| [T1.5](#t15--clickhouse-multi-tenant) | ClickHouse | T2.3 | T1.4, T1.6 |
| [T1.6](#t16--mongodb) | MongoDB | T1.7 | T1.4, T1.5 |
| [T1.7](#t17--airflow) | Airflow | T2.6 | — |

---

## T1.1 — Repositório e cluster base

### Ações

1. `git init` em `datamart-poc/` e `git checkout -b feat/v2-olap`. **Nenhum commit** — o primeiro commit é
   do usuário.
2. `.gitignore` com `__pycache__/`, `*.py[cod]`, `.venv/`, `.pytest_cache/`, `benchmark/results/`, `*.log`.
3. `cluster/minikube-up.sh`: substituir os defaults fixos das linhas 6-8 (`CPUS=4`, `MEMORY=8g`,
   `DISK=40g`) por um `case "$PROFILE"`, dimensionado contra os 15,5 GiB reais do WSL2
   ([ARQUITETURA.md §5](../ARQUITETURA.md#5-orçamento-de-recursos)):
   - `small` → `--cpus=4 --memory=8g` — **o perfil da POC**, e o default do script
   - `full` → `--cpus=6 --memory=10g` — disponível, nunca exercitado
4. Pré-checagem que **aborta**, não avisa:
   - RAM total do WSL2 (`/proc/meminfo`) contra o perfil mais 5 GiB de folga para o próprio WSL2;
   - espaço livre em `/var/lib/docker` contra 82 GiB (76 Gi de claims + ~6 GiB de imagens).
5. `scripts/profile.sh` com dois subcomandos:
   - `quiesce` — escala Airflow e MongoDB para 0 réplicas; **falha** se houver `SparkApplication` em estado
     não-terminal;
   - `resume` — restaura as réplicas.

### Artefatos

`.gitignore`, `cluster/minikube-up.sh` (alterado), `scripts/profile.sh` (novo).

`DISK` sobe de 40g para 90g. O driver `docker` ignora `--disk-size` — quem manda é o espaço livre do
`DockerRootDir`, e é ele que a pré-checagem mede.

### Aceite

```bash
bash cluster/minikube-up.sh
docker inspect minikube --format '{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}'
```
Deve retornar `8589934592 4000000000`.

Não use `kubectl get node` para isso: no driver `docker` o kubelet anuncia a capacidade do **host**, não a
do cgroup do container. Medido em [TESTES.md](../TESTES.md) — 8 GiB de cgroup, 15,47 GiB anunciados.

```bash
git symbolic-ref --short HEAD     # feat/v2-olap
git log --oneline                 # fatal: ... does not have any commits yet
```

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| WSL2 sem RAM para o perfil | `minikube start` avisa e **continua**; depois pods `OOMKilled`, nó `NotReady` | A pré-checagem aborta antes do `minikube start` |
| vhdx sem espaço | Taint `node.kubernetes.io/disk-pressure`, pods evictados, ClickHouse `Code 243 NOT_ENOUGH_SPACE` | Pré-checagem de disco; `docker system prune -a` |

---

## T1.2 — MinIO

### Ações

1. Manter `infra/minio/minio-standalone.yaml` como está — a V1 já entrega StatefulSet de 1 réplica, PVC
   20Gi e Secret `minio-creds`.
2. Acrescentar o prefixo `stage/` ao `infra/minio/bucket-provision-job.yaml`, ao lado dos três existentes.
   Ele é o destino da saída de emergência da carga (ver [E2](E2-execucao.md) T2.3).

### Artefatos

`infra/minio/bucket-provision-job.yaml` (alterado).

### Aceite

```bash
kubectl -n datamart wait --for=condition=complete job/minio-provision --timeout=180s
mc ls poc/datamart/
```
Deve listar `data-bee_replication/`, `business_datavault_data-bee/`, `gold_datavault/`, `stage/`.

---

## T1.3 — Spark Operator e imagem honeycomb

### Ações

1. Manter o chart `spark-operator/spark-operator` e o `infra/spark/operator-values.yaml` da V1.
2. Construir a imagem a partir do `Dockerfile` do **honeycomb**, não do fork. O honeycomb já resolve os
   jars por `ADD` em build-time — o incidente #5 do `TROUBLESHOOTING.md` (ivy falhando no Spark Operator
   por `HOME=/nonexistent`) já está resolvido upstream.
3. Acrescentar ao `Dockerfile` os jars do connector ClickHouse, no conjunto exato que a V1 provou contra o
   incidente #11:
   - `com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.10.0`
   - `com.clickhouse:clickhouse-client:0.9.8`
   - `com.clickhouse:clickhouse-http-client:0.9.8`
   - `org.apache.httpcomponents.client5:httpclient5:5.2.1`

### Artefatos

`spark-source-code/Dockerfile` (alterado em T2.1, jars aqui).

### Aceite

```bash
minikube image build -t honeycomb:poc -f spark-source-code/Dockerfile spark-source-code/
docker run --rm honeycomb:poc ls /opt/spark/jars | grep -c clickhouse
```
Deve retornar `≥ 3`.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Incidente #11 (LZ4) sobre Java 17 | `IllegalArgumentException: Magic is not correct` no log do driver, na primeira escrita | Fallback `spark.sql.catalog.clickhouse.option.compress=false` — custa banda intra-cluster, irrelevante aqui. Fallback final: caminho Parquet + `s3()`, em [E2](E2-execucao.md) T2.3 |

---

## T1.4 — PostgreSQL — braço de comparação

Corrige o defeito **D3** ([ARQUITETURA.md §4](../ARQUITETURA.md#4-defeitos-verificados-no-ambiente)).

### Ações

1. StatefulSet `postgres:16-alpine`, PVC 20Gi, requests `300m` / `640Mi`, limits `2` / `1792Mi`.
2. Tuning por `postgresql.conf` em ConfigMap:
   ```
   shared_buffers = 384MB
   effective_cache_size = 1GB
   work_mem = 24MB
   random_page_cost = 1.1
   ```
3. `ddl/postgres/02_indices.sql`, aplicado **após** a carga:
   ```sql
   CREATE INDEX IF NOT EXISTS ix_fact_200_cep_dash
     ON public.fact_200_cep (filial, banco, unidade_producao_id, "timestamp");
   ANALYZE public.fact_200_cep;
   ```
4. **Opcional, recomendado:** terceiro braço `pg-tuned` — mesma tabela com `PARTITION BY RANGE
   ("timestamp")` mensal mais índice BRIN (Block Range Index).

### Por que o item 3 não é opcional

`GeneratePostgresQueryUtils` cria a tabela com um único índice, o da chave primária, que para
`fact_200_cep` é `andon_peso_id` — e não é o filtro do dashboard. Sem este índice, toda query do benchmark
faz varredura sequencial, e a comparação vira um espantalho que não sobrevive à primeira pergunta técnica.

### Por que o item 4 vale o custo

"Então é só arrumar o Postgres" é o contra-argumento mais previsível da sala. Antecipá-lo com uma barra
medida vale mais do que mais um fator 2× na barra do ClickHouse. Custa uma DDL e um script de carga.

### Artefatos

`infra/postgres/postgres-statefulset.yaml`, `infra/postgres/postgres-config.yaml`,
`ddl/postgres/02_indices.sql`, opcionalmente `ddl/postgres/03_pg_tuned.sql`.

### Aceite

```bash
psql -c "\d+ fact_200_cep"          # mostra ix_fact_200_cep_dash
psql -c "EXPLAIN <q04>"             # não contém "Seq Scan"
```

---

## T1.5 — ClickHouse multi-tenant

Decisão detalhada em [ADR-003](../decisoes/ADR-003-rbac-multi-tenant.md).

### Ações

1. Reescrever `infra/clickhouse/chi-datamart.yaml`:
   - **Só** o usuário de bootstrap `dm_admin`, com `access_management: 1`;
   - `requests` `1Gi`/`500m` e `limits` `2304Mi`/`3`;
   - `max_server_memory_usage` 1,5 GiB e `mark_cache_size` 384 MiB — os defaults (90% da RAM vista e
     5 GiB, respectivamente) estouram sozinhos o limite do container;
   - qualquer setting de sessão sob `profiles:`, **nunca** sob `settings:`.
2. `ddl/rbac/10_tenant.sql.tpl`, renderizado por tenant:
   ```sql
   CREATE DATABASE IF NOT EXISTS dm_{{TENANT}};

   CREATE SETTINGS PROFILE IF NOT EXISTS p_{{TENANT}}_ro SETTINGS
       readonly = 2, join_use_nulls = 1,
       max_memory_usage = 1500000000, max_execution_time = 30,
       max_result_rows = 200000;

   CREATE QUOTA IF NOT EXISTS q_{{TENANT}}
       KEYED BY user_name FOR INTERVAL 1 MINUTE
       MAX queries = 120, errors = 20, result_rows = 5000000,
           read_rows = 500000000, execution_time = 60
       TO r_{{TENANT}}_ro;

   CREATE ROLE IF NOT EXISTS r_{{TENANT}}_ro;
   CREATE ROLE IF NOT EXISTS r_{{TENANT}}_loader;

   GRANT SELECT ON dm_{{TENANT}}.* TO r_{{TENANT}}_ro;
   GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE,
         ALTER MOVE PARTITION, ALTER DELETE ON dm_{{TENANT}}.* TO r_{{TENANT}}_loader;

   CREATE USER IF NOT EXISTS u_{{TENANT}}_ro IDENTIFIED WITH sha256_password BY '{{PWD_RO}}'
       DEFAULT ROLE r_{{TENANT}}_ro SETTINGS PROFILE p_{{TENANT}}_ro;
   CREATE USER IF NOT EXISTS u_{{TENANT}}_loader IDENTIFIED WITH sha256_password BY '{{PWD_LD}}'
       DEFAULT ROLE r_{{TENANT}}_loader;
   ```
3. Job idempotente que renderiza e aplica o template para **dois** tenants: `acme` e `globex`.
4. `scripts/verify-rbac.sh` com asserções **negativas**.

### Notas de precisão

- `readonly = 2`, não `1`: o valor `1` proíbe alterar settings e quebra clientes que emitem `SET`.
- `ALTER MOVE PARTITION` é o privilégio que cobre `REPLACE PARTITION`; `ALTER DELETE` é exigido no lado de
  origem do movimento. Faltando qualquer um, o erro nomeia exatamente o privilégio ausente.
- `join_use_nulls = 1` não é preferência de estilo. Sem ele, um `LEFT JOIN` preenche o lado ausente com o
  **default do tipo** em vez de `NULL` — e o painel mostra um número diferente do Postgres **sem erro
  nenhum**. É a divergência mais difícil de rastrear depois.

### Por que dois tenants e não um

Um tenant não prova nada sobre isolamento. Se só houver dado real de um, carregar uma cópia em `dm_globex`
e declarar isso no relatório.

### Artefatos

`infra/clickhouse/chi-datamart.yaml` (reescrito, com o Secret `ch-creds` no mesmo arquivo e antes do CHI),
`ddl/rbac/10_tenant.sql.tpl`, `infra/clickhouse/job-rbac.yaml`, `scripts/verify-rbac.sh`,
`scripts/bootstrap.sh` (passos 7 e 8).

O Secret `ch-creds` é a fonte única das senhas: o CR referencia `dm_admin` por
`k8s_secret_password`, o Job lê as dos tenants por volume, e o `verify-rbac.sh` lê as mesmas chaves.

O template cria `USER` e só depois faz `GRANT` da role e `ALTER USER ... DEFAULT ROLE`. `CREATE USER ...
DEFAULT ROLE r` numa role ainda não concedida ao usuário falha.

### Aceite

```bash
bash scripts/verify-rbac.sh
```
As quatro asserções negativas passam:
1. `INSERT` do usuário `_ro` falha com `Code 497 (ACCESS_DENIED)`;
2. `u_acme_ro` lendo `dm_globex` falha com `Code 497`;
3. `SELECT value FROM system.settings WHERE name='readonly'` retorna `2`;
4. `SELECT value FROM system.settings WHERE name='join_use_nulls'` retorna `1`.

> Um teste que só afirma o caminho feliz não detecta privilégio **a mais**. As asserções precisam ser
> negativas.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Identidade declarada no CR **e** em SQL | `GRANT` falha: *"Cannot update user X in users.xml because this storage is readonly"* | Nenhuma identidade de tenant no CR — só `dm_admin` |
| Operador sobrescreve `users.d` no reconcile | Usuário some após `kubectl apply` no CHI | Só o bootstrap vive no CR; o resto em SQL, que o operador não toca |
| Profile declarado em `config.d` | Setting silenciosamente ignorado | Settings de sessão vão sob `profiles:` |
| `k8s_secret_password` não resolvido | `dm_admin` autentica vazio; Job falha com `Code 516 (AUTHENTICATION_FAILED)` | Exige operador ≥ 0.18 (usamos 0.24) e o Secret aplicado **antes** do CHI. Fallback: `dm_admin/password` em claro no CR, como fazia a V1 |

---

## T1.6 — MongoDB

### Ações

1. StatefulSet `mongo:7`, uma réplica, PVC 8Gi, com `--wiredTigerCacheSizeGB 0.25` **fixo**.
2. Job de seed que insere o documento em `Data_Catalog.k8s_<tenant>` a partir de
   `airflow/mongo-seed/k8s_<tenant>.json`, com os campos do control plane produtivo:
   `filiais[]`, `tables[{name, chave_pk}]`, `schedule_interval`, `honeycomb_version`, `source_tenants`.

> O cache do WiredTiger precisa ser fixado. O dimensionamento default é `max(256MB, 0.5×(RAM−1GB))` e, em
> imagens ou kernels sem consciência de cgroup, ele dimensiona contra a RAM do **nó**, não do container —
> o pod é `OOMKilled` sem ter recebido uma única query.

### Artefatos

`infra/mongodb/mongodb-statefulset.yaml`, `infra/mongodb/job-seed.yaml`,
`airflow/mongo-seed/k8s_acme.json`, `airflow/mongo-seed/k8s_globex.json`.

### Aceite

```bash
kubectl -n datamart exec sts/mongodb -- mongosh --quiet \
  --eval 'db.getSiblingDB("Data_Catalog").k8s_acme.findOne()'
```
Retorna o documento com `filiais`, `tables` e `schedule_interval`.

---

## T1.7 — Airflow

### Ações

1. Chart `apache-airflow/airflow` **1.16.0** — o mesmo de produção. `executor: LocalExecutor`;
   `redis.enabled`, `flower.enabled`, `statsd.enabled` e `triggerer.enabled` em `false`.
2. **`postgresql.enabled: false`** e StatefulSet `postgres:16-alpine` próprio para o metadata DB, com
   Secret carregando a connection string em `data.metadataSecretName`.
3. Imagem custom de três linhas: `FROM apache/airflow:2.11.2` mais `pip install pymongo`.
4. DAGs e manifestos por ConfigMap, montados em `/opt/airflow/dags` e `/opt/airflow/dags/manifests`.
5. Variable via env var `AIRFLOW_VAR_MONGODB_K8S_TEST` no scheduler e no webserver.
6. Role e RoleBinding no namespace `datamart` para a SA **`airflow-scheduler`**:
   - `sparkoperator.k8s.io` / `sparkapplications`: `create, get, list, watch, delete, patch`
   - core / `pods`, `pods/log`, `events`: `get, list, watch`

### Por que cada desvio do default

| Item | Motivo |
|---|---|
| `postgresql.enabled: false` | O subchart é Bitnami; a mudança de hospedagem das imagens quebra o pull. Sinal: `ImagePullBackOff` em `airflow-postgresql-0` |
| Imagem custom | A DAG produtiva importa `pymongo` diretamente, não o provider `apache-airflow-providers-mongo` |
| ConfigMap em vez de `minikube mount` | `minikube mount` é um processo de longa duração que morre com o terminal no WSL2. Limite de 1 MiB por ConfigMap, folgado aqui |
| Variable por env var | Backend de env var não escreve no metadata DB, então o ambiente é reproduzível. **O nome precisa ser o exato de produção** para que o arquivo da DAG seja byte-idêntico ao de `dtlk-airflow-pipeline/dags/` |
| SA `airflow-scheduler` | Com `LocalExecutor` **não existe deployment de workers** — as tasks rodam dentro do pod do scheduler |

> Produção usa `CeleryExecutor` e git-sync. Ambos os desvios são de recurso e de conveniência local, e
> estão declarados aqui para que a apresentação não os apresente como equivalência.

### Artefatos

`infra/airflow/values.yaml`, `infra/airflow/postgres-metadata.yaml`, `infra/airflow/rbac-spark.yaml`,
`images/airflow/Dockerfile`.

### Aceite

```bash
kubectl -n airflow exec deploy/airflow-scheduler -- airflow dags list-import-errors
```
Saída vazia.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Subchart Bitnami não puxa | `ImagePullBackOff` em `airflow-postgresql-0` | `postgresql.enabled=false` |
| `pymongo` ausente | `ModuleNotFoundError: pymongo` em `list-import-errors` | Imagem custom |
| RoleBinding para a SA errada | `sparkapplications... is forbidden: User "system:serviceaccount:airflow:airflow-worker" cannot create` | RoleBinding para `airflow-scheduler` |

---

## Checklist Go/No-Go do épico

- [ ] `PROFILE=full bash scripts/bootstrap.sh` termina sem erro
- [ ] `kubectl -n datamart get pods` — todos `Running`, nenhum `CrashLoopBackOff`
- [ ] `mc ls poc/datamart/` lista os 4 prefixos
- [ ] `docker run --rm honeycomb:poc ls /opt/spark/jars | grep -c clickhouse` ≥ 3
- [ ] `bash scripts/verify-rbac.sh` — 4 asserções negativas passam
- [ ] `mongosh` retorna o documento de control plane dos 2 tenants
- [ ] `airflow dags list-import-errors` vazio
- [ ] `bash scripts/profile.sh quiesce && bash scripts/profile.sh resume` funciona nos dois sentidos
