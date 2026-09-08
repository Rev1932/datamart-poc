# Épico 1 — Infraestrutura

| Campo | Valor |
|---|---|
| Versão | 1.0 |
| Data | 2026-09-04 |
| Status | **Completo** — as 7 tasks fechadas com aceite executado. Estado por task em [../TODO.md](../TODO.md) |
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
2. **Não reconstruir o honeycomb.** A imagem publicada em
   `hub.datawake.cloud/dw-dados/honeycomb:latest` já traz tudo o que a POC precisa:

   | Componente | Versão na imagem |
   |---|---|
   | Spark | 3.5.8 (Scala 2.12, Hadoop 3.3.4) |
   | Java | 17 |
   | Python | 3.10.12 |
   | Delta | 3.3.2 |
   | hadoop-aws / aws-java-sdk-bundle | 3.3.4 / 1.12.262 |
   | Aplicação | `/opt/spark/app` (`src/main`, `resources/queries`) |

3. `images/spark/Dockerfile` vira um **overlay de 3 linhas** sobre essa imagem, acrescentando só o
   connector ClickHouse — o único componente que a imagem de produção ainda não carrega:
   - `com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.10.0`
   - `com.clickhouse:clickhouse-client:0.9.8`
   - `com.clickhouse:clickhouse-http-client:0.9.8`
   - `org.apache.httpcomponents.client5:httpclient5:5.2.1`
4. `infra/spark/smoke/smoke_clickhouse.py` e `infra/spark/sparkapplication-smoke-clickhouse.yaml`:
   exercitam `ClickHouseCatalog.initialize` e uma leitura, sem tocar em Delta nem em dado real.
5. `infra/spark/spark-secrets.yaml` passa a expor `DATAMART_CH_*` em vez de `CLICKHOUSE_*` — corrige o
   defeito **D2** ([ARQUITETURA §4](../ARQUITETURA.md#4-defeitos-verificados-no-ambiente)).

> A especificação original mandava construir a imagem a partir do `Dockerfile` do honeycomb. Era
> retrabalho: reproduzir localmente uma imagem que já existe publicada, e assumir o risco de ela sair
> diferente da de produção. O overlay mantém a POC rodando **exatamente** o binário produtivo.

### Artefatos

`images/spark/Dockerfile` (reescrito), `infra/spark/smoke/smoke_clickhouse.py`,
`infra/spark/sparkapplication-smoke-clickhouse.yaml`, `infra/spark/spark-secrets.yaml` (reescrito),
`infra/spark/spark-rbac.yaml` (alterado).

### Aceite

```bash
docker build -f images/spark/Dockerfile -t honeycomb:poc images/spark/
minikube image load honeycomb:poc
kubectl apply -f infra/spark/sparkapplication-smoke-clickhouse.yaml
kubectl -n datamart get sparkapplication smoke-clickhouse \
  -o jsonpath='{.status.applicationState.state}'
```
Deve chegar a `COMPLETED`, com `[smoke] OK` no log do driver.

Contar jars não é aceite: prova que o arquivo está no disco, não que o connector conversa com o servidor.
O smoke roda como `u_acme_loader` e exercita o caminho inteiro — catálogo, autenticação, RBAC e leitura.

### Notas de precisão

- Copiar de `/tmp/.ivy2/jars/` e não de `find -name '*.jar'`: o diretório `cache/` guarda os **mesmos**
  jars com outro nome, e cada um entraria duas vezes no classpath.
- Nada de `chown -R /opt/spark/jars`: reescreve os 257 jars da base numa camada nova e engorda a imagem
  em ~650 MB. `cp` como root já cria os jars legíveis por qualquer uid. Com isso o overlay custa **20 MB**.
- `httpclient5` resolve para **5.4.4**, não para os 5.2.1 pedidos: o ivy escolhe a versão mais alta exigida
  pelo `clickhouse-client:0.9.8`. O smoke valida a combinação real.
- A Role do ServiceAccount `spark` precisa de `deletecollection` em `services` e `persistentvolumeclaims`:
  o driver limpa esses recursos por `labelSelector` ao encerrar. Sem isso o job conclui, mas com
  `Forbidden` no log e recursos órfãos.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Incidente #11 (LZ4) sobre Java 17 | `IllegalArgumentException: Magic is not correct` no `ClickHouseCatalog.initialize` | **Não reproduziu** contra o ClickHouse 24.8 com o conjunto de jars acima. O smoke é a prova, e roda em ~1 min |
| CRD do Spark Operator defasado | Campo do manifesto ignorado em silêncio | Os CRD vieram da instalação anterior (2.5.0) e o chart agora é 2.5.2. Funcionou; ao subir de minor, remover os três CRD antes |
| Registry inacessível | `docker pull` falha no passo 2 do bootstrap | A imagem já está local. O bootstrap só puxa se faltar |

---

## T1.4 — PostgreSQL — braço de comparação

Corrige o defeito **D3** ([ARQUITETURA.md §4](../ARQUITETURA.md#4-defeitos-verificados-no-ambiente)).

### Ações

1. StatefulSet `postgres:16-alpine`, PVC 20Gi, requests `300m` / `640Mi`, limits `2` / `1792Mi`.
2. Tuning por **argumento** (`-c chave=valor`), não por arquivo:
   ```
   shared_buffers=384MB   effective_cache_size=1GB   work_mem=24MB
   random_page_cost=1.1   max_connections=40         track_io_timing=on
   shared_preload_libraries=pg_stat_statements
   ```
   `include_dir` só é aceito **dentro** do `postgresql.conf` — passá-lo por `-c` derruba o servidor com
   `unrecognized configuration parameter`. E um `config_file` próprio exigiria replicar `hba_file` e
   `ident_file`. `track_io_timing` é pré-requisito do `EXPLAIN (ANALYZE, BUFFERS)` do benchmark.
3. `ddl/postgres/02_indices.sql`, aplicado **após** a carga:
   ```sql
   CREATE INDEX IF NOT EXISTS ix_fact_200_cep_dash
     ON public.fact_200_cep (filial, banco, unidade_producao_id, "timestamp");
   ANALYZE public.fact_200_cep;
   ```
4. Terceiro braço `pg-tuned` em `ddl/postgres/03_pg_tuned.sql`, no schema **`gold_tuned`**: a mesma
   tabela com `PARTITION BY RANGE ("timestamp")` mensal, o **mesmo** índice do painel, e um BRIN
   (Block Range Index) sobre `timestamp` com `pages_per_range = 32`.

   Três detalhes decidem se este braço mede alguma coisa:

   | Detalhe | Por quê |
   |---|---|
   | O `INSERT` tem `ORDER BY "timestamp"` | BRIN guarda min/max por faixa de blocos. Em tabela embaralhada o índice existe, é consultado, e não descarta bloco nenhum |
   | O índice do painel é repetido aqui | Sem ele o braço tunado **perderia** para o simples, e a comparação viraria um espantalho ao contrário |
   | Sem `PRIMARY KEY` | Tabela particionada exige que a PK contenha a chave de partição. Esta é alvo de leitura, nunca de escrita concorrente |

   A geração das partições usa `\gexec` e não um bloco `DO`: o psql **não** substitui `:origem` dentro
   de dollar-quoting.

### Por que o item 3 não é opcional

`GeneratePostgresQueryUtils` cria a tabela com um único índice, o da chave primária, que para
`fact_200_cep` é `andon_peso_id` — e não é o filtro do dashboard. Sem este índice, toda query do benchmark
faz varredura sequencial, e a comparação vira um espantalho que não sobrevive à primeira pergunta técnica.

### Por que o item 4 vale o custo

"Então é só arrumar o Postgres" é o contra-argumento mais previsível da sala. Antecipá-lo com uma barra
medida vale mais do que mais um fator 2× na barra do ClickHouse. Custa uma DDL e um script de carga.

### Artefatos

`infra/postgres/postgres-statefulset.yaml`, `infra/postgres/job-init.yaml`,
`ddl/postgres/02_indices.sql`, `ddl/postgres/03_pg_tuned.sql`. Ambos rodam **depois** da carga, nessa ordem.

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
   - qualquer setting de sessão sob `profiles:`, **nunca** sob `settings:`;
   - `configuration.files` desligando os system logs do proprio ClickHouse — ver
     [D12](../TESTES.md#d12--os-system-logs-do-clickhouse-derrubam-o-servidor).
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

   -- exigidas pelo connector Spark; ver D11
   GRANT SELECT ON system.{clusters,macros,databases,tables,columns,parts}
         TO r_{{TENANT}}_loader;

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
2. Job de seed que insere em **duas** coleções por tenant, a partir de `airflow/mongo-seed/`:

   | Coleção | Lida por | Campos |
   |---|---|---|
   | `k8s_<tenant>` | DAG `bronze_silver` | `filiais[]`, `tables[{name, chave_pk[]}]`, `schedule_interval`, `honeycomb_version` |
   | `k8s_<tenant>_gold` | DAG `gold_datamart` | `tables[{name, chave_pk[], gold_type}]`, `schedule_interval`, `honeycomb_version` |

   O contrato saiu da leitura das DAGs produtivas, não de suposição. Duas correções à especificação
   original: são **duas** coleções, não uma; e `source_tenants` não entra — é exclusivo da DAG de
   super-tenant (`k8s_lakatos_silver_super_tenant`), que a POC não replica.

3. Probe de readiness por `tcpSocket`, não `mongosh --eval`: o mongosh é Node.js e não sobe dentro do
   `timeoutSeconds` default de 1s. Ver [D9](../TESTES.md#d9--probe-lento-derruba-o-dns-do-service-headless).

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
3. **Nada de imagem custom.** `dw-dados/datawake-airflow:0.1.0` já é Airflow **2.11.2** com **pymongo
   4.10.1**, e todos os imports da DAG produtiva resolvem nela. Mesma correção de T1.3: a imagem existe,
   construir outra é retrabalho com risco de divergir da de produção.
4. DAGs por ConfigMap, copiadas para um `emptyDir` por **initContainer**.
5. Variable via env var `AIRFLOW_VAR_MONGODB_K8S_TEST` no scheduler e no webserver.
6. Role e RoleBinding no namespace `datamart` para a SA **`airflow-scheduler`**:
   - `sparkoperator.k8s.io` / `sparkapplications`: `create, get, list, watch, delete, patch, update`
   - core / `pods`, `pods/log`, `events`: `get, list, watch`
7. `airflow/dags/smoke_control_plane.py`: prova Variable, pymongo, o contrato do control plane e o RBAC
   do spark-operator, replicando os idiomas da DAG produtiva.

### Por que cada desvio do default

| Item | Motivo |
|---|---|
| `postgresql.enabled: false` | O subchart é Bitnami; a mudança de hospedagem das imagens quebra o pull. Sinal: `ImagePullBackOff` em `airflow-postgresql-0` |
| Imagem de produção, sem build | Já traz Airflow 2.11.2 e pymongo. A DAG produtiva importa `pymongo` direto, não o provider |
| ConfigMap em vez de `minikube mount` | `minikube mount` é um processo de longa duração que morre com o terminal no WSL2 |
| **initContainer + `emptyDir`** | Montar a ConfigMap DIRETO em `/opt/airflow/dags` quebra o walker de DAGs — ver [D13](../TESTES.md#d13--configmap-montada-em-optairflowdags-quebra-o-walker-de-dags) |
| Variable por env var | Backend de env var não escreve no metadata DB, então o ambiente é reproduzível. **O nome precisa ser o exato de produção** para que o arquivo da DAG seja byte-idêntico ao de `dtlk-airflow-pipeline/dags/` |
| SA `airflow-scheduler` | Com `LocalExecutor` **não existe deployment de workers** — as tasks rodam dentro do pod do scheduler |
| `webserverSecretKey` literal | Sem isso cada `helm upgrade` rotaciona a chave e invalida as sessões da UI |

> Produção usa `CeleryExecutor` e git-sync. Ambos os desvios são de recurso e de conveniência local, e
> estão declarados aqui para que a apresentação não os apresente como equivalência.

### Artefatos

`infra/airflow/values.yaml`, `infra/airflow/postgres-metadata.yaml`, `infra/airflow/rbac-spark.yaml`,
`airflow/dags/smoke_control_plane.py`. **Não há `images/airflow/Dockerfile`** — a imagem de produção
basta.

### Aceite

```bash
kubectl -n airflow exec statefulset/airflow-scheduler -c scheduler -- \
  airflow dags list-import-errors
```
Saída `No data found`.

O scheduler é **StatefulSet** neste chart, não Deployment, e o pod tem dois containers — daí o
`statefulset/` e o `-c scheduler`. Um `deploy/airflow-scheduler` falha com `NotFound`.

Um DagBag vazio também não tem erro de import, então o aceite se apoia na `smoke_control_plane`: ela é
carregada, roda e prova o caminho inteiro.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Subchart Bitnami não puxa | `ImagePullBackOff` em `airflow-postgresql-0` | `postgresql.enabled=false` |
| `pymongo` ausente | `ModuleNotFoundError: pymongo` em `list-import-errors` | A imagem de produção já o traz |
| RoleBinding para a SA errada | `sparkapplications... is forbidden: User "system:serviceaccount:airflow:airflow-worker" cannot create` | RoleBinding para `airflow-scheduler`; a task `checar_rbac_spark` prova |
| ConfigMap montada direto em `dags/` | `Detected recursive loop when walking DAG directory` | initContainer copiando para `emptyDir` |

---

## Checklist Go/No-Go do épico

- [x] `bash scripts/bootstrap.sh` (perfil `small`) termina sem erro — T-E1-44
- [x] `kubectl get pods` — nenhum `CrashLoopBackOff`
- [x] `mc ls poc/datamart/` lista os 4 prefixos — T-E1-10
- [x] o smoke do connector ClickHouse chega a `COMPLETED` — T-E1-32
- [x] `bash scripts/verify-rbac.sh` — 4 asserções negativas passam — T-E1-16
- [x] `mongosh` retorna o documento de control plane dos 2 tenants — T-E1-27
- [x] `airflow dags list-import-errors` devolve `No data found` e a `smoke_control_plane` roda — T-E1-35, T-E1-36
- [x] `bash scripts/profile.sh quiesce && resume` funciona nos dois sentidos — com workload real

> **Os oito itens verificados, o primeiro num cluster criado do zero.** O `minikube delete` foi feito e o
> `bootstrap.sh` reconstruiu a stack inteira em 12 passos sem intervenção. A reprodutibilidade deixou de
> ser intenção e virou medida.

### Legado da V1 ainda na árvore

`scripts/run-{ingest,gold,normalize}.sh` e `infra/spark/sparkapplication-{ingest,gold,normalize}.yaml`
são da V1 e apontam para a imagem `datamart-spark:poc`, que não existe mais. Estão marcados com aviso e
**abortam com exit 1** em vez de falhar em silêncio. Saem quando [E2](E2-execucao.md) T2.6 entregar os
manifestos novos.
