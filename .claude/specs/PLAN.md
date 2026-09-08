# POC Datamart — MinIO + Spark + ClickHouse no Kubernetes

## Context

O objetivo é construir uma POC de uma arquitetura de disponibilização de dados (datamart como endpoint de delivery). O fluxo é:

1. **Bronze** — um microserviço replica dados para pastas com granularidade tabelar dentro de um bucket no **MinIO**.
2. **Silver** — um pipeline **Apache Spark** normaliza os dados e regrava em outro prefixo do mesmo bucket, em formato **Delta Lake**. *A lógica de código-fonte já existe — só é preciso a infraestrutura para rodá-la.*
3. **Datamart** — o **próprio Spark** lê a camada silver (Delta Lake), aplica **regra de negócio + agregações** e **ingere em batch** o resultado numa instância **ClickHouse** configurada para leitura rápida.

O foco da POC é **validar a eficiência do ClickHouse**, em especial o possível **gargalo de merge/update durante ingestões em batch**. O merge se dá por **chave primária única = hash a nível de linha**. Toda a stack roda em **Kubernetes**.

Este documento entrega: (a) especificação e configuração de cada serviço; (b) plano de desenvolvimento individual de cada componente; (c) plano de integração ponta a ponta; (d) roteiro de benchmark do merge — que é o entregável central da POC.

## Premissas adotadas (ajustáveis)

| Decisão | Escolha padrão | Racional |
|---|---|---|
| Cluster K8s | **minikube** local em WSL2 (driver `docker`) | POC leve, reprodutível, sem custo de nuvem; addons prontos |
| Deploy | Operators (Altinity CH, Kubeflow Spark, MinIO) | Idiomático em K8s, realista vs. produção |
| Formato da silver/gold | **Delta Lake** sobre MinIO (S3A) | Definido pelo pipeline existente; exige `delta-spark` no Spark |
| Fonte do datamart | **gold** (não silver) | É onde a regra de negócio/agregação e o hash `hk_business_id` já existem |
| Ingestão gold→CH | **Nova pipeline Spark** (novo tipo no `PipelineFactory`) reusa a leitura da gold e grava o DataFrame no ClickHouse via **connector oficial ClickHouse-Spark** | Só o `write` muda (ClickHouse em vez de Delta/MinIO); leitura idêntica à gold atual |
| Engine de merge | `ReplacingMergeTree(load_dts)` `ORDER BY (hk_business_id)` | `hk_business_id` = `sha2(concat_ws("_", <chave_pk>), 256)`; dedup por hash é o caso exato do engine |
| Spark | Imagem baseada em `apache/spark`, job on-demand via `SparkApplication` | Só infra; o código de negócio já existe |

> Se qualquer premissa não bater com o cenário real (ex.: cluster on-prem existente, ou Scala em vez de PySpark), ajusto o plano na revisão.

---

## 0. Execução local do Kubernetes (WSL2 + minikube)

O cluster roda **inteiramente dentro do WSL2**, não em nuvem. O minikube cria um "nó" do Kubernetes como um **container Docker** rodando no daemon Docker do WSL2 (driver `docker`). Não há VM extra: o kubelet, API server, etc. ficam nesse container; MinIO/Spark/ClickHouse rodam como pods dentro dele.

**Pré-requisitos no WSL2:**
- Docker acessível no WSL2 (Docker Engine instalado no Ubuntu do WSL2, **ou** Docker Desktop no Windows com integração WSL2 ativada) — `docker ps` precisa funcionar.
- Binários `minikube`, `kubectl` e `helm` no PATH do WSL2.
- Recursos suficientes: a POC pede CPU/memória para 3 serviços + Spark; sugerido `--cpus=4 --memory=8g` (ajustável conforme a máquina; configurável no `.wslconfig` do Windows).

**Como sobe (`cluster/minikube-up.sh`):**
```bash
minikube start --driver=docker --cpus=4 --memory=8g --disk-size=40g
minikube addons enable storage-provisioner   # storage class 'standard' (PVCs)
minikube addons enable metrics-server         # métricas de recurso (opcional)
# addon ingress só se formos expor via host; na POC usamos port-forward
```

**Acesso aos serviços (sem LoadBalancer real):**
- Padrão: `kubectl port-forward` para MinIO Console (`:9001`), ClickHouse HTTP (`:8123`) e Spark UI.
- Alternativa: `minikube service <svc> --url` ou `minikube tunnel` (se optarmos por `Service` tipo LoadBalancer).

**Imagens de container (ponto de atenção no minikube):** a imagem custom do Spark (com delta-spark + connector CH + código existente) precisa estar disponível **dentro** do minikube. Duas formas:
1. `minikube image load datamart-spark:poc` após `docker build` no WSL2; **ou**
2. `eval $(minikube docker-env)` e `docker build` direto no daemon do minikube (evita a etapa de load).
Operators (MinIO, spark-operator, clickhouse-operator) usam imagens públicas — baixadas normalmente.

**Persistência:** PVCs usam a storage class `standard` (hostPath dentro do nó minikube). Cuidado: `minikube delete` apaga os dados — para a POC isso é aceitável e até desejável entre rodadas de benchmark.

**Teardown:** `minikube stop` (preserva) / `minikube delete` (zera tudo).

---

## Código-fonte Spark existente (análise)

O código PySpark vive em `spark-source-code/src/main/`. `main.py` recebe args (`--pipeline`, `--chave-pk`, `--config-name`, `--table-name`, `--is_merge_schema`, `--colunas_zorder`) e o `core/pipeline_factory.py` (`PipelineFactory`) despacha para o tipo pedido. Tipos existentes hoje:

- **`bronze_silver`** (`PipelineBronzeToSilverWrapper`): lê Parquet da bronze (`data-bee_replication/...`) → transformers (RawDataVault/HardBusinessRules/Validate) → grava Delta na silver (`business_datavault_data-bee/<table>`, CDF + `partitionBy("source")`) → registra a tabela no **Trino** (schema silver).
- **`gold`** (`PipelineGoldWrapper`): `PipelineGold` usa `RepositoryGoldDataVaults` cujo `read()` roda `spark.sql(<query de resources/queries>)` sobre a silver e adiciona **`hk_business_id = sha2(concat_ws("_", <chave_pk>), 256)`** (o "hash a nível de linha") + `load_dts = current_timestamp()`; `write()` faz **MERGE** no Delta gold por `hk_business_id` e roda optimize/z-order. Depois registra a tabela no **Trino** (schema gold).
- **`oee_cleaner`**: limpeza de temporários no MinIO.

Config em HOCON via `utils/config_manager.py` (`resources/application-<env>.conf`), com chaves como `minio.base_path`, `spark.s3.*`, `spark.conf.*`, `trino.*`. `utils/session.py` cria a SparkSession aplicando `spark.s3` e iterando `spark.conf`. `utils/trino_connection.py` é o padrão de "conexão com o destino de delivery" — o análogo do que faremos para o ClickHouse.

**Lacuna:** não há **nenhuma** conexão/escrita para ClickHouse. O delivery hoje é Trino sobre Delta. A tarefa nova (detalhada em "Tarefa: nova pipeline de ingestão ClickHouse") adiciona um tipo no factory que reusa a leitura da gold e grava o DataFrame no ClickHouse.

---

## Estrutura de repositório proposta

```
datamart-poc/
├── README.md
├── cluster/
│   └── minikube-up.sh                # start do minikube + addons + storage class
├── infra/
│   ├── 00-namespaces.yaml
│   ├── minio/
│   │   ├── minio-standalone.yaml     # StatefulSet + Service + PVC + Secret
│   │   └── bucket-provision-job.yaml # Job com `mc` cria bucket e prefixos
│   ├── spark/
│   │   ├── operator-values.yaml      # values do helm chart spark-operator
│   │   ├── spark-rbac.yaml           # ServiceAccount + Role p/ driver
│   │   ├── spark-secrets.yaml        # credenciais MinIO (S3A) + ClickHouse
│   │   ├── sparkapplication-normalize.yaml  # Job A: --pipeline bronze_silver
│   │   ├── sparkapplication-gold.yaml       # Job B: --pipeline gold
│   │   └── sparkapplication-ingest.yaml     # Job C: --pipeline datamart (gold→ClickHouse)
│   └── clickhouse/
│       ├── operator-install.md       # ref. instalação do clickhouse-operator
│       └── chi-datamart.yaml         # ClickHouseInstallation (settings de merge)
├── images/
│   └── spark/
│       └── Dockerfile                # apache/spark + hadoop-aws + delta-spark + connector CH + spark-source-code/
├── spark-source-code/                # código PySpark existente (+ nova pipeline `datamart`)
├── ddl/
│   └── 01_create_datamart_table.sql  # tabela ReplacingMergeTree (destino do Job C)
├── scripts/
│   ├── bootstrap.sh                  # sobe cluster + operators + infra
│   ├── seed-bronze.sh                # popula bronze com dados de exemplo
│   ├── run-normalize.sh              # dispara Job A (bronze_silver)
│   ├── run-gold.sh                   # dispara Job B (gold)
│   └── run-ingest.sh                 # dispara Job C (datamart → ClickHouse)
└── benchmark/
    ├── ingest-loop.sh                # N batches com chaves sobrepostas
    └── merge-metrics.sql             # consultas às system tables do CH
```

---

## 1. MinIO — Object Storage (Bronze + Silver)

**Papel:** armazenamento distribuído S3-compatível. Um único bucket `datamart` com prefixos `bronze/<tabela>/` (dados replicados) e `silver/<tabela>/` (**tabelas Delta Lake**, com `_delta_log/`).

**Especificação (POC):**
- Deploy **standalone** (StatefulSet 1 réplica) com PVC (storage class `standard` do minikube). *Nota:* escala para modo distribuído (MinIO Operator Tenant, 4+ drives) sem mudar o contrato S3 — mantido fora do escopo da POC.
- `Service` ClusterIP para API (`:9000`) e Console (`:9001`); Console exposto via port-forward.
- Credenciais em `Secret` (`MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`), referenciado por Spark (S3A) e pelo connector.
- Bucket + prefixos criados por um `Job` que roda `mc mb` / `mc mkdir` após o MinIO ficar Ready.

**Configuração-chave:**
- `region` fixa (ex.: `us-east-1`) para compatibilidade com S3A.
- Recursos POC: 512Mi–1Gi RAM, 250m–500m CPU; PVC 10–20Gi.

**Critério de pronto:** `mc ls` lista o bucket; leitura de uma tabela Delta de teste (`_delta_log/` + arquivos de dados) funciona via S3A.

---

## 2. Apache Spark — Processamento e Ingestão

**Papel:** empacotar e orquestrar os pipelines PySpark existentes via `SparkApplication` (um por execução, selecionado por `--pipeline`):
- **Job A — `bronze_silver`:** lê bronze e grava silver em **Delta Lake** no MinIO. *(código existente)*
- **Job B — `gold`:** roda a query sobre a silver, gera `hk_business_id`/`load_dts`, faz MERGE no Delta gold e registra no Trino. *(código existente)*
- **Job C — `datamart` (NOVO):** reusa a leitura da gold e **grava o DataFrame no ClickHouse** em vez de Delta/MinIO. Código a ser adicionado — ver "Tarefa: nova pipeline de ingestão ClickHouse". É o job medido no benchmark de merge.

**Especificação:**
- **Kubeflow Spark Operator** (helm chart `spark-operator`) no namespace `spark`. Ciclo de vida via CRD `SparkApplication` (um por job).
- **Imagem** (`images/spark/Dockerfile`): base `apache/spark:3.5.x` (Java/Python conforme o código) + as dependências:
  - `hadoop-aws` + `aws-java-sdk-bundle` (S3A → MinIO),
  - `delta-spark` (ex.: `io.delta:delta-spark_2.12:3.2.x`, compatível com Spark 3.5) para ler/gravar Delta,
  - **connector oficial ClickHouse-Spark** (`com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12` + `clickhouse-jdbc`/client) para o Job C,
  - o código-fonte existente (`spark-source-code/`) copiado/instalado na imagem.
- **RBAC**: `ServiceAccount` + `Role`/`RoleBinding` para o driver criar pods de executor.
- **SparkApplication** (A, B, C) definem: `mainApplicationFile` (`main.py`), args (`--pipeline`, `--chave-pk`, `--config-name`, `--table-name`), `driver`/`executor`, `image`, `sparkConf` (S3A/Delta e — no Job C — catálogo ClickHouse).

**Configuração S3A + Delta (via `sparkConf`):**
```
spark.hadoop.fs.s3a.endpoint             = http://minio.minio.svc.cluster.local:9000
spark.hadoop.fs.s3a.path.style.access    = true
spark.hadoop.fs.s3a.connection.ssl.enabled = false
spark.hadoop.fs.s3a.impl                 = org.apache.hadoop.fs.s3a.S3AFileSystem
spark.sql.extensions                     = io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog          = org.apache.spark.sql.delta.catalog.DeltaCatalog
```
Credenciais do MinIO via `secretKeyRef` (não hardcode).

**Configuração do Job C — escrita no ClickHouse (connector), via `spark.conf.*` no HOCON:**
```
spark.sql.catalog.clickhouse             = com.clickhouse.spark.ClickHouseCatalog
spark.sql.catalog.clickhouse.host        = <Service do CHI, ex.: clickhouse-datamart.clickhouse.svc>
spark.sql.catalog.clickhouse.protocol    = http   # ou grpc
spark.sql.catalog.clickhouse.user/password = <secret>
spark.sql.catalog.clickhouse.database    = datamart
```
Escrita em batch (`df.writeTo("clickhouse.datamart.fato").append()`), com tamanho de batch controlado para evitar “too many parts” no CH. A dedup por `hk_business_id` é resolvida no ClickHouse pelo `ReplacingMergeTree` (merge em background) — é exatamente o gargalo que a POC mede.

**Recursos POC:** driver 1 core / 1–2Gi; 2 executors 1 core / 2Gi.

**Critério de pronto:**
- Job A (`bronze_silver`): grava tabela Delta na silver; `COMPLETED`.
- Job B (`gold`): grava tabela Delta na gold com `hk_business_id`; `COMPLETED`.
- Job C (`datamart`): escreve N linhas em `datamart.fato` no ClickHouse; `COMPLETED`.

---

## 3. ClickHouse — Datamart (foco da POC)

**Papel:** endpoint de leitura rápida; recebe do Spark, em batch, os dados já agregados/com regra de negócio; faz merge/dedup por hash de linha. É onde se mede o gargalo de merge.

**Especificação:**
- **Altinity clickhouse-operator** instalado no namespace `clickhouse`.
- `ClickHouseInstallation` (CHI): 1 shard / 1 réplica (POC — sem replicação para isolar custo de merge local), PVC dedicado, `podAntiAffinity` irrelevante em nó único.
- Usuário de aplicação (usado pelo connector Spark) + perfil com settings de merge (abaixo). Endpoints HTTP (`:8123`) e nativo (`:9000`) expostos in-cluster para o connector.

### 3.1 Modelagem para merge por hash de linha

A chave e a versão saem prontas da gold (ver análise do código): `hk_business_id` é um **sha2-256 (String hex de 64 chars)** e `load_dts` é o timestamp da carga.

```sql
CREATE TABLE datamart.fato
(
    hk_business_id  String,          -- chave primária = hash de linha (sha2-256) produzido na gold
    load_dts        DateTime64(3),   -- timestamp da carga; versão p/ escolher a linha vencedora
    is_deleted      UInt8 DEFAULT 0, -- opcional: soft-delete (ReplacingMergeTree com coluna de deleção)
    ...colunas de negócio (resultado da query da gold)...
)
ENGINE = ReplacingMergeTree(load_dts)
ORDER BY (hk_business_id)         -- PK = ORDER BY: dedup ocorre por hk_business_id
PARTITION BY ...                  -- baixa cardinalidade p/ NÃO fragmentar merges (ver abaixo)
SETTINGS index_granularity = 8192;
```

**Por que `ReplacingMergeTree`:** deduplica linhas com mesma `ORDER BY` mantendo a de maior `load_dts`. É exatamente “merge por chave única (hash) mantendo a versão mais nova”. Alternativas (`CollapsingMergeTree`/`VersionedCollapsingMergeTree`) só valem se houver necessidade de sinais +1/−1 de estado — não é o caso.
> Nota: `hk_business_id` é String de 64 chars; avaliar `FixedString(64)` (ou converter para `UInt128`/`UInt64` via `reinterpretAsUInt*`/`sipHash64`) se o custo de comparação/ordenção da String pesar no merge — é um dos eixos de tuning do benchmark.

### 3.2 Configuração para eficiência de merge (o coração da POC)

**Estratégia de partição:** partição deve ser **grossa** (ex.: `toYYYYMM(data)` ou um domínio de negócio de baixa cardinalidade), nunca por algo derivado do hash. Merges só ocorrem *dentro* de uma partição — partições demais = muitas parts pequenas que nunca consolidam.

**Settings de servidor (no CHI), com racional:**
- `optimize_on_insert = 0` — evita merge síncrono no INSERT; deixa o merge em background (menor latência de ingestão).
- `background_pool_size` / `background_merges_mutations_concurrency_ratio` — aumentar concorrência de merge conforme cores disponíveis.
- `max_bytes_to_merge_at_max_space_in_pool` — teto de tamanho de merge; ajustar para permitir consolidação de parts grandes.
- `parts_to_delay_insert` / `parts_to_throw_insert` — proteção contra “too many parts”; reforça a regra de **inserir em batches grandes**, não muitos INSERTs pequenos.
- `min_bytes_for_wide_part` — controla wide vs compact parts; wide favorece leitura de merge em volumes maiores.
- `async_insert = 1` + `wait_for_async_insert` — só se o Spark acabar gerando muitos INSERTs pequenos; com batches grandes por partição normalmente não é necessário.

**Leitura deduplicada (delivery):**
- `SELECT … FROM datamart.fato FINAL` para ver o estado consolidado antes do merge de background completar.
- Mitigar custo do `FINAL` com `do_not_merge_across_partitions_select_final = 1` e boa escolha de `ORDER BY`.
- Consolidação forçada em janela de manutenção: `OPTIMIZE TABLE datamart.fato FINAL` (usado no benchmark para medir custo/tempo do merge completo).

### 3.3 Ingestão gold→ClickHouse (feita pelo Spark)

A ingestão é responsabilidade do **Job C do Spark** — a nova pipeline `datamart` (ver seção 2 e "Tarefa: nova pipeline de ingestão ClickHouse"): reusa a leitura da gold (query + `hk_business_id`/`load_dts`) e faz `append` na tabela `datamart.fato` via connector ClickHouse-Spark. O ClickHouse apenas **recebe** os INSERTs e resolve a dedup por `hk_business_id` nos merges de background do `ReplacingMergeTree`.

Recomendações para não estrangular o merge no lado do CH:
- Spark escreve **poucos batches grandes** por partição, não muitos micro-INSERTs (evita “too many parts”).
- Alinhar o particionamento de escrita do Spark à `PARTITION BY` da tabela, para que cada INSERT caia em poucas partições.

**Critério de pronto:** Job C `COMPLETED`; `SELECT count() FROM datamart.fato FINAL` retorna o número esperado de chaves únicas.

---

## Tarefa: nova pipeline de ingestão ClickHouse (código Spark)

Objetivo: adicionar um **novo tipo no `PipelineFactory`** (ex.: `datamart`) que **reusa a leitura da gold exatamente como é hoje** e, no lugar de gravar Delta no MinIO, **grava o DataFrame no ClickHouse** via connector ClickHouse-Spark. Nada da leitura/regras/hash muda — só o destino do `write`.

**Arquivos a tocar (em `spark-source-code/src/main/`):**

1. **`core/pipeline_orchestrator.py`** — nova classe `PipelineGoldClickHouse(PipelineGold)` que herda `extract()`/`transform()` da gold (reuso de `RepositoryGoldDataVaults.read()` → query + `hk_business_id` + `load_dts`) e sobrescreve apenas `save(data)` para escrever no ClickHouse (via novo helper/repo, item 3), sem o MERGE/optimize do Delta.
2. **`core/pipeline_factory.py`** — novo wrapper `PipelineDatamartWrapper` (roda `PipelineGoldClickHouse.run()`; **sem** o passo Trino, diferente do `PipelineGoldWrapper`) e registro no dict: `self.pipelines["datamart"] = PipelineDatamartWrapper`.
3. **`utils/clickhouse_connection.py`** (NOVO) — análogo ao `trino_connection.py`, mas para escrita Spark: lê `clickhouse.*` do `ConfigManager` e expõe `write(df, table)` fazendo `df.writeTo("clickhouse.<database>.<table>").append()` (ou `format("clickhouse")` conforme a API do connector), com opções de batch. Alternativamente, um `RepositoryGoldClickHouse` em `repo/repository.py` que reusa o `read()` da gold e implementa `write()` para o ClickHouse — escolher um dos dois para não duplicar a leitura.
4. **`utils/session.py`** — nenhuma mudança de código necessária: o catálogo `clickhouse` entra via `spark.conf.*` no HOCON (já iterado pelo `SparkSessionFactory`). Só garantir que o jar do connector esteja na imagem (seção 2).
5. **`resources/application-<env>.conf`** — adicionar bloco `clickhouse { host, port, http_port, user, password, database, table }` e as chaves `spark.conf."spark.sql.catalog.clickhouse*"` (host/protocol/user/password/database) para o `SparkSessionFactory` aplicar.
6. **`ddl/01_create_datamart_table.sql`** — DDL da tabela destino (`ReplacingMergeTree(load_dts) ORDER BY (hk_business_id)`, ver 3.1). Pré-criar a tabela (recomendado, p/ controlar engine/partição/settings de merge) em vez de deixar o connector auto-criar.

**Execução:** `spark-submit main.py --pipeline datamart --config-name <cli> --table-name <tab> --chave-pk <cols...>` (mesmos args da gold). Empacotado no `sparkapplication-ingest.yaml` (Job C).

**Critérios de aceite:**
- `--pipeline datamart` lê a mesma query/gold, gera `hk_business_id`/`load_dts` idênticos aos da gold, e popula `datamart.fato` no ClickHouse.
- Reexecutar com chaves sobrepostas **não duplica** linhas após `OPTIMIZE ... FINAL` (dedup por `hk_business_id`, vencendo o maior `load_dts`).
- Nenhuma regressão nos pipelines `bronze_silver`/`gold` (o novo tipo é aditivo).
- Sem credenciais hardcoded — tudo via config/secret.

---

## Plano de desenvolvimento (individual)

Cada componente é validado isoladamente antes de integrar.

- **Fase 0 — Cluster:** `cluster/minikube-up.sh` (minikube start + addons + storage class `standard`), namespaces `minio`/`spark`/`clickhouse`. `bootstrap.sh` orquestra tudo.
- **Fase 1 — MinIO:** deploy standalone → `Job` cria bucket/prefixos → validar leitura/escrita de uma tabela Delta de exemplo (`seed-bronze.sh`).
- **Fase 2 — Spark:** instalar spark-operator → buildar imagem com hadoop-aws + delta-spark + connector CH + código existente → disponibilizar no minikube (`minikube image load` ou `minikube docker-env`) → aplicar RBAC → rodar **Job A** (`bronze_silver`) e **Job B** (`gold`) → validar tabelas Delta na silver e gold.
- **Fase 3 — ClickHouse:** instalar clickhouse-operator → aplicar CHI com settings de merge → criar tabela `ReplacingMergeTree` (`ddl/01`) → smoke test de INSERT/SELECT FINAL manual.
- **Fase 4 — Pipeline `datamart` (código):** implementar a "Tarefa: nova pipeline de ingestão ClickHouse" (novo tipo no factory + escrita no CH + config) → rodar **Job C** (`--pipeline datamart`) → validar linhas em `datamart.fato`.

## Plano de integração

- **I1 — Spark ↔ MinIO:** já exercitado na Fase 2 (Jobs A/B leem bronze/silver e gravam Delta). Validar S3A path-style, `_delta_log` e credenciais via Secret.
- **I2 — Spark ↔ ClickHouse:** rodar **Job C** (`--pipeline datamart`) com o connector, reusando a leitura da gold e escrevendo em `datamart.fato`. Validar conectividade Spark→CH in-cluster, mapeamento de schema e append em batch.
- **I3 — Ponta a ponta:** `seed-bronze` → `run-normalize` (Job A) → `gold` (Job B) → `run-ingest` (Job C) → `SELECT … FINAL`. Contagem de chaves únicas confere com a gold.

## Plano de benchmark do merge (entregável central)

- **B1 — Carga base:** Job C ingere X milhões de linhas únicas; medir tempo de escrita do Spark e `system.parts` (nº de parts, bytes).
- **B2 — Batches com sobreposição:** `benchmark/ingest-loop.sh` dispara N execuções do Job C onde M% dos `hk_business_id` colidem com dados existentes (simula update via merge). Após cada batch, coletar de `system.parts`, `system.merges`, `system.part_log`:
  - nº de parts ativas e “too many parts” (delays/throws),
  - throughput de merge (bytes/s), duração média de merge,
  - latência de `SELECT … FINAL` vs. sem `FINAL`.
- **B3 — Consolidação:** `OPTIMIZE TABLE … FINAL`; medir tempo total e verificar dedup (count == chaves únicas).
- **B4 — Varredura de tuning:** repetir B2 variando `background_pool_size`, `max_bytes_to_merge_at_max_space_in_pool`, tamanho de batch e estratégia de partição; tabular impacto no gargalo de merge.
- Consultas centralizadas em `benchmark/merge-metrics.sql`. (Opcional: expor métricas Prometheus do CH para visualizar em Grafana — fora do caminho crítico.)

**Saída da POC:** relatório respondendo “o ClickHouse com `ReplacingMergeTree` por hash sustenta as ingestões em batch sem que o merge vire gargalo, e sob quais settings?”.

---

## Verificação (end-to-end)

1. `cluster/minikube-up.sh` → `kubectl get nodes` mostra o nó minikube `Ready`. `scripts/bootstrap.sh` → `kubectl get pods -A` mostra MinIO, spark-operator e ClickHouse `Running`.
2. `scripts/seed-bronze.sh` → `mc ls datamart/bronze/...` lista os dados de exemplo.
3. `scripts/run-normalize.sh` (Job A) e o `gold` (Job B) → `kubectl get sparkapplication -n spark` `COMPLETED`; `mc ls datamart/.../gold/.../_delta_log/` mostra a tabela Delta gold com `hk_business_id`.
4. `clickhouse-client < ddl/01` (cria a tabela) → `scripts/run-ingest.sh` (Job C, `--pipeline datamart`) `COMPLETED` → `SELECT count() FROM datamart.fato FINAL` == nº de chaves únicas da gold.
5. `benchmark/ingest-loop.sh` + `benchmark/merge-metrics.sql` → coleta métricas de merge sob carga; resultados tabulados no README.

## Pontos em aberto para confirmar na revisão

- Versão do stack no código existente: `apache/spark` (3.5.x?) e `delta-spark` correspondente — confirmar para fixar a versão do connector ClickHouse-Spark e o Scala (2.12/2.13) na imagem.
- Tabela destino no ClickHouse: **pré-criada via `ddl/01`** (recomendado, p/ controlar engine/partição/settings) ou deixar o connector auto-criar? Confirmar também a chave de `PARTITION BY` (coluna de negócio de baixa cardinalidade).
- Mapa de colunas da gold → ClickHouse: além de `hk_business_id`/`load_dts`, quais colunas de negócio entram no `datamart.fato` (por tabela/`--table-name`)?
