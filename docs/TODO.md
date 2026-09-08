# TODO — POC V2

**Fonte única de verdade do progresso.** Nenhum outro documento registra estado. Épicos e ADRs dizem o que
fazer e por quê; este arquivo diz onde está.

Uma task só fecha quando o comando de aceite roda e a saída bate. Fechamento não é opinião.

| Estado | Significado |
|---|---|
| ⬜ | pendente |
| 🟨 | em andamento |
| 🟥 | bloqueado — a causa fica na linha |
| ✅ | feito, com o aceite verificado |

**Progresso:** 7 de 19 tasks fechadas (T0.1, T1.1 a T1.6). Falta T1.7 para fechar o Épico 1.

Execução dos testes registrada em [TESTES.md](TESTES.md) — 36 testes, 30 verdes, 1 defeito aberto.

---

## Marcos

| Marco | Critério | Estado |
|---|---|---|
| **M0 — Especificação fechada** | E0 inteiro | ✅ |
| **M1 — Stack de pé** | Checklist Go/No-Go de [E1](epicos/E1-infraestrutura.md) inteiro | 🟨 |
| **M2 — Dado fluindo** | Um trigger no Airflow carrega os dois braços pelo Dataset | ⬜ |
| **M3 — Dado íntegro** | `compare-counts.sh` com `delta = 0` em toda linha | ⬜ |
| **M4 — Evidência pronta** | `benchmark/results/RESULTADO.md` com as 8 seções | ⬜ |

M3 é portão duro: sem ele, M4 não começa.

---

## Épico 0 — Especificação

Não é um épico de implementação: registra os documentos como entregáveis, para que nada fique fora deste
arquivo. Fecha quando a especificação para de mudar por descoberta e passa a mudar só por execução.

### ✅ T0.1 — Especificação e registro
Aceite: todo link interno resolve e toda âncora existe — verificado por varredura

- [x] `docs/README.md` — índice, glossário, premissas, mapa papel→documento
- [x] `docs/ARQUITETURA.md` — desenho alvo, orçamento de recursos, o que a POC não prova
- [x] `docs/TODO.md` — este arquivo, fonte única de progresso
- [x] `docs/TESTES.md` — registro de execução de teste por épico
- [x] `docs/epicos/E1`, `E2`, `E3` — especificação por épico
- [x] `docs/decisoes/ADR-001..004`

---

## Épico 1 — Infraestrutura → [especificação](epicos/E1-infraestrutura.md)

### ✅ T1.1 — Repositório e cluster base
Aceite: `docker inspect minikube --format '{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}'`
→ `8589934592 4000000000` — [T-E1-20](TESTES.md#42-t11--repositório-e-cluster-base)

- [x] `git init` + branch `feat/v2-olap`, sem commit
- [x] `.gitignore`
- [x] `PROFILE=small|full` em `cluster/minikube-up.sh`
- [x] Pré-checagem de RAM e disco do WSL2 que **aborta**
- [x] `scripts/profile.sh quiesce|resume`
- [x] Ajustar `requests`/`limits` dos manifestos ao envelope do perfil — os três
      `infra/spark/sparkapplication-*.yaml` (executor 2×2g → 1×1g + `memoryOverhead` explícito)

`small` é o perfil da POC e o default do script — decisão de limite de infra local. O `full` fica
disponível e **fora de escopo de teste**. O aceite antigo (`kubectl get node`) caiu com
[D4](TESTES.md#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup).

### ✅ T1.2 — MinIO
Aceite: os 4 prefixos listados, incluindo `stage/` — [T-E1-10](TESTES.md#43-t12--minio)

- [x] Manter `minio-standalone.yaml`
- [x] Prefixo `stage/` no `bucket-provision-job.yaml`

### ✅ T1.3 — Spark Operator e imagem honeycomb
Aceite: o smoke do connector chega a `COMPLETED` —
[T-E1-32](TESTES.md#47-t13--spark-operator-e-imagem-honeycomb)

- [x] Chart `spark-operator` 2.5.2 e `operator-values.yaml`
- [x] `images/spark/Dockerfile` — overlay sobre `hub.datawake.cloud/dw-dados/honeycomb:latest`,
      **sem reconstruir o honeycomb**
- [x] Jars do connector ClickHouse (custo: 20 MB sobre a base)
- [x] `infra/spark/smoke/smoke_clickhouse.py` e `sparkapplication-smoke-clickhouse.yaml`
- [x] `infra/spark/spark-secrets.yaml` com `DATAMART_CH_*` (corrige **D2**)
- [x] `deletecollection` na Role do ServiceAccount `spark` (`infra/spark/spark-rbac.yaml`)

Contar jars foi descartado como aceite: prova que o arquivo está no disco, não que o connector conversa
com o servidor. O smoke roda como `u_acme_loader` e cobre catálogo, autenticação, RBAC e leitura.
**O incidente #11 (LZ4) não reproduziu.** Dois defeitos no caminho:
[D11](TESTES.md#d11--rbac-por-database-não-basta-para-o-connector-spark) (o RBAC de T1.5 não cobria as
tabelas de `system` que o connector lê — e o aceite de T1.5 passou mesmo assim) e
[D12](TESTES.md#d12--os-system-logs-do-clickhouse-derrubam-o-servidor) (os system logs do ClickHouse
somaram 7,9 M de linhas em 4h ociosas e derrubaram o servidor por OOM).

### ✅ T1.4 — PostgreSQL (corrige **D3**)
Aceite: `EXPLAIN` sai de `Parallel Seq Scan` (3207 buffers) para `Index Scan` (607) —
[T-E1-24](TESTES.md#45-t14--postgresql-o-braço-de-comparação)

- [x] `infra/postgres/postgres-statefulset.yaml` — StatefulSet `postgres:16-alpine` + PVC 20Gi
- [x] Tuning por `-c`: `shared_buffers`, `effective_cache_size`, `work_mem`, `random_page_cost`,
      `track_io_timing`, `pg_stat_statements`
- [x] `ddl/postgres/02_indices.sql` + `ANALYZE`
- [x] `ddl/postgres/01_tenants.sql` e `infra/postgres/job-init.yaml` — um database por tenant, idempotente
- [x] `ddl/postgres/00_probe_indice.sql` — prova a escolha de plano sem depender da carga
- [ ] *(opcional, aguarda decisão)* terceiro braço `pg-tuned` particionado + BRIN

Dois defeitos no caminho: [D7](TESTES.md#d7--argumento-inválido-no-primeiro-boot-envenena-o-pgdata)
(um `-c` invalido no primeiro boot deixa o PGDATA permanentemente meio-inicializado) e
[D10](TESTES.md#d10--docker-entrypoint-initdbd-é-pulado-em-silêncio-num-pvc-reusado)
(`/docker-entrypoint-initdb.d` e pulado em silencio num PVC reusado).

### ✅ T1.5 — ClickHouse multi-tenant → [ADR-003](decisoes/ADR-003-rbac-multi-tenant.md)
Aceite: `verify-rbac.sh` 4/4 — [T-E1-16](TESTES.md#44-t15--clickhouse-multi-tenant)

- [x] Reescrever `chi-datamart.yaml`: só `dm_admin`, `limits` 2304Mi, tetos de memória do servidor
- [x] `ddl/rbac/10_tenant.sql.tpl`
- [x] `infra/clickhouse/job-rbac.yaml` — Job idempotente para 2 tenants (`acme`, `globex`)
- [x] `GRANT SELECT` nas 6 tabelas de `system` que o connector Spark lê (**D11**)
- [x] `configuration.files` desligando os system logs do ClickHouse (**D12**)
- [x] `scripts/verify-rbac.sh` com asserções negativas
- [x] Encadear no `scripts/bootstrap.sh` — passo 7 (RBAC) e passo 8 (tabelas de fato)

Passou depois de dois defeitos corrigidos: [D5](TESTES.md#d5--podtemplate-declarado-mas-nunca-aplicado)
(o `podTemplate` era ignorado — imagem `latest`, sem limite de memória; **erro herdado da V1**) e
[D6](TESTES.md#d6--background_pool_size--ratio-abaixo-do-mínimo-de-sanidade) (o servidor recusava iniciar).
O `REPLACE PARTITION` pelo `loader` foi validado aqui, antes de T2.3 existir.

### ✅ T1.6 — MongoDB
Aceite: `findOne()` em `Data_Catalog.k8s_acme` devolve o documento —
[T-E1-27](TESTES.md#46-t16--mongodb-o-control-plane)

- [x] `infra/mongodb/mongodb-statefulset.yaml` — `mongo:7`, `--wiredTigerCacheSizeGB 0.25` fixo, probe `tcpSocket`
- [x] `airflow/mongo-seed/` — 4 documentos: `k8s_<tenant>` e `k8s_<tenant>_gold`, 2 tenants
- [x] `infra/mongodb/job-seed.yaml` idempotente
- [x] Contrato validado pelo mesmo caminho das DAGs produtivas

Duas correcoes a especificacao, tiradas da leitura das DAGs reais: sao **duas** colecoes por tenant, nao
uma; e `source_tenants` nao entra — e exclusivo da DAG de super-tenant.
[D9](TESTES.md#d9--probe-lento-derruba-o-dns-do-service-headless): probe lento derruba o DNS do Service
headless e o seed morre sem conseguir conectar.

### ⬜ T1.7 — Airflow
Aceite: `airflow dags list-import-errors` vazio

- [ ] Chart 1.16.0, `LocalExecutor`, sem Redis/Flower/statsd/triggerer
- [ ] `postgresql.enabled: false` + StatefulSet de metadata próprio
- [ ] Imagem custom com `pymongo`
- [ ] DAGs e manifestos por ConfigMap
- [ ] `AIRFLOW_VAR_MONGODB_K8S_TEST`
- [ ] RoleBinding para a SA **`airflow-scheduler`**

---

## Épico 2 — Execução → [especificação](epicos/E2-execucao.md)

### ⬜ T2.1 — Re-sync com `honeycomb@main` → [ADR-001](decisoes/ADR-001-resync-honeycomb.md)
Aceite: `pytest -q` passa **sem alteração** após o rsync

- [ ] `rsync` de `honeycomb@main` sobre `spark-source-code/`
- [ ] Preservar `utils/clickhouse_connection.py`
- [ ] `DATAMART_CH_*` no `_ENV_SCHEMA` (corrige **D2**)

### ⬜ T2.2 — Janela de carga (corrige **D1**) → [ADR-004](decisoes/ADR-004-janela-de-carga.md)
Aceite: recarregar a mesma janela duas vezes **não muda** a contagem da partição

- [ ] `JANELA_INICIO`/`JANELA_FIM` em `queryutils.build_query`
- [ ] Substituir `INTERVAL 10 DAYS` em `fact_200_cep.sql:44`
- [ ] Guarda de cobertura integral antes da troca

### ⬜ T2.3 — Repositório ClickHouse → [ADR-002](decisoes/ADR-002-modelagem-clickhouse.md)
Aceite: `pytest -m integration ...clickhouse.py -q` → `passed`, incluindo recarga

- [ ] `utils/clickhouse_datamart.py` (cliente HTTP)
- [ ] `RepositoryGoldDatamartClickhouse` com a sequência de troca de partição
- [ ] `PipelineGoldDatamartClickhouse` + chave na factory
- [ ] `ddl/clickhouse/01_fact_200_cep.sql`
- [ ] Teste de integração com testcontainers

### ⬜ T2.4 — Braço Postgres e simetria experimental
Aceite: o diff entre os dois repositórios toca **exclusivamente** `write()`

- [ ] Extrair `read()`/`transform()` para base comum
- [ ] `.na.drop` no `transform` compartilhado
- [ ] `DECIMAL(18,4)` em vez de `DOUBLE`, nos dois braços

### ⬜ T2.5 — Carga bronze
Aceite: `bash scripts/load-bronze.sh --validate` passa com o dado real

- [ ] `load-bronze.sh` substitui `seed-bronze.sh`
- [ ] Modo `--validate` que falha nomeando o caminho ausente
- [ ] Destino derivado do documento Mongo

### ⬜ T2.6 — DAGs
Aceite: um trigger no `bronze_silver` dispara as duas DAGs gold pelo Dataset

- [ ] `k8s_<tenant>_bronze_silver.py`
- [ ] `k8s_<tenant>_gold_datamart_pg.py`
- [ ] `k8s_<tenant>_gold_datamart_ch.py`
- [ ] 3 manifestos com placeholders `TENANT`/`VERSION`
- [ ] Preservar `outlets` na task não-mapeada e `max_active_tis_per_dag=1`

---

## Épico 3 — Validação → [especificação](epicos/E3-validacao.md)

### ⬜ T3.0 — **PORTÃO** de janela
Aceite: [ADR-004](decisoes/ADR-004-janela-de-carga.md) preenchido com o número medido

- [ ] Medir meses distintos tocados por execução típica
- [ ] Decidir `REPLACE PARTITION` × `ReplacingMergeTree`
- [ ] Registrar no ADR-004

### ⬜ T3.1 — **PORTÃO** de corretude
Aceite: `bash benchmark/compare-counts.sh` → `delta = 0` em **toda** linha

- [ ] `compare-counts.sh` com `count` e `sum(valor)` por `(filial, mês)`
- [ ] Executar e conferir

> Sem `delta = 0`, T3.2 não roda. Número de performance sobre dado divergente é pior que nenhum número.

### ⬜ T3.2 — Suíte de leitura
Aceite: desvio de p95 < 30% entre rodadas idênticas

- [ ] `views/00_views.{pg,ch}.sql`
- [ ] q01–q03
- [ ] **q04** — a central: colunas nomeadas de view, filtro, agregação
- [ ] **q05** — `SELECT *`, a armadilha de propósito
- [ ] `read-bench.sh`: cronometragem e instrumentação separadas, `query_id` explícito
- [ ] Modo cold/warm
- [ ] Modo concorrente `-c N`
- [ ] `report.sh` com `clickhouse-local`
- [ ] Rodar com `-c 1` e `-c 8`, após `profile.sh quiesce`

### ⬜ T3.3 — Vizinho barulhento
Aceite: p95 degrada sob carga do vizinho e volta após a quota

- [ ] Carregar `dm_globex`
- [ ] `noisy-neighbour.sh` com os 3 pontos de medição

### ⬜ T3.4 — Relatório
Aceite: `RESULTADO.md` com as 8 seções, nenhum campo vazio

- [ ] 1 Ambiente · 2 **Corretude** · 3 Latência · 4 I/O
- [ ] 5 Compressão · 6 q05 destacada · 7 Vizinho barulhento
- [ ] 8 **Ressalvas** (page cache, minikube, volume, isolamento)
- [ ] Gráfico de barras ASCII por query

---

## Pendências que dependem do usuário

| # | O quê | Bloqueia |
|---|---|---|
| 1 | Carregar os Parquet reais no layout do honeycomb | T2.5, e por consequência todo o E3 |
| 2 | ~~Ajustar `.wslconfig`~~ — resolvido: perfis redimensionados para os 15,5 GiB atuais | — |
| 3 | Primeiro commit da branch `feat/v2-olap` | nada — pode ser a qualquer momento |
| 4 | Decidir se o terceiro braço `pg-tuned` entra | T1.4 (opcional) |
