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

**Progresso:** 17 de 20 tasks fechadas. **Épicos 1 e 2 completos** — M1 e M2 atingidos. No E3, **os
dois portões fecharam**: a T3.0 com a estratégia de carga decidida no ADR-004 (POC com `REPLACE
PARTITION`, produção futura com B2), e a T3.1 com `delta = 0` nos três braços do `dm_acme` — M3
atingido. O [estado de partida](#estado-de-partida--o-que-o-e2-entrega-ao-e3) tem os dois tenants
carregados de 2026-07 a 2026-09, índice do painel e `gold_tuned` aplicados. Próximo: T3.2.

Execução dos testes registrada em [TESTES.md](TESTES.md), **1 defeito aberto**:
[D4](TESTES.md#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup), sem correção possível e mitigado.
D16, D17 e D18 foram corrigidos e verificados.

---

## Marcos

| Marco | Critério | Estado |
|---|---|---|
| **M0 — Especificação fechada** | E0 inteiro | ✅ |
| **M1 — Stack de pé** | Checklist Go/No-Go de [E1](epicos/E1-infraestrutura.md) — **9 de 9** | ✅ |
| **M2 — Dado fluindo** | Um trigger no Airflow carrega os dois braços, na mesma janela | ✅ |
| **M3 — Dado íntegro** | `compare-counts.sh` com `delta = 0` em toda linha | ✅ |
| **M4 — Evidência pronta** | `benchmark/results/RESULTADO.md` com as 8 seções | ⬜ |

M3 é portão duro: sem ele, M4 não começa.

A ressalva do M1 **caiu**: o cluster foi destruído e o `bootstrap.sh` rodou do zero, 12 de 12 passos,
exit 0, com os 8 itens do Go/No-Go verificados em seguida —
[T-E1-44](TESTES.md#413-t-e1-44--bootstrapsh-de-ponta-a-ponta-num-cluster-limpo).
A reprodutibilidade da POC passa a ser afirmação **medida**, não intenção.

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

## ✅ Épico 1 — Infraestrutura → [especificação](epicos/E1-infraestrutura.md)

**Encerrado.** 8 tasks (T1.1 a T1.8), Go/No-Go 9 de 9, entregue em cinco PRs parceladas. A stack sobe de
um cluster vazio pelo `bootstrap.sh` em 12 passos, com os sete serviços acessíveis por
`scripts/ports.sh`. Doze defeitos registrados na execução (D4 a D15) mais quatro achados de auditoria
(A1 a A4); **um permanece aberto**
([D4](TESTES.md#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup)), sem correção possível e
mitigado por trocar o critério de aceite de `kubectl get node` para `docker inspect`.

O que o épico **não** entrega, e é bom estar dito: nenhum dado real. Toda medição até aqui é sobre
infraestrutura de pé, não sobre o dado do cliente. Isso começa em [E2](epicos/E2-execucao.md).

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
- [x] `infra/minio/console-nodeport.yaml` e `scripts/minio-ui.sh` — console acessível para a carga manual

### ✅ T1.3 — Spark Operator e imagem honeycomb
Aceite: o smoke do connector chega a `COMPLETED` —
[T-E1-32](TESTES.md#47-t13--spark-operator-e-imagem-honeycomb)

- [x] Chart `spark-operator` 2.5.2 e `operator-values.yaml`
- [x] `images/spark/Dockerfile` — overlay sobre `honeycomb:3.3.0-local`, **sem reconstruir o app**.
      Revisado em 2026-09-10: era overlay do `:latest` do registry, que não publica a 3.3.0
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
- [x] `ddl/postgres/03_pg_tuned.sql` — terceiro braço no schema `gold_tuned`: particionado por mês + BRIN

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

### ✅ T1.7 — Airflow
Aceite: `airflow dags list-import-errors` → `No data found` —
[T-E1-35](TESTES.md#49-t17--airflow)

- [x] Namespace `airflow` em `infra/00-namespaces.yaml`
- [x] Chart 1.16.0 (`infra/airflow/values.yaml`), `LocalExecutor`, sem Redis/Flower/statsd/triggerer
- [x] `infra/airflow/postgres-metadata.yaml` — `postgresql.enabled: false` + metadata DB próprio
- [x] **Sem imagem custom**: `dw-dados/datawake-airflow:0.1.0` já é 2.11.2 com pymongo 4.10.1
- [x] DAGs por ConfigMap, copiadas por initContainer para `emptyDir`
- [x] `AIRFLOW_VAR_MONGODB_K8S_TEST` com o nome exato de produção
- [x] `infra/airflow/rbac-spark.yaml` — RoleBinding para a SA **`airflow-scheduler`**
- [x] `airflow/dags/smoke_control_plane.py` — prova Variable, pymongo, contrato e RBAC

O scheduler é **StatefulSet** neste chart, não Deployment, e o pod tem dois containers: o aceite exige
`statefulset/airflow-scheduler -c scheduler`. `scripts/profile.sh` foi corrigido junto.
[D13](TESTES.md#d13--configmap-montada-em-optairflowdags-quebra-o-walker-de-dags): montar a ConfigMap
direto em `/opt/airflow/dags` quebra o walker de DAGs com `Detected recursive loop`.

### ✅ T1.8 — Acesso aos serviços e portabilidade de shell
Aceite: `check-shell-portability.sh` sai 0 e `ports.sh` sobe os serviços a partir do fish —
[§4.15](TESTES.md#415-t18--acesso-aos-serviços-e-portabilidade-de-shell)

Task extra, aberta depois do fechamento do épico. Nasceu de dois atritos de uso diário: o shell do
usuário é fish e a documentação instruía sintaxe de bash; e o acesso aos serviços dependia de
`port-forward` manual, que morre com o terminal.

**Parte A — portabilidade**

- [x] `cluster/minikube-up.sh` — `--profile`, `--disk`, `--help`; variáveis de ambiente seguem aceitas
- [x] `scripts/seed-bronze.sh` — `--src-dir`, `--config-name`, `--table`
- [x] `scripts/submit.sh` — `--dry`, `--instances`, `--driver-mem`, `--exec-mem`, `--exec-cores`
- [x] Mensagens que os próprios scripts imprimem passam a mostrar a forma com flag
- [x] Os dois `until ... do ... done` da documentação viram `kubectl wait --for=jsonpath=...`
- [x] `eval "$(minikube docker-env)"` documentado ao lado da forma fish, `minikube docker-env --shell fish | source`
- [x] `scripts/check-shell-portability.sh` — detector de regressão, com teste negativo

**Parte B — acesso por nome: descartada**

A pesquisa está em [pesquisa-ingress-dns-wsl2.md](pesquisa-ingress-dns-wsl2.md). Resumo da decisão: o
addon `ingress-dns` está abandonado (imagem removida do registry, issue fechada como *not planned*), e a
metade que faltaria — fazer o resolvedor do WSL2 consultar o cluster — exige `generateResolvConf=false`,
com risco relatado de quebrar a resolução de nome corporativa. **Decisão do usuário: o custo não
compensa.** Entra no lugar um gerenciador de `port-forward`.

**Parte B' — `scripts/ports.sh`**

- [x] Sete portas: `console`, `s3`, `airflow`, `clickhouse`, `ch-native`, `postgres`, `mongo`
- [x] Segundo plano: devolve o prompt, não ocupa o terminal
- [x] Supervisor por porta que **reabre sozinho** quando a conexão cai — a dor que originou a task
- [x] `--start` idempotente, `--stop`, `--restart`, `--status`, `--logs`, `--creds`, `--list`, `--only`
- [x] Detecta porta ocupada por processo alheio e ignora aquela chave, sem abortar o resto
- [x] `scripts/minio-ui.sh` corrigido — [D15](TESTES.md#d15--minio-uish-aponta-para-um-service-que-não-existe)

- [x] `helm upgrade` do Airflow aplicado (revisão 4), autorizado pelo usuário
- [x] Artefatos da pesquisa de ingress-dns removidos do cluster, autorizado pelo usuário

[D14](TESTES.md#d14--webserver-do-airflow-em-oomkill-cíclico): o webserver do Airflow estava em OOMKill
cíclico (160 restarts em 17 h). Só apareceu porque o `ports.sh` tentou usar a porta — nenhum teste do
épico chegava a acessá-lo. A causa não era só o número de workers: **um único worker ocupa 577 MiB**, e o
teto de 768Mi que eu havia apertado ficava abaixo do necessário com qualquer configuração. Resolvido com
`workers: "1"` mais teto de `1280Mi`; o pod está em **0 restarts**.

---

## ✅ Épico 2 — Execução → [especificação](epicos/E2-execucao.md)

**Encerrado em 2026-09-11.** 6 tasks, Go/No-Go 10 de 10. Um trigger em `k8s_acme_datamart` carregou
Postgres e ClickHouse a partir da silver, com a mesma query e a mesma janela, e os dois chegaram a
**1 039 682 linhas**, iguais filial a filial e sem chave repetida. Código entregue nas PRs #8 e #9.

O que custou mais do que o código foi o **dado**: a cópia de `dw_andon_peso` por `mc mirror` nunca
convergiu, porque a tabela é reescrita a cada commit e o log chega antes dos dados. Resolvido copiando
só o snapshot fixado de uma versão — [incidente #15](TROUBLESHOOTING.md#15-sparkfilenotfoundexception-na-silver).

**Replanejado em 2026-09-09 (spec v3.0).** A cadeia vai da **silver direto para os datamarts**: saem o
passo bronze → silver e a materialização da gold no Delta (feature futura, fora do escopo da POC). Sobram
2 passos Spark, e o fork já está desenhado assim — `PipelineGoldClickHouse` herda `read()`/`transform()`
de `PipelineGold` e troca só o `save()`.

Sem gold materializada, a comparabilidade dos dois braços deixa de ser estrutural e passa a depender da
**janela explícita** (T2.2) mais a silver parada durante a DAG. Por isso T2.2 vira a task mais importante
do épico, e as duas cargas vivem numa DAG só, no mesmo DagRun.

Ordem de execução: T2.1 → T2.5 → T2.2 → T2.3 ∥ T2.4 → T2.6. **T2.7 não existe na v3.0.**

### ✅ Contrato da silver — fechado em 2026-09-09
Decisão: **a seed se adequa ao prefixo**. `airflow/mongo-seed/` corrigido em três frentes, todas lidas dos
predicados de join de `fact_200_cep.sql`:

- [x] 4 tabelas com prefixo `dw_`, incluindo `dw_material`
- [x] `chave_pk` da silver = `id`, `unidade_origem`, `dataset_origem` — os aliases `<tabela>_id` são da gold
- [x] `chave_pk` da gold = `filial`, `banco`, `andon_peso_id`

A chave curta da gold **perdia linha em silêncio**: `dap.id` repete entre filiais, o `hk_business_id`
colidia e o `ReplacingMergeTree` colapsava o par. Era o modo de falha dominante do épico.

### ✅ T2.1 — Re-sync com honeycomb **3.3.0** → [ADR-001](decisoes/ADR-001-resync-honeycomb.md)
Aceite: **173 passed, 28 deselected** — idêntico à tag pura, logo o rsync não introduziu nada

- [x] `rsync` da tag `3.3.0` (`a5f2fa4`) sobre `spark-source-code/`, extraída por `git archive`
- [x] Preservar `utils/clickhouse_connection.py`
- [x] `DATAMART_CH_*` no `_ENV_SCHEMA` (corrige **D2**)

**Não é `main`.** O `origin/main` (`b0d7472`) reescreveu `fact_200_cep.sql` como `UNION ALL` de 8 tabelas
silver mais a gold `dim_limites`, e removeu o filtro temporal. A 3.3.0 é a última tag em que a query bate
com o dado ingerido e com o desenho do épico.

**A imagem passou a carregar esse código em 2026-09-10.** Até então `honeycomb:poc` era overlay do
`:latest` do registry — sem `datamart_ch` na factory e com o `INTERVAL 10 DAYS` na query. O registry não
publica a 3.3.0 (só `3.9.0`..`3.9.10`), então a base sai do `Dockerfile` da própria release, que veio no
rsync e é auto-contido. Junto foi corrigido o `minikube image load`, **no-op silencioso** quando a tag já
existe no nó — [incidente #12](TROUBLESHOOTING.md#12-minikube-image-load-não-substitui-tag-existente).

### ✅ T2.5 — Contrato de entrada da silver
**Não é task de código.** O usuário ingere as tabelas silver já em Delta, de fora do repositório. Aqui só
fica declarado o que o resto do épico precisa encontrar.

Aceite: conferido em 2026-09-10 lendo o `schemaString` do `metaData` de cada checkpoint Delta

- [x] 4 tabelas Delta em `s3a://datamart/business_datavault_data-bee/<tabela>/`, nomes `dw_` do contrato
- [x] Cada uma com `id`, `unidade_origem`, `dataset_origem` — os `INNER JOIN` da query casam por elas
- [x] Uma tabela por nome, com todas as filiais dentro: a discriminação é por coluna, não por caminho

`real` e os dois limites vêm como `decimal(5,1)`, o que confirma o `Decimal(9,3)` da DDL do ClickHouse.

`unidade_origem` e `dataset_origem` são colunas do dado, não do caminho. Ausentes, o job morre em
`UNRESOLVED_COLUMN`.

### ✅ T2.2 — Janela de carga (corrige **D1**) → [ADR-004](decisoes/ADR-004-janela-de-carga.md)
Aceite: as 2 queries parseiam no Spark com a janela substituída, e o build **recusa** sem o parâmetro

- [x] `JANELA_INICIO`/`JANELA_FIM` em `queryutils.build_query`, vindos de `runtime_parameters`
- [x] `--janela_inicio`/`--janela_fim` em `main.py`, campos em `PipelineConfig`
- [x] `INTERVAL 10 DAYS` substituído por predicado fechado à esquerda e aberto à direita
- [x] Guarda de cobertura integral antes da troca de partição (em T2.3)
- [x] Abortar quando o parâmetro faltar — **nunca** cair de volta para `current_timestamp()`
- [x] 6 testes unitários novos, mais o de contrato passando a exigir a janela

A recusa é por varredura do texto **após** a substituição: query sem placeholder segue intacta, query com
placeholder e sem valor aborta nomeando qual faltou.

### ✅ T2.3 — Braço ClickHouse → [ADR-002](decisoes/ADR-002-modelagem-clickhouse.md)
Aceite: **5 passed** contra o ClickHouse real, com o usuário `u_acme_loader` — valida o RBAC junto

- [x] `utils/clickhouse_datamart.py` — cliente HTTP para DDL e troca de partição
- [x] `RepositoryDatamartClickhouse`: `read`/`transform` iguais aos de `RepositoryGoldDatamart`, só `write` difere
- [x] `PipelineDatamartClickhouse` + chave `datamart_ch` na factory
- [x] `ddl/clickhouse/01_fact_200_cep.sql`, aplicada em `dm_acme` e `dm_globex`
- [x] 5 testes de integração: sequência completa, recarga, e as 3 guardas

Sem testcontainers: o pacote não está no `requirements.txt` da 3.3.0 e há um ClickHouse de pé. Os testes
pulam sozinhos quando `DATAMART_CH_*` não está no ambiente.

`load_dts` é `current_timestamp()` avaliado em cada braço: os dois destinos terão valores diferentes
nessa coluna **por construção**. Comparação linha a linha no E3 precisa excluí-la.

### ✅ T2.4 — Braço Postgres e simetria experimental
Aceite: `read` e `transform` são o **mesmo objeto de função** nos dois braços — asserção executável,
não conferência de olho. **190 passed** na unidade, **6 passed** no Postgres real, **6 passed** no
ClickHouse real

- [x] Base `RepositoryDatamart` com `read`/`transform`; os dois braços sobrescrevem só `write`
- [x] `.na.drop` no `transform` compartilhado, dirigido por `colunas_obrigatorias`
- [x] `DECIMAL(9,3)` em vez de `DOUBLE`, na query — os 4 casts, incluindo o do `WHERE`
- [x] Chave `datamart_pg` na factory
- [x] `colunas_obrigatorias` no `PipelineConfig`, no `main.py` e no `--tables_json`, mais a seed

A herança não veio de `PipelineGold`, como a v3.0 previa: na 3.3.0 o braço Postgres é
`RepositoryGoldDatamart`, um `Repository`, e `PipelineGold` grava Delta. A base nova fica no nível do
repositório, que é onde `read`/`transform` de fato vivem.

**A limpeza é configurável, não constante.** `["timestamp", "filial", "banco", "unidade_producao_id"]`
é específico de `fact_200_cep`; fixá-la na classe quebraria toda tabela gold que não tenha essas
colunas. Vazio é o default e desliga a limpeza, então `gold_datamart` em produção não muda.

**Defeito encontrado no caminho:** `tests/integration/conftest.py` construía `PipelineConfig` com
`pipeline_type`/`config_name`/`topic`/`chave_pk`, campos que não existem mais. Os 6 testes do braço
Postgres erravam no setup **desde antes da 3.3.0** e ninguém viu, porque `-m "not integration"` os
deselecionava. Corrigido e executados pela primeira vez.

### ✅ T2.6 — DAG
Aceite: run `manual__2026-09-11T12:33:58` com as 5 tasks em `success`, e **1 039 682 linhas nos dois
destinos** para a janela `2026-09-01` → `2026-10-01` —
[§5.8](TESTES.md#58-t26--aceite-sobre-o-snapshot-fixado)

- [x] `k8s_acme_datamart.py` e `k8s_globex_datamart.py` sobre `datamart_dag.py`, duas cargas **em sequência**
- [x] 1 manifesto com placeholders `TENANT`/`VERSION` — o que difere entre os braços é só `--pipeline`
- [x] Janela do `data_interval` do DagRun, calculada **uma vez** e consumida pelos dois
- [x] `spark-<tenant>-config` e `spark-<tenant>-secret`, que dão sentido ao placeholder `TENANT`
- [x] Catálogo ClickHouse na `SparkSessionFactory` — lacuna de T2.3, só visível fora do teste
- [x] Carga concluída nos dois destinos — `carga_postgres` em 7,5 min, `carga_clickhouse` em 8,7 min

| Filial | Postgres | ClickHouse |
|---|---:|---:|
| LIMEIRA | 710 774 | 710 774 |
| MARACANAU | 68 632 | 68 632 |
| PAULINIA | 212 264 | 212 264 |
| POMPEIA | 7 463 | 7 463 |
| UBERABA | 40 549 | 40 549 |
| **Total** | **1 039 682** | **1 039 682** |

`count(distinct hk_business_id)` também é 1 039 682 dos dois lados. O ClickHouse tem uma única partição
ativa, `202609`. A primeira tentativa, em 2026-09-10, morreu em `SparkFileNotFoundException` por
arquivos da silver ausentes no bucket. Era dado, não código, e foi resolvida pela recópia fixada em
uma versão.

Uma DAG, não duas encadeadas por Dataset: sem gold materializada não há produtor, e o mesmo DagRun é o
que garante janela idêntica nos dois braços. Em sequência porque dois drivers Spark não cabem no nó.

Saem: a DAG de `bronze_silver`, a DAG de gold, o Dataset e os `outlets`, o `expand_kwargs` por filial e o
`max_active_tis_per_dag=1`.

---

## Épico 3 — Validação → [especificação](epicos/E3-validacao.md)

### Estado de partida — o que o E2 entrega ao E3

**Pronto**

- [x] Silver **imóvel**: `dw_andon_peso` é o snapshot da v3321 de produção, com o log até 3321 e os 5
      arquivos vivos. Nada escreve nela, então a premissa "silver parada durante a carga" deixa de
      depender de disciplina
- [x] Distribuição da v3321 medida: **11,95 M de linhas em 14 meses**, 5 filiais —
      [§5.8](TESTES.md#58-t26--aceite-sobre-o-snapshot-fixado)
- [x] `dm_acme` carregado em 2026-09, com paridade entre os dois destinos

**Falta.** Os quatro primeiros vão nesta ordem, cada um dependendo do anterior. O do globex é
independente e só precisa vir antes da T3.3.

- [x] **Decidir o volume** — decisão do usuário em 2026-09-11: **de 2026-07 a 2026-09**, 7,13 M de
      linhas na silver, 60 % da v3321. São os três meses de maior volume, todos com as 5 filiais
- [x] 2026-07 e 2026-08 carregados em `dm_acme` (`-e 2026-07-15`, `-e 2026-08-15`), 19 e 22 min.
      **7 114 401 linhas nos dois destinos**, com diferença zero em `count` e `sum(valor)` nas 15 combinações
      `(filial, mês)` — [TESTES §6.1](TESTES.md#61-preparação--carga-do-recorte-e-índice-do-dashboard)
- [x] `ddl/postgres/02_indices.sql` em `dm_acme` — `ix_fact_200_cep_dash`, 401 MB, mais `ANALYZE`
- [x] `ddl/postgres/03_pg_tuned.sql` em `dm_acme` — 3 partições mensais, 7 114 401 linhas, `EXCEPT ALL`
      contra o `public` vazio. Revelou o [D17](TESTES.md#d17--o-gold_tuned-criava-os-índices-antes-da-carga):
      o script criava os índices antes da carga. Corrigido no script e reconstruído no banco
- [x] `dm_globex` carregado com o mesmo recorte: 7 114 401 linhas, paridade exata entre os destinos e
      **idêntico ao `dm_acme`** — os dois leem a mesma silver. Só a T3.3 usa este tenant
- [x] RBAC com o dado real: `verify-rbac.sh` 4/4, e cada leitor lê o próprio tenant e recebe `Code 497`
      no outro

- [x] Recarga de 2026-09 sobre o dado existente: idempotente nos dois destinos. O merge do Postgres
      custou 3,5× a primeira carga; a troca de partição do ClickHouse, ~0,1 s nos dois casos —
      [TESTES §6.2](TESTES.md#62-recarga-sobre-dado-existente--o-custo-do-merge)
- [x] `VACUUM (PARALLEL 0, ANALYZE)` em `public.fact_200_cep` depois da recarga — zero versões mortas
- [x] Correção do [D18](TESTES.md#d18--o-devshm-de-64-mib-derruba-o-vacuum-paralelo-do-postgres)
      aplicada: `/dev/shm` de 256Mi no Postgres. Um hash join paralelo chegou a 96 MiB ali, o que falharia
      com o limite antigo

**A preparação está completa.** Os dois portões, T3.0 e T3.1, estão fechados. O próximo passo é a T3.2.

### ✅ T3.0 — **PORTÃO** de janela
Aceite: [ADR-004](decisoes/ADR-004-janela-de-carga.md) preenchido com o número medido — 1 partição por
execução na POC, 2 em produção; atraso p99 de 213 h — [TESTES §6.4](TESTES.md#64-t30--portão-de-janela)

- [x] Medir meses distintos tocados por execução típica — e o atraso de chegada, nas 5 filiais
- [x] Decidir `REPLACE PARTITION` × `ReplacingMergeTree` — medidos os dois, com três variantes de RMT
- [x] Registrar no ADR-004

**Decisão do usuário:** a POC fica com `REPLACE PARTITION`; produção terá o B2 (RMT por watermark de
`load_dts`), a implementar no futuro. O foco da POC segue sendo a comparação de leitura entre ClickHouse
e Postgres. Análise em [analise-estrategia-carga-clickhouse.md](analise-estrategia-carga-clickhouse.md).

### ✅ T3.1 — **PORTÃO** de corretude
Aceite: `bash benchmark/compare-counts.sh` → `delta = 0` em **toda** linha — 15 chaves
`(filial, mês)` nos três braços do `dm_acme`, exit 0 — [TESTES §6.3](TESTES.md#63-t31--portão-de-corretude)

- [x] `compare-counts.sh` com `count` e `sum(valor)` por `(filial, mês)`
- [x] Executar e conferir

O braço `pgt` entra na comparação. O `dm_globex` não tem `gold_tuned` e passa com `--sem-pgt`. O caminho
de falha foi exercitado sobre extratos adulterados, sem tocar nos bancos: `count`, soma com diferença de
0,001, chave ausente e chave a mais saem todos com código 1.

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

### Abertas

| # | O quê | Bloqueia |
|---|---|---|
| 12 | **Resíduos no cluster e no disco**, para você remover: pods `pytest-t21`, `pytest-t23`, `pytest-t24`, `pytest-t26` e `smoke-clickhouse-driver` (`Completed`); `~/silver-ref/dw_andon_peso/` (20 GiB, a cópia incompleta); o database `bench_carga` do ClickHouse (3,2 GiB, 10 tabelas da análise de carga). A referência estática é `~/silver-ref/dw_andon_peso-v3321/` e **fica**. Os schemas `teste_t24*` já foram removidos | Nada, só ocupam espaço |

### Encerradas

| # | O quê | Desfecho |
|---|---|---|
| 11 | Estratégia de carga (T3.0) | **Decidido em 2026-09-11:** POC com `REPLACE PARTITION`; produção futura com B2, RMT por watermark de `load_dts` — [ADR-004](decisoes/ADR-004-janela-de-carga.md), [análise](analise-estrategia-carga-clickhouse.md) |
| 13 | Aplicar a correção do D18 | **Aplicada em 2026-09-11**, com autorização do usuário: `/dev/shm` de 256Mi no Postgres, pod recriado, dado intacto no PVC |
| 10 | Volume do E3 | **De 2026-07 a 2026-09**, decisão do usuário em 2026-09-11: 7,13 M de linhas na silver. Os 11 meses anteriores ficam de fora — 4,82 M de linhas |
| 9 | Cópia consistente de `dw_andon_peso` | **Resolvido em 2026-09-11.** O `mc mirror` da tabela viva nunca convergiu. Em vez dele, copiada só a v3321: o log até ali e os 5 arquivos que o snapshot referencia, 1,73 GiB, conferidos pelo tamanho que o log registra e pelo rodapé Parquet — [incidente #15](TROUBLESHOOTING.md#15-sparkfilenotfoundexception-na-silver) |
| 3 | Ajustar `.wslconfig` | Resolvido: perfis redimensionados para os 15,5 GiB reais |
| 4 | Primeiro commit da branch | Feito |
| 5 | Nomes de `filial` no seed do Mongo | **Não é decisão da POC**: é contrato de arquitetura entre os serviços. `scripts/minio-ui.sh` deriva e imprime os caminhos a partir do control plane, em vez de pedir que alguém os reconcilie na mão |
| 6 | `TRUNCATE` dos system logs acumulados | **Dispensável.** Medido: 11,67 MiB em disco num PVC com 819 GiB livres, e as seis tabelas estão **congeladas** (verificado por amostragem — [T-E1-38](TESTES.md#411-verificações-de-encerramento-do-épico)). O risco era de memória em merge, e esse já foi eliminado |
| 7 | CRD do Spark Operator na 2.5.0 com chart 2.5.2 | **Sem risco.** Diff dos três CRD instalados contra os do chart 2.5.2: **zero linhas divergentes** ([T-E1-39](TESTES.md#411-verificações-de-encerramento-do-épico)) |
| 1 | Ingerir as 4 tabelas silver em Delta | **Feito** em 2026-09-10. Contrato conferido coluna a coluna — T2.5 |
| 8 | Terceiro braço `pg-tuned` | **Entra.** Schema `gold_tuned`, entregue e medido — [T-E1-41](TESTES.md#412-t14-complemento--o-braço-pg-tuned) |
