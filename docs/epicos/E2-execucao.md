# Épico 2 — Execução

| Campo | Valor |
|---|---|
| Versão | 3.1 |
| Data | 2026-09-11 |
| Status | **Encerrado** — Go/No-Go 10 de 10 |
| Objetivo | Carregar os dois datamarts **direto da silver**, pela mesma query, na mesma janela |
| Depende de | [E1](E1-infraestrutura.md) completo ✅ |
| Bloqueia | E3 inteiro |
| Progresso | [../TODO.md](../TODO.md) |

---

## O que mudou da versão 2.0

A 2.0 materializava a gold no Delta e servia os dois braços a partir dela. A 3.0 corta esse passo: a
cadeia vai da **silver direto para os datamarts**. Decisão do usuário — os recursos do Delta na camada
gold são desejáveis, mas são feature de outro momento, não da POC.

| | Versão 2.0 | Versão 3.0 |
|---|---|---|
| Passos Spark | 3 (`gold`, `datamart_pg`, `datamart_ch`) | **2** (`datamart_pg`, `datamart_ch`) |
| A gold | materializada no Delta, uma vez | **não materializada** — é a query, executada por cada braço |
| O que os braços leem | o Delta gold, os mesmos bytes | a silver, pela **mesma query com a mesma janela** |
| Encadeamento | Dataset `honeycomb://<tenant>/gold` | uma DAG, duas tasks, **o mesmo DagRun** |
| Tasks | 7 | **6** — T2.7 sai |
| Código novo | guarda do Trino + config da gold | **nenhum** nessa frente |

### O que isto simplifica de verdade

O fork já está desenhado assim. Não é adaptação — é usar o que existe:

```python
class PipelineGoldClickHouse(PipelineGold):
    """Datamart: reusa exatamente a leitura da gold (query sobre a silver + hk_business_id
    + load_dts) e, no lugar de gravar Delta no MinIO, grava no ClickHouse.
    Só o `save` muda em relação à PipelineGold."""
```

Três consequências concretas:

1. **A pendência do Trino desaparece.** `TrinoConnection` é instanciado no `PipelineGoldWrapper`
   (chave `gold`) e no `PipelineBronzeToSilverWrapper` — nenhum dos dois entra no escopo. O
   `PipelineDatamartWrapper` já roda **sem passo Trino**. Não há nada a guardar por configuração.
2. **`chave_pk` e `colunas_zorder` deixam de ser pré-requisito de gravação.** `colunas_zorder` só serve
   ao `OPTIMIZE` do Delta, que sai junto. `chave_pk` continua sendo lida — ver o modo de falha abaixo.
3. **A simetria dos dois braços já é código, não plano.** `read()` e `transform()` vêm de `PipelineGold`
   por herança; só o `save()` difere. A refatoração de T2.4 vira conferência, não construção.

### O que isto custa — e como o custo é pago

A junção de quatro tabelas roda **duas vezes**, uma por braço, em instantes diferentes. Se nada mais
mudasse, os dois braços poderiam ler conjuntos de linhas diferentes, e o portão de corretude do
[E3](E3-validacao.md) acusaria uma diferença que não é do motor.

Duas coisas fecham essa brecha, e **as duas são obrigatórias**:

| Origem da divergência | Fechamento |
|---|---|
| `current_timestamp() - INTERVAL 10 DAYS` no `WHERE` da query — o corte anda entre as duas execuções | [T2.2](#t22--janela-de-carga): a janela vira parâmetro, passada pelo chamador |
| A silver mudar entre a execução de um braço e a do outro | [T2.6](#t26--dag): **uma** DAG, duas tasks, o mesmo `data_interval`; carga da silver não concorre com a DAG |

> **É a mudança de risco desta versão.** Na 2.0, a comparabilidade dos braços era garantida pela
> estrutura — mesmos bytes, não havia como divergir. Na 3.0 ela é garantida por **disciplina de janela**:
> a query é determinística *se e somente se* os limites vierem de fora e a silver estiver parada.
> T2.2 deixa de ser correção de defeito e passa a ser pré-requisito do experimento.

---

## Objetivo

Carregar Postgres e ClickHouse a partir da silver, com a mesma query e a mesma janela, para que a
comparação do [E3](E3-validacao.md) meça o motor e não o recorte do dado.

## Desenho da cadeia

```
Usuário ingere a silver em Delta  ─→  business_datavault_data-bee/<tabela>
                                          │
   MongoDB Data_Catalog.k8s_<tenant>  ────┤  control plane: tables, schedule, versão
                                          ▼
                       Airflow  →  DAG k8s_<tenant>_datamart
                                   um DagRun, uma janela
                    ┌─────────────────────┴─────────────────────┐
                    ▼                                           ▼
      task carga_postgres                          task carga_clickhouse
      --pipeline datamart_pg                       --pipeline datamart_ch
                    │                                           │
       query fact_200_cep.sql sobre a silver, JANELA_INICIO / JANELA_FIM
                    ▼                                           ▼
        Postgres (staging + ON CONFLICT)          ClickHouse dm_<tenant> (REPLACE PARTITION)
                    └─────────────────  benchmark/  ────────────┘
                            mesmas queries, mesmo hardware
```

## Premissas

1. O usuário ingere as tabelas silver **já em Delta**, de fora do repositório. A POC não converte, não
   copia e não gera dado sintético.
2. O delta de código sobre o honeycomb é desenhado para virar PR upstream.
3. **A silver não muda durante a execução da DAG.** É premissa, não garantia do código — ver
   [riscos de T2.6](#riscos-4).
4. Os dois braços recebem os **mesmos** `JANELA_INICIO` e `JANELA_FIM`, do mesmo DagRun.

## Fora de escopo

| Fora | Por quê |
|---|---|
| `bronze_silver` via Spark | Não produz evidência para a tese. O usuário ingere a silver pronta, em Delta |
| **Gold materializada no Delta** | Decisão do usuário: os recursos do Delta na gold são feature futura, não da POC |
| Passo Trino (`create_table`) | Nenhum pipeline do escopo o invoca — ver [o que isto simplifica](#o-que-isto-simplifica-de-verdade) |
| `oee_cleaner` | Não participa da cadeia do datamart |
| Alterar o honeycomb produtivo | O que sai daqui é patch candidato, não deploy |

---

## Contrato da silver — fechado

Resolvido em 2026-09-09. **Decisão do usuário: a seed se adequa ao prefixo.** A query
`resources/queries/fact_200_cep.sql` é a fonte da verdade; `airflow/mongo-seed/k8s_<tenant>.json` foi
corrigido para bater com ela.

```
delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_andon_peso`        dap
delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_ordem_producao`    dop
delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_unidade_producao`  dup
delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_material`          dma
```

Ao aplicar o prefixo apareceram mais duas divergências, ambas lidas dos predicados de join:

| Campo | Antes | Agora | Consequência de não corrigir |
|---|---|---|---|
| `tables[].name` | 3 tabelas sem prefixo | 4 tabelas com `dw_` | `Path does not exist` na primeira execução |
| `chave_pk` da silver | `andon_peso_id`, … | `id`, `unidade_origem`, `dataset_origem` | `--validate` de T2.5 conferindo coluna inexistente |
| `chave_pk` da gold | `["andon_peso_id"]` | `["filial", "banco", "andon_peso_id"]` | **Perda silenciosa de linha** — ver abaixo |

> **A chave curta da gold perdia dado.** `andon_peso_id` é `dap.id`, único dentro de uma origem; a fato
> agrega várias (`filiais[]` declara duas por tenant). Duas filiais com o mesmo `id` produzem o mesmo
> `hk_business_id`, o `ReplacingMergeTree` do ClickHouse colapsa o par e **uma das linhas some sem erro**.
> `filial` e `banco` estão na projeção da query exatamente por isso — a chave só não os usava.
>
> Este era o modo de falha dominante do épico. Continua valendo o assert
> `count(distinct hk_business_id) == count(*)` no aceite de T2.3: ele é o que provaria a correção, ou
> apanharia a próxima variante do mesmo erro com o dado real.

O `--validate` de T2.5 é a rede: um `name` que não exista no MinIO falha ali, nomeado, antes de qualquer
job Spark.

---

## Tasks

Na ordem de execução. A numeração é de criação, não de ordem. **T2.7 não existe na 3.0.**

| Ordem | Task | Entrega | Bloqueia |
|---|---|---|---|
| 1 | [T2.1](#t21--re-sync-com-honeycomb-330) | Re-sync de `spark-source-code/` com a tag `3.3.0` do honeycomb | todas |
| 2 | [T2.5](#t25--contrato-de-entrada-da-silver) | Contrato de entrada da silver — documentação | T2.3, T2.4 |
| 3 | [T2.2](#t22--janela-de-carga) | Query parametrizada por janela — corrige **D1** | T2.3, T2.4 |
| 4 | [T2.3](#t23--braço-clickhouse) | `RepositoryDatamartClickhouse` + troca de partição | T3.1 |
| 5 | [T2.4](#t24--braço-postgres-e-simetria-experimental) | Braço Postgres pela mesma leitura | T3.1 |
| 6 | [T2.6](#t26--dag) | DAG única com as duas cargas | — |

T2.3 e T2.4 são paralelizáveis depois de T2.2.

---

## T2.1 — Re-sync com honeycomb `3.3.0`

Decisão e argumento completo em [ADR-001](../decisoes/ADR-001-resync-honeycomb.md). **Inalterado desde a 1.0.**

### Ações

1. `rsync` da tag **`3.3.0`** sobre `spark-source-code/`, excluindo `.git`, `.venv`, `__pycache__`,
   `.pytest_cache`. Extrair com `git archive`, que não altera o repositório de origem.
   **Não usar `main`**: ver [ADR-001, revisão de 2026-09-09](../decisoes/ADR-001-resync-honeycomb.md).
2. Preservar `src/main/utils/clickhouse_connection.py` do fork — 30 linhas, sem dependência do código
   velho, e é o único ativo que o fork tem.
3. Acrescentar ao `_ENV_SCHEMA` de `src/main/utils/config_manager.py`:

   | Env var | Caminho pontilhado |
   |---|---|
   | `DATAMART_CH_URL` | `datamart_clickhouse.url` |
   | `DATAMART_CH_USER` | `datamart_clickhouse.user` |
   | `DATAMART_CH_PASSWORD` | `datamart_clickhouse.password` |
   | `DATAMART_CH_DATABASE` | `datamart_clickhouse.database` |
   | `DATAMART_CH_CATALOG` | `datamart_clickhouse.catalog` |

### Por que o prefixo novo (defeito D2)

`CLICKHOUSE_URL`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SERVICE_NAME` e
`CLICKHOUSE_VERIFY_SSL` **já existem** no `_ENV_SCHEMA` e pertencem ao `handler_logger` de auditoria, que
em produção aponta para `api-logs.datawake.cloud`. Reusar o prefixo faz o log de auditoria escrever no
datamart, ou o contrário.

`DATAMART_CH_DATABASE` por tenant é o análogo exato de `POSTGRES_DATABASE` por tenant — então a
substituição `spark-TENANT-config` que a DAG produtiva já faz funciona sem nenhuma alteração. Essa
simetria é o que torna o patch aceitável upstream.

### O que o re-sync apaga, e o que ganha

Somem `PipelineGoldClickHouse` e `PipelineDatamartWrapper`, adições do fork que gravavam por `append`
simples — substituídas por T2.3.

Entra o que o fork não tinha: **`RepositoryGoldDatamart`**, o braço Postgres pronto, com staging via JDBC
e `INSERT ... ON CONFLICT`. É o molde que T2.3 espelha e o que reduz T2.4 a conferência.

### A imagem precisa carregar esse código

Rsync sozinho não muda o que roda no cluster: `honeycomb:poc` era overlay de
`hub.datawake.cloud/dw-dados/honeycomb:latest`, e o app vinha de lá — sem `datamart_ch` na factory e
com o `INTERVAL 10 DAYS` na query. O registry não publica a 3.3.0 (só `3.9.0`..`3.9.10`), então a base
passa a sair do `Dockerfile` da própria release, que veio no rsync e é auto-contido:

```bash
docker build -f spark-source-code/Dockerfile -t honeycomb:3.3.0-local spark-source-code/
docker build -f images/spark/Dockerfile      -t honeycomb:poc         images/spark/
docker save honeycomb:poc | docker exec -i minikube docker load
```

> **O `minikube image load` é no-op silencioso quando a tag já existe no nó**, mesmo com
> `--overwrite=true`. Com `imagePullPolicy: Never`, o pod acha a tag e roda o código velho — job verde,
> evidência inválida. Ver [incidente #12](../TROUBLESHOOTING.md#12-minikube-image-load-não-substitui-tag-existente).

### Artefatos

`spark-source-code/` (substituído), `src/main/utils/config_manager.py` (alterado),
`images/spark/Dockerfile` (reescrito), `scripts/bootstrap.sh` (alterado).

### Aceite

```bash
cd spark-source-code && pytest -q
```
A suíte unitária do honeycomb passa **sem nenhuma alteração** após o rsync.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Re-sync quebra algo que só o fork tinha | `pytest` passa mas o job falha no cluster | Rodar `pytest -q` imediatamente após o rsync, antes de qualquer edição |

---

## T2.5 — Contrato de entrada da silver

**Não é task de código.** O usuário ingere as tabelas silver já em Delta, vindas de outro lugar. O
repositório não converte, não copia e não valida — só declara onde o dado precisa estar para o resto do
épico funcionar.

### O contrato

```
s3a://datamart/business_datavault_data-bee/<tabela>/     ← tabela Delta
```

`<tabela>` é o `name` de cada entrada de `tables` no documento Mongo do tenant, conforme o
[contrato da silver](#contrato-da-silver--fechado): `dw_andon_peso`, `dw_ordem_producao`,
`dw_unidade_producao`, `dw_material`.

A query lê **por caminho Delta**, não por catálogo — é o que permite dispensar o Trino. Uma tabela Delta
por nome, com todas as filiais dentro: a discriminação é por coluna, não por caminho.

### Colunas que a query exige

Lidas dos predicados de join de `resources/queries/fact_200_cep.sql`:

| Tabela | Colunas usadas |
|---|---|
| `dw_andon_peso` | `id`, `unidade_origem`, `dataset_origem`, `nr_ordem_producao`, `unidade_producao_nome`, `material`, `data_hora`, `real`, `limite_superior`, `limite_inferior` |
| `dw_ordem_producao` | `id`, `unidade_origem`, `dataset_origem`, `nr_ordem_producao`, `produto_id` |
| `dw_unidade_producao` | `id`, `unidade_origem`, `dataset_origem`, `codigo` |
| `dw_material` | `id`, `unidade_origem`, `dataset_origem`, `codigo` |

> **`unidade_origem` e `dataset_origem` são colunas do dado, não do caminho.** Os três `INNER JOIN` da
> query casam por elas. Se vierem ausentes, o job morre em `UNRESOLVED_COLUMN` — e nenhuma conferência de
> caminho teria pego.

### Aceite

As quatro tabelas existem como Delta nos caminhos acima, com as colunas da tabela acima, e o
[T3.0](E3-validacao.md#t30--portão-de-janela) consegue medir a distribuição de `data_hora`.

**Cumprido em 2026-09-10.** Conferido sem Spark, lendo o `schemaString` do `metaData` de cada checkpoint
Delta pelo `s3()` do ClickHouse: as quatro tabelas existem com o nome `dw_`, todas trazem `id`,
`unidade_origem` e `dataset_origem`, e `real`/`limite_superior`/`limite_inferior` vêm como
`decimal(5,1)` — o que confirma o `Decimal(9,3)` da DDL. Falta só o T3.0, que mede a distribuição.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Caminho ou nome divergente do contrato | `Path does not exist` na primeira execução do braço | Contrato acima, conferido pelo usuário na ingestão |
| Tabela vazia | O braço conclui com **sucesso** e zero linha | O assert de contagem de T2.6 acusa |
| Silver recarregada durante o DagRun | Os dois braços leem conteúdos diferentes | Premissa 3 — não ingerir com DAG ativa |

---

## T2.2 — Janela de carga

Corrige o defeito **D1** ([ARQUITETURA.md §4](../ARQUITETURA.md#4-defeitos-verificados-no-ambiente)).
Decisão em [ADR-004](../decisoes/ADR-004-janela-de-carga.md).

> **Esta é a task mais importante do épico na versão 3.0.** Sem a gold materializada, a janela é a única
> coisa que faz os dois braços verem o mesmo conjunto de linhas. Na 2.0 ela corrigia um defeito de
> escrita; aqui ela sustenta o experimento inteiro.

### Os dois problemas que a janela resolve

| Problema | Sem a janela | Com a janela |
|---|---|---|
| **Determinismo entre os braços** | `current_timestamp()` anda entre a execução do braço PG e a do CH: recortes diferentes, contagens diferentes | O corte é literal e vem do DagRun — idêntico nos dois |
| **D1, incremento parcial** | Trocar a partição `202609` com um DataFrame de dias 10–20 **apaga** os dias 1–9 | A janela alinha ao mês, e a guarda recusa cobertura parcial |

### O problema, concretamente

`spark-source-code/resources/queries/fact_200_cep.sql:44`:

```sql
WHERE to_timestamp(substring(dap.data_hora, 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS')
      >= current_timestamp() - INTERVAL 10 DAYS
```

Duas falhas na mesma linha: o predicado é **aberto à direita e móvel**, e não tem fronteira de mês. Com
`PARTITION BY toYYYYMM(timestamp)` no destino, trocar a partição `202609` com um DataFrame que contém só
parte do mês apaga o resto. Sem erro, sem aviso. A contagem da partição simplesmente cai.

### Ações

1. `src/main/utils/queryutils.py`, função `build_query`: acrescentar os placeholders `JANELA_INICIO` e
   `JANELA_FIM` à substituição que já existe para `MINIO_BASE_PATH`, `S3_PATH_SILVER` e `S3_PATH_GOLD`.
   São ~6 linhas, e é uma mudança digna de upstream. Os valores vêm de `config_params`, não do ambiente —
   é o que permite a DAG passá-los por execução.
2. `resources/queries/fact_200_cep.sql`: substituir o predicado por
   ```sql
   WHERE ts >= 'JANELA_INICIO' AND ts < 'JANELA_FIM'
   ```
   com os limites arredondados para o primeiro dia do mês.
3. Guarda executável no repositório de destino, antes da troca:
   - `min(df.timestamp) >= JANELA_INICIO`
   - `JANELA_INICIO == toStartOfMonth(JANELA_INICIO)`
4. **Falhar quando o parâmetro faltar.** Sem janela informada, abortar — nunca cair de volta para
   `current_timestamp()`. O fallback silencioso reintroduziria D1 exatamente onde ninguém olharia.

### Artefatos

`src/main/utils/queryutils.py` (alterado), `resources/queries/fact_200_cep.sql` (alterado).

### Aceite

Duas conferências, não uma:

```bash
# 1. Idempotência: recarregar a mesma janela não muda a contagem da partição
bash scripts/ports.sh --only clickhouse
curl -s "http://localhost:8123/?user=..." --data-binary \
  "SELECT count() FROM dm_acme.fact_200_cep WHERE toYYYYMM(timestamp)=202609"
```

```sql
-- 2. Determinismo entre braços: a MESMA janela nos dois lados dá a MESMA contagem
SELECT count() FROM dm_acme.fact_200_cep WHERE toYYYYMM(timestamp)=202609;   -- ClickHouse
SELECT count(*) FROM public.fact_200_cep WHERE date_trunc('month', timestamp) = '2026-09-01';  -- Postgres
```

Os números de (2) são iguais. É este assert que substitui a garantia estrutural da 2.0.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Janela não propagada até a query | Job roda e carrega o recorte errado, **sem erro** | Logar a query final resolvida, uma vez, no início do job |
| Fallback para `current_timestamp()` | Divergência intermitente entre os braços, irreprodutível | Abortar quando o parâmetro faltar (ação 4) |
| Janela fora do intervalo do dado | Carga vazia com **sucesso** | Conferir a janela contra a distribuição de `data_hora` da v3321 — 2025-08 a 2026-09, em [TESTES §5.8](../TESTES.md#58-t26--aceite-sobre-o-snapshot-fixado) |

---

## T2.3 — Braço ClickHouse

### A leitura, que agora vem de graça

O braço herda `read()` e `transform()` de `PipelineGold`: a query sobre a silver, mais `hk_business_id`
(SHA-256 das colunas de `chave_pk`) e `load_dts`. **Só o `save()` é novo.** É a forma que o fork já tem;
o que T2.3 troca é o destino do `save`, hoje um `writeTo(...).append()` simples.

> **`load_dts` é `current_timestamp()`, avaliado em cada braço.** Os dois datamarts terão valores
> diferentes nessa coluna, por construção. Comparação linha a linha no [E3](E3-validacao.md) precisa
> excluí-la explicitamente — é o falso positivo mais provável do portão de corretude.

### O que o connector não faz

| Operação | Connector ClickHouse-Spark |
|---|---|
| `INSERT` | ✅ `df.writeTo("cat.db.tbl").append()` |
| `CREATE TABLE` via catálogo | ⚠️ não expressa `LowCardinality` nem `Decimal` fielmente |
| `ALTER TABLE ... REPLACE PARTITION` | ❌ **impossível** |

`ALTER TABLE` é parseado pelo **Spark**, e a API V2 só suporta `ADD`/`DROP`/`RENAME COLUMN` e
`SET TBLPROPERTIES`. Não há escape hatch de SQL arbitrário na API pública do connector.

**Conclusão: é preciso um cliente HTTP no driver.** Isso não é custo novo — `handler_logger.py` já faz
`requests.Session().post()` contra o ClickHouse hoje, e `requests` já está no `requirements.txt`. Zero
dependência nova, zero jar novo.

E a DDL das tabelas **não** passa pelo Spark: fica em `ddl/`, aplicada por Job, para preservar
`LowCardinality(String)` e `Decimal(9,3)` exatos.

### Ações

1. `src/main/utils/clickhouse_datamart.py` — cliente HTTP para DDL e troca de partição.
2. `RepositoryDatamartClickhouse` em `src/main/repo/repository.py`, com esta sequência de escrita:
   ```
   1. particoes = df.select(date_format("timestamp","yyyyMM")).distinct().collect()
   2. GUARDA DE COBERTURA INTEGRAL                                          ← D1
   3. stg = f"{tabela}_stg_{uuid4().hex[:8]}"
      CREATE TABLE {db}.{stg} AS {db}.{tabela}
   4. df.repartition(date_format("timestamp","yyyyMM")).writeTo(f"{cat}.{db}.{stg}").append()
   5. CONFERÊNCIA: count(stg) == df.count()   → aborta ANTES da troca
   6. para cada p: ALTER TABLE {db}.{tabela} REPLACE PARTITION '{p}' FROM {db}.{stg}
   7. finally: DROP TABLE {db}.{stg}
   ```
3. `PipelineDatamartClickhouse` em `core/pipeline_orchestrator.py`, herdando de `PipelineGold` e
   sobrescrevendo só o `save()`.
4. Chave `datamart_ch` em `core/pipeline_factory.py`, substituindo a `datamart` genérica do fork.
5. DDL em `ddl/clickhouse/01_fact_200_cep.sql` conforme
   [ARQUITETURA.md §3.2](../ARQUITETURA.md#32-ddl-alvo).
6. Teste de integração com testcontainers, **incluindo caso de recarga da mesma partição**.

### Notas sobre cada passo

| Passo | Armadilha |
|---|---|
| 3 | Uma staging **por execução**, não por partição. `CREATE TABLE AS` copia estrutura, engine, `ORDER BY`, `PARTITION BY` e política de armazenamento — os três requisitos do `REPLACE PARTITION`. O sufixo `uuid4` evita colisão entre execuções concorrentes |
| 4 | O `repartition` pela expressão de partição evita `Code 252 TOO_MANY_PARTS`: sem ele, cada task escreve em todas as partições e são geradas `tasks × partições` parts |
| 4 | Manter `async_insert` **desligado** neste caminho — queremos um INSERT grande por task, não micro-lotes |
| 6 | A atomicidade é **por partição**, não pelo conjunto. Com 3 meses tocados há uma janela em que 1 de 3 está trocado. Aceitável na POC; testar comandos separados por vírgula num único `ALTER` é barato e pode resolver |
| 6 | A staging fica no **mesmo database** do alvo — cross-database funciona em versões recentes mas não sempre |

### Saída de emergência

Se o connector brigar (incidente #11 reaparecendo sobre Java 17), o caminho é remover o connector do
caminho crítico: `df.write.parquet("s3a://datamart/stage/<tabela>/<part>/")` e depois
`INSERT INTO stg SELECT * FROM s3(...)` disparado pelo mesmo cliente HTTP. Usa só `requests` e a config
S3A que já existe. Credenciais por *named collection*, nunca inline — inline vaza em `system.query_log`.

### Artefatos

`src/main/utils/clickhouse_datamart.py`, `src/main/repo/repository.py` (alterado),
`src/main/core/pipeline_orchestrator.py` (alterado), `src/main/core/pipeline_factory.py` (alterado),
`ddl/clickhouse/01_fact_200_cep.sql`,
`tests/integration/test_repository_datamart_clickhouse.py`.

### Aceite

```bash
pytest -m integration tests/integration/test_repository_datamart_clickhouse.py -q
```
`passed`, incluindo o caso de recarga da mesma partição.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| **D1**: incremento parcial | `count()` da partição **cai** entre execuções, sem erro | Guarda do passo 2 + janela (T2.2) |
| `chave_pk` vazia | Todo `hk_business_id` colide; o `ReplacingMergeTree` colapsa a tabela para poucas linhas, **sem erro** | Assert `count(distinct hk_business_id) == count(*)` no aceite |
| `NULL` em coluna de particionamento | `Code 349 CANNOT_CONVERT_TO_NULLABLE` no meio da carga | Limpeza no `transform` compartilhado (T2.4) |
| Staging concorrente | `Table ..._stg already exists`, ou troca com dado de outra execução | Sufixo `uuid4().hex[:8]` |
| `Code 252 TOO_MANY_PARTS` | INSERT rejeitado após alguns lotes | `repartition` no passo 4 |
| Política de armazenamento divergente | `Tables have different storage policies` | `CREATE TABLE AS` garante hoje; quebra se um dia houver tiering só no destino |

---

## T2.4 — Braço Postgres e simetria experimental

### Onde a simetria vive na versão 3.0

Na 2.0 a simetria era estrutural: os dois braços liam o mesmo arquivo. Aqui ela tem **duas fontes**, e
convém saber qual cobre o quê:

| Fonte | O que garante | O que não garante |
|---|---|---|
| Herança de `PipelineGold` | `read()` e `transform()` são literalmente o mesmo código nos dois braços | Nada sobre *quando* cada um roda |
| Janela explícita (T2.2) | Os dois recortam o mesmo intervalo, mesmo rodando em instantes diferentes | Nada se a silver mudar no meio |

O que sobra descoberto é a premissa 3 — silver parada durante a DAG. Não há como o código garantir isso;
está registrado como risco em T2.6.

### Ações

1. Extrair a base `RepositoryDatamart` com `read()` e `transform()`; `RepositoryGoldDatamart` e
   `RepositoryDatamartClickhouse` herdam dela e sobrescrevem **só** `write()`. Se algum precisar tocar
   `read()` ou `transform()`, a diferença sobe para a base — nunca fica em um dos ramos.

   > **Não é `PipelineGold`, como a v3.0 previa.** Na 3.3.0 o braço Postgres é `RepositoryGoldDatamart`,
   > um `Repository` próprio, e `PipelineGold` grava Delta. A herança certa fica no nível do repositório,
   > que é onde `read`/`transform` de fato vivem.
2. No `transform` compartilhado, acrescentar a limpeza, **dirigida por configuração**:
   ```python
   .na.drop(subset=self.colunas_obrigatorias)   # vazio = desligada
   ```
   logando a contagem descartada. A lista chega por `--colunas_obrigatorias`, por `--tables_json`
   (`colunas_obrigatorias` por tabela) ou pela seed do Mongo.

   > **Constante quebraria produção.** `["timestamp", "filial", "banco", "unidade_producao_id"]` é
   > específico de `fact_200_cep`; fixá-la na classe faria `na.drop` estourar em toda tabela gold que
   > não tenha essas colunas. Vazio é o default, então `gold_datamart` não muda de comportamento.
3. Trocar `CAST(... AS DOUBLE)` por `DECIMAL(9,3)` na query — assim os **dois** braços recebem o tipo
   certo da origem, em vez de cada um converter por conta. Precisão 9 porque a origem é `Decimal(5,1)`,
   medida no Parquet real: cabe em `Decimal32` (4 bytes), enquanto 10 a 18 forçam `Decimal64` (8).
4. Chave `datamart_pg` em `core/pipeline_factory.py`, apelido de `gold_datamart`: a DAG fica simétrica
   com `datamart_ch` também no nome.

### Por que a limpeza precisa ser compartilhada

`timestamp` não pode ser `Nullable` sob `PARTITION BY`, mas
`to_timestamp(substring(dap.data_hora, 1, 23), ...)` devolve `NULL` em string malformada, e
`dop.produto_id`/`dup.id` vêm de `INNER JOIN` mas passam se a silver tiver a coluna nula.

> Se só o braço ClickHouse limpar, as contagens divergem e a tabela de corretude (T3.1) acusa uma diferença
> que **não é do motor** — o benchmark perde credibilidade por um bug nosso.

### Artefatos

`src/main/repo/repository.py` (refatorado), `resources/queries/fact_200_cep.sql` (alterado),
`src/main/core/pipeline_factory.py` (alterado), `src/main/utils/pipeline_config.py` (alterado),
`src/main/main.py` (alterado), `src/main/core/table_runner.py` (alterado),
`tests/unit/test_repository_datamart.py` (novo), `airflow/mongo-seed/k8s_<tenant>_gold.json` (alterado).

### Aceite

A simetria vira asserção executável, em vez de conferência de olho:

```python
assert RepositoryGoldDatamart.read      is RepositoryDatamart.read
assert RepositoryGoldDatamart.transform is RepositoryDatamart.transform
assert RepositoryDatamartClickhouse.read      is RepositoryDatamart.read
assert RepositoryDatamartClickhouse.transform is RepositoryDatamart.transform
assert RepositoryGoldDatamart.write is not RepositoryDatamartClickhouse.write
```

**Cumprido em 2026-09-10:** 190 passed na unidade, 6 passed no Postgres real (`dm_acme`) e 6 passed no
ClickHouse real. O sexto caso do ClickHouse é novo: `timestamp` nulo é descartado pelo `transform`
compartilhado e a troca de partição aceita a carga.

### Defeito encontrado no caminho

`tests/integration/conftest.py` construía `PipelineConfig` com `pipeline_type`, `config_name`, `topic` e
`chave_pk` — campos que não existem mais. Os 6 testes do braço Postgres erravam no **setup**, desde antes
da 3.3.0, e `-m "not integration"` escondia isso. Eram a única cobertura do código que esta task
refatora. Corrigidos, mais dois ajustes para poderem rodar contra um Postgres já de pé:
`DATAMART_PG_*` no ambiente dispensa o testcontainers, e o fixture `spark` só pede o driver por
`spark.jars.packages` quando o jar não está no classpath — resolver por ivy exige rede.

---

## T2.6 — DAG

**Uma** DAG por tenant, `k8s_<tenant>_datamart`, com duas tasks. Cópia de
`dtlk-airflow-pipeline/dags/k8s_unipac_bronze_silver.py` com o mínimo de alteração possível.

```
k8s_<tenant>_datamart   (schedule do documento Mongo)
    │
    ├── carga_postgres    SparkKubernetesOperator  --pipeline datamart_pg
    └── carga_clickhouse  SparkKubernetesOperator  --pipeline datamart_ch
```

### Por que uma DAG e não duas encadeadas por Dataset

Na 2.0, o Dataset `honeycomb://<tenant>/gold` existia porque havia um **produtor**: a DAG de gold. Sem
gold materializada não há produtor, e um Dataset ancorado na silver seria disparado pela carga manual do
usuário — que não é uma DAG.

O ganho não é só de simplificação. As duas tasks no mesmo DagRun compartilham `data_interval_start` e
`data_interval_end`, que é de onde saem `JANELA_INICIO` e `JANELA_FIM`. **A janela idêntica nos dois
braços passa a ser consequência da estrutura da DAG**, não de dois agendamentos concordarem.

As duas tasks rodam **em sequência**, não em paralelo: o nó tem ~3 GiB livres do orçamento de E1, e dois
drivers Spark com seus executores não cabem juntos. O E3 mede latência de **leitura** no destino, então
o tempo de parede da carga não é variável do experimento.

### O que preservar literalmente

| Elemento | Por quê |
|---|---|
| `serverSelectionTimeoutMS=5000` no `MongoClient` | Sem isso, o Mongo fora do ar estoura o `dagbag_import_timeout` e a DAG some do DagBag |
| `try/except` de parse time degradando para `schedule=None` | A DAG entra em modo manual em vez de desaparecer |
| Manifesto externo com placeholders `TENANT`/`VERSION`, sobrescrevendo **só** `spec.arguments` | É o padrão produtivo |

### O que sai em relação à 1.0 e à 2.0

| Elemento | Por quê sai |
|---|---|
| DAG `k8s_<tenant>_bronze_silver.py` | O passo não está no escopo |
| DAG `k8s_<tenant>_gold.py` e o Dataset | A gold não é mais materializada — não há produtor |
| `outlets` / `Dataset(...)` | Sem encadeamento entre DAGs, não há o que publicar |
| `expand_kwargs(montar_execucoes())` | O mapeamento dinâmico existia para rodar **1 pod por filial** na bronze→silver |
| `max_active_tis_per_dag=1` | Existia para evitar que duas filiais na mesma tabela se sobrescrevessem no `bronze_silver`. Sem tasks mapeadas, não há concorrência a limitar |
| Roteamento por `gold_type` de três vias | As tasks daqui são de destino único, uma cada — ver `airflow/mongo-seed/README.md` |

> Os três últimos saem **como consequência** de o `bronze_silver` sair, não por decisão independente. Se o
> passo voltar ao escopo, voltam junto — e o `max_active_tis_per_dag=1` não é opcional lá: sem ele, duas
> filiais na mesma tabela perdem dado real do usuário.

### Artefatos

`airflow/dags/datamart_dag.py` (fábrica compartilhada), `airflow/dags/k8s_acme_datamart.py` e
`k8s_globex_datamart.py`, `airflow/dags/manifests/spark-honeycomb-datamart.yaml`,
`infra/spark/spark-tenant-config.yaml`, `infra/spark/spark-secrets.yaml` (reescrito),
`src/main/utils/session.py` (alterado), `scripts/bootstrap.sh` e `infra/airflow/values.yaml`.

### Três desvios da v3.0, e o porquê de cada um

**Um manifesto, não dois.** Os braços diferem só no `--pipeline`, que a DAG monta. Dois arquivos
idênticos divergiriam na primeira correção feita em um deles.

**Uma fábrica e dois arquivos de tenant.** Produção tem um arquivo autocontido por tenant; com dois
tenants seriam ~200 linhas duplicadas. A lógica vive em `datamart_dag.py` e cada tenant é um
`criar_dag("<tenant>")`. O `dag_id` continua literal, que é o que o Airflow indexa.

**`max_active_tis_per_dag=1` fica.** A v3.0 o removeu junto com o `expand_kwargs` por filial. Mas as
tasks continuam mapeadas — sobre `tables[]`, não sobre filiais — e duas tabelas no config subiriam
dois drivers ao mesmo tempo. Remover uma guarda porque hoje ela não é exercida é como o nó vai ser
derrubado quando alguém acrescentar a segunda tabela.

### O que a DAG precisou trazer junto

| Peça | Por quê |
|---|---|
| `spark-<tenant>-config` / `-secret` | O `spark-secrets` estava fixo no acme. Sem separar, o placeholder `TENANT` do manifesto não teria o que resolver |
| Catálogo ClickHouse na `SparkSessionFactory` | **Lacuna de T2.3**: o teste de integração montava a própria `SparkSession`. Num job real o `writeTo` não resolveria o nome da tabela |
| Delta e S3A no `sparkConf` | O `spark-defaults.conf` da imagem é sombreado pelo Spark-on-K8s — [incidente #13](../TROUBLESHOOTING.md#13-spark-defaultsconf-da-imagem-não-chega-ao-driver-sob-o-operator) |
| `manifests/` no ConfigMap das DAGs | `--from-file` de diretório ignora subpasta; o initContainer recoloca o template onde a DAG o procura |

### Aceite

```bash
kubectl -n airflow exec airflow-scheduler-0 -c scheduler -- \
  airflow dags trigger k8s_acme_datamart
```

Sem `-e`, a janela é o mês do instante do trigger. Para carregar outro mês, passe um `-e` dentro dele,
posterior ao `start_date` ([incidente #14](../TROUBLESHOOTING.md#14-dagrun-verde-sem-executar-nenhuma-task))
e já passado ([incidente #16](../TROUBLESHOOTING.md#16-dagrun-com-logical_date-futura-fica-queued-até-a-data-chegar)).

1. As duas tasks concluem `success`;
2. Postgres e ClickHouse têm a **mesma contagem** na partição carregada;
3. `airflow dags list-import-errors` continua vazio.

**Fechado em 2026-09-11**, sobre o snapshot v3321 de `dw_andon_peso`:

| Item | Resultado |
|---|---|
| (1) | Run `manual__2026-09-11T12:33:58`, 5 tasks `success`. `carga_postgres` 7,5 min, `carga_clickhouse` 8,7 min |
| (2) | **1 039 682** linhas nos dois lados, iguais filial a filial; `count(distinct hk_business_id)` idem |
| (3) | `No data found` |

A janela foi `2026-09-01 00:00:00` → `2026-10-01 00:00:00`, e o ClickHouse ficou com uma partição ativa,
`202609`. Detalhe em [TESTES §5.8](../TESTES.md#58-t26--aceite-sobre-o-snapshot-fixado).

A primeira tentativa, em 2026-09-10, chegou a 15 stages e morreu em `SparkFileNotFoundException`: a
cópia da silver estava sem arquivos do snapshot. Era dado, não código
([incidente #15](../TROUBLESHOOTING.md#15-sparkfilenotfoundexception-na-silver)). O CR renderizado
naquela execução já provava a cadeia da DAG até a borda do dado.

### Riscos {#riscos-4}

| Risco | Sinal | Mitigação |
|---|---|---|
| Silver alterada durante o DagRun | Contagens divergentes entre os braços, irreprodutível | Premissa 3. A silver do cluster é um snapshot fixado (v3321), que nada reescreve. Se for recopiada, fazer com a DAG pausada; o aceite (2) acusa |
| Os dois drivers Spark simultâneos | `OOMKilled` no nó, ou pod `Pending` sem recurso | Tasks em sequência, e `max_active_tis_per_dag=1` dentro de cada uma |
| Trigger com `logical_date` anterior ao `start_date` | DagRun **verde** com zero task instance | `start_date` em 2025-01-01, antes do dado mais antigo da silver — [incidente #14](../TROUBLESHOOTING.md#14-dagrun-verde-sem-executar-nenhuma-task) |
| SA errada no RoleBinding | `serviceaccount:airflow:airflow-worker cannot create sparkapplications` | RoleBinding para `airflow-scheduler` (E1 T1.7) |

> **`trigger_rule="all_done"` na segunda task foi descartado.** A v3.0 o propunha para que uma falha no
> braço PG não deixasse o CH sem carga. O efeito é o oposto do desejado: com o PG falho nada foi
> carregado nele, e rodar o CH assim mesmo produz **um** datamart carregado — exatamente o estado
> inconsistente que a regra queria evitar. Com `all_success`, uma falha em qualquer braço para a cadeia,
> e o par continua comparável.

---

## Checklist Go/No-Go do épico

Verificado em 2026-09-11. Cada item aponta para onde a saída está registrada.

- [x] `pytest -q` da suíte unitária passa após o rsync, sem alteração — 173 passed, igual à tag pura (T2.1)
- [x] As 4 tabelas silver estão em Delta sob `business_datavault_data-bee/`, com os nomes `dw_` do contrato (T2.5)
- [x] As 4 têm `id`, `unidade_origem` e `dataset_origem` — T2.5, e a junção completou sobre o dado real (T2.6)
- [x] A query aborta quando `JANELA_INICIO`/`JANELA_FIM` não são informados (T2.2)
- [x] Recarregar a mesma janela duas vezes não muda a contagem da partição no ClickHouse — T-E2-02, sobre
      DataFrame sintético, e depois sobre o dado real, nos dois destinos
      ([TESTES §6.2](../TESTES.md#62-recarga-sobre-dado-existente--o-custo-do-merge))
- [x] `count(distinct hk_business_id) == count(*)` nos dois destinos — 1 039 682 = 1 039 682 (T2.6)
- [x] Postgres e ClickHouse têm a **mesma contagem** para a mesma janela — 1 039 682, filial a filial (T2.6)
- [x] `pytest -m integration` do repositório ClickHouse passa, incluindo recarga — 6 passed (T2.4)
- [x] O diff entre os dois repositórios toca exclusivamente `write()` — asserção de identidade (T2.4)
- [x] Um trigger na DAG carrega os dois destinos (T2.6)

## O que este épico NÃO prova

- **Que a ingestão inteira funciona.** O passo bronze → silver ficou fora, e a própria entrada da silver
  é do usuário; a POC começa numa silver que já existe.
- **Que a gold em Delta funciona.** Foi retirada do escopo por decisão de produto. A POC não produz
  evidência a favor nem contra ela.
- **Que os dois braços leram exatamente as mesmas linhas.** Prova que leram o **mesmo recorte declarado**
  e chegaram à mesma contagem. Com a silver parada isso é equivalente; com a silver em movimento, não.
  Na execução que fechou o épico ela estava parada: um snapshot fixado, que nada reescreve.
- **Que a recarga é idempotente sobre o dado real** — no fechamento do épico, só em teste de integração.
  Coberto depois, na preparação do E3 ([TESTES §6.2](../TESTES.md#62-recarga-sobre-dado-existente--o-custo-do-merge)).
- **Desempenho de carga.** A POC mede latência de **leitura** no destino, não velocidade de ingestão.
- **Que o patch é aceitável upstream.** É candidato. Quem decide é a revisão do honeycomb.
