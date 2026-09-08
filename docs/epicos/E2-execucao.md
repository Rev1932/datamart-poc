# Épico 2 — Execução

| Campo | Valor |
|---|---|
| Versão | 1.0 |
| Data | 2026-09-04 |
| Status | Não iniciado |
| Objetivo | Um trigger no Airflow carrega os dois braços a partir do mesmo Delta silver |
| Depende de | [E1](E1-infraestrutura.md) completo |
| Bloqueia | E3 inteiro |
| Progresso | [../TODO.md](../TODO.md) |

---

## Objetivo

Transformar o fork defasado em código idêntico ao produtivo, acrescentar o braço ClickHouse, e orquestrar
os dois braços pelo mesmo Dataset do Airflow.

## Premissas

1. O usuário carrega os Parquet reais; a POC não gera dado sintético.
2. O delta de código sobre o honeycomb é desenhado para virar PR upstream.
3. `read()` e `transform()` são compartilhados entre os braços; só `write()` difere.

## Fora de escopo

Alterar o honeycomb produtivo. O que sai daqui é um patch candidato, não um deploy.

---

## Tasks

| Task | Entrega | Bloqueia |
|---|---|---|
| [T2.1](#t21--re-sync-com-honeycombmain) | Re-sync de `spark-source-code/` com `honeycomb@main` | T2.2, T2.3, T2.4 |
| [T2.2](#t22--janela-de-carga) | Query parametrizada por janela — corrige **D1** | T2.3 |
| [T2.3](#t23--repositório-clickhouse) | `RepositoryGoldDatamartClickhouse` + troca de partição | T3.1 |
| [T2.4](#t24--braço-postgres-e-simetria-experimental) | Braço Postgres com `read`/`transform` compartilhados | T3.1 |
| [T2.5](#t25--carga-bronze) | Carga bronze com validação de layout | T3.0 |
| [T2.6](#t26--dags) | Três DAGs encadeadas por Dataset | — |

T2.3 e T2.4 são paralelizáveis depois de T2.1.

---

## T2.1 — Re-sync com `honeycomb@main`

Decisão e argumento completo em [ADR-001](../decisoes/ADR-001-resync-honeycomb.md).

### Ações

1. `rsync` de `honeycomb@main` sobre `spark-source-code/`, excluindo `.git`, `.venv`, `__pycache__`,
   `.pytest_cache`.
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

### Artefatos

`spark-source-code/` (substituído), `src/main/utils/config_manager.py` (alterado).

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

## T2.2 — Janela de carga

Corrige o defeito **D1** ([ARQUITETURA.md §4](../ARQUITETURA.md#4-defeitos-verificados-no-ambiente)).
Decisão em [ADR-004](../decisoes/ADR-004-janela-de-carga.md).

### O problema, concretamente

`spark-source-code/resources/queries/fact_200_cep.sql:44`:

```sql
WHERE to_timestamp(substring(dap.data_hora, 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS')
      >= current_timestamp() - INTERVAL 10 DAYS
```

Com `PARTITION BY toYYYYMM(timestamp)`, trocar a partição `202609` com um DataFrame que contém só os dias
10–20 **apaga os dias 1–9 de setembro**. Sem erro, sem aviso. A contagem da partição simplesmente cai.

### Ações

1. `src/main/utils/queryutils.py`, função `build_query`: acrescentar os placeholders `JANELA_INICIO` e
   `JANELA_FIM` à substituição que já existe para `MINIO_BASE_PATH` e `S3_PATH_SILVER`. São ~6 linhas, e é
   uma mudança digna de upstream.
2. `resources/queries/fact_200_cep.sql`: substituir o predicado por
   ```sql
   WHERE ts >= 'JANELA_INICIO' AND ts < 'JANELA_FIM'
   ```
   com os limites arredondados para o primeiro dia do mês.
3. Guarda executável antes da troca, no repositório:
   - `min(df.timestamp) >= JANELA_INICIO`
   - `JANELA_INICIO == toStartOfMonth(JANELA_INICIO)`

### Artefatos

`src/main/utils/queryutils.py` (alterado), `resources/queries/fact_200_cep.sql` (alterado).

### Aceite

Carregar duas vezes a mesma janela e a contagem da partição **não muda**:
```bash
ch --query "SELECT count() FROM dm_acme.fact_200_cep WHERE toYYYYMM(timestamp)=202609"
# roda a carga de novo
ch --query "SELECT count() FROM dm_acme.fact_200_cep WHERE toYYYYMM(timestamp)=202609"
```
Os dois números são iguais.

---

## T2.3 — Repositório ClickHouse

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
`LowCardinality(String)` e `Decimal(18,4)` exatos.

### Ações

1. `src/main/utils/clickhouse_datamart.py` — cliente HTTP para DDL e troca de partição.
2. `RepositoryGoldDatamartClickhouse` em `src/main/repo/repository.py`, com esta sequência de escrita:
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
3. `PipelineGoldDatamartClickhouse` em `core/pipeline_orchestrator.py`.
4. Chave `gold_datamart_clickhouse` em `core/pipeline_factory.py`.
5. DDL em `ddl/clickhouse/01_fact_200_cep.sql` conforme
   [ARQUITETURA.md §3.2](../ARQUITETURA.md#32-ddl-alvo).
6. Teste de integração com testcontainers, espelhando
   `tests/integration/test_repository_gold_datamart.py`, **incluindo caso de recarga da mesma partição**.

### Notas sobre cada passo

| Passo | Armadilha |
|---|---|
| 3 | Uma staging **por execução**, não por partição. `CREATE TABLE AS` copia estrutura, engine, `ORDER BY`, `PARTITION BY` e política de armazenamento — os três requisitos do `REPLACE PARTITION`. O sufixo `uuid4` evita colisão entre filiais concorrentes |
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
`tests/integration/test_repository_gold_datamart_clickhouse.py`.

### Aceite

```bash
pytest -m integration tests/integration/test_repository_gold_datamart_clickhouse.py -q
```
`passed`, incluindo o caso de recarga da mesma partição.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| **D1**: incremento parcial | `count()` da partição **cai** entre execuções, sem erro | Guarda do passo 2 + query por janela (T2.2) |
| `NULL` em coluna de particionamento | `Code 349 CANNOT_CONVERT_TO_NULLABLE` no meio da carga | Limpeza no `transform` compartilhado (T2.4) |
| Staging concorrente entre filiais | `Table ..._stg already exists`, ou troca com dado da outra filial | Sufixo `uuid4().hex[:8]` |
| `Code 252 TOO_MANY_PARTS` | INSERT rejeitado após alguns lotes | `repartition` no passo 4 |
| Política de armazenamento divergente | `Tables have different storage policies` | `CREATE TABLE AS` garante hoje; quebra se um dia houver tiering só no destino |

---

## T2.4 — Braço Postgres e simetria experimental

### Ações

1. Extrair `read()` e `transform()` de `RepositoryGoldDatamart` para uma classe base comum, herdada pelos
   dois braços. **Só o `write()` difere.**
2. No `transform` compartilhado, acrescentar a limpeza:
   ```python
   .na.drop(subset=["timestamp", "filial", "banco", "unidade_producao_id"])
   ```
   logando a contagem descartada.
3. Trocar `CAST(... AS DOUBLE)` por `DECIMAL(18,4)` na query — **nos dois braços**, senão o
   `get_type_sql` do Postgres mapeia para `DOUBLE PRECISION` e a comparação numérica desalinha.

### Por que a limpeza precisa ser compartilhada

`timestamp` não pode ser `Nullable` sob `PARTITION BY`, mas
`to_timestamp(substring(dap.data_hora, 1, 23), ...)` devolve `NULL` em string malformada, e
`dop.produto_id`/`dup.id` vêm de `INNER JOIN` mas passam se a silver tiver a coluna nula.

> Se só o braço ClickHouse limpar, as contagens divergem e a tabela de corretude (T3.1) acusa uma diferença
> que **não é do motor** — o benchmark perde credibilidade por um bug nosso. A simetria é o que permite a
> afirmação mais forte da apresentação: *"mesma leitura, mesma transformação, mesma máquina — só o destino
> muda"*.

### Artefatos

`src/main/repo/repository.py` (refatorado), `resources/queries/fact_200_cep.sql` (alterado).

### Aceite

```bash
git diff --stat   # o diff entre os dois repositórios toca exclusivamente write()
```
Mais: `pytest -q` continua passando.

---

## T2.5 — Carga bronze

### Contrato de layout

O `bronze_silver` lê Parquet **exatamente** deste caminho:

```
s3a://datamart/data-bee_replication/data-bee_<filial>/<tabela>/*.parquet
```

`<filial>` é o valor que aparece em `filiais[]` no documento Mongo, e `<tabela>` é o `name` de cada entrada
de `tables`. Um caminho errado não produz erro — produz um job que conclui com sucesso e uma silver vazia.

### Ações

1. Substituir o placeholder `scripts/seed-bronze.sh` por `scripts/load-bronze.sh`, com modo `--validate`.
2. `--validate` lê o documento Mongo do tenant, percorre cada combinação filial × tabela, e **falha
   nomeando o caminho ausente**.
3. Modo de carga: `mc mirror` de um diretório local para o prefixo correto, derivando o destino do
   documento Mongo em vez de exigir que o usuário monte o caminho à mão.

### Artefatos

`scripts/load-bronze.sh` (substitui `seed-bronze.sh`).

### Aceite

```bash
bash scripts/load-bronze.sh --validate
```
Lista os Parquet encontrados por filial × tabela, ou falha nomeando o primeiro caminho ausente.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Dado real fora do layout | `bronze_silver` loga "nenhum arquivo" e **conclui com SUCESSO** — falha silenciosa | `--validate` falha antes; e assert de contagem > 0 ao fim do job |

---

## T2.6 — DAGs

Três arquivos, cópias de `dtlk-airflow-pipeline/dags/k8s_unipac_bronze_silver.py` com o mínimo de
alteração possível.

| DAG | Schedule | Pipeline |
|---|---|---|
| `k8s_<tenant>_bronze_silver.py` | do documento Mongo | `bronze_silver` |
| `k8s_<tenant>_gold_datamart_pg.py` | `[Dataset("honeycomb://<tenant>/silver")]` | `gold_datamart` |
| `k8s_<tenant>_gold_datamart_ch.py` | `[Dataset("honeycomb://<tenant>/silver")]` | `gold_datamart_clickhouse` |

**As duas DAGs gold disparam do mesmo Dataset.** O mesmo silver alimenta os dois braços automaticamente —
o rigor experimental sai de graça do padrão produtivo.

### O que preservar literalmente

| Elemento | Por quê |
|---|---|
| `serverSelectionTimeoutMS=5000` no `MongoClient` | Sem isso, o Mongo fora do ar estoura o `dagbag_import_timeout` e a DAG some do DagBag |
| `try/except` de parse time degradando para `schedule=None` | A DAG entra em modo manual em vez de desaparecer |
| `expand_kwargs(montar_execucoes())` | Dynamic task mapping: 1 `SparkApplication` por filial |
| `outlets` na task **não-mapeada** (`publicar_silver`, `trigger_rule="all_done"`) | Na task mapeada, o Dataset dispara o gold **uma vez por filial** |
| `max_active_tis_per_dag=1` | Duas filiais na mesma tabela causam perda de dado no `bronze_silver` (overwrite sem proteção). **Replicá-lo mantém a POC fiel; removê-lo perde dado real do usuário** |
| Manifesto externo com placeholders `TENANT`/`VERSION`, sobrescrevendo **só** `spec.arguments` | É o padrão produtivo |

### Artefatos

`airflow/dags/k8s_<tenant>_{bronze_silver,gold_datamart_pg,gold_datamart_ch}.py`,
`airflow/dags/manifests/spark-honeycomb-{bronze-silver,gold-datamart-pg,gold-datamart-ch}.yaml`.

### Aceite

```bash
airflow dags trigger k8s_acme_bronze_silver
```
1. Uma `SparkApplication` por filial;
2. as duas DAGs gold disparam **sozinhas** pelo Dataset, sem trigger manual;
3. `airflow dags list-import-errors` continua vazio.

### Riscos

| Risco | Sinal | Mitigação |
|---|---|---|
| Dataset dispara o gold N vezes | N DagRuns do gold por rajada | `outlets` na task não-mapeada |
| SA errada no RoleBinding | `serviceaccount:airflow:airflow-worker cannot create sparkapplications` | RoleBinding para `airflow-scheduler` (E1 T1.7) |

---

## Checklist Go/No-Go do épico

- [ ] `pytest -q` da suíte unitária passa após o rsync, sem alteração
- [ ] Recarregar a mesma janela duas vezes não muda a contagem da partição
- [ ] `pytest -m integration` do repositório ClickHouse passa, incluindo recarga
- [ ] O diff entre os dois repositórios toca exclusivamente `write()`
- [ ] `bash scripts/load-bronze.sh --validate` passa com o dado real do usuário
- [ ] Um trigger no `bronze_silver` dispara as duas DAGs gold pelo Dataset
- [ ] Postgres e ClickHouse contêm dado ao fim do encadeamento
