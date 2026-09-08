# Tuning — o que foi ajustado, por quê, e como conferir

| Campo | Valor |
|---|---|
| Versão | 1.0 |
| Data | 2026-09-08 |
| Status | Vigente. Todos os valores estão aplicados e verificados no cluster |
| Escopo | Todo ajuste feito na POC V2: PostgreSQL (dois braços), ClickHouse, MongoDB, Spark, Airflow e o nó |
| Fora de escopo | Tuning de produção. Os números aqui são dimensionados para 4 vCPU / 8 GiB |
| Glossário | Termos gerais em [README.md](README.md#2-glossário); os específicos de tuning estão na §3 |

---

## Sumário

1. [Como usar](#1-como-usar)
2. [O modo de falha dominante](#2-o-modo-de-falha-dominante)
3. [Glossário de tuning](#3-glossário-de-tuning)
4. [PostgreSQL — servidor](#4-postgresql--servidor)
5. [PostgreSQL — braço `pg`](#5-postgresql--braço-pg)
6. [PostgreSQL — braço `pg-tuned`](#6-postgresql--braço-pg-tuned)
7. [ClickHouse — servidor](#7-clickhouse--servidor)
8. [ClickHouse — por tenant](#8-clickhouse--por-tenant)
9. [MongoDB, Spark e Airflow](#9-mongodb-spark-e-airflow)
10. [O que NÃO foi ajustado, e por quê](#10-o-que-não-foi-ajustado-e-por-quê)
11. [Checklist de conferência](#11-checklist-de-conferência)

---

## 1. Como usar

| Papel | O que ler |
|---|---|
| Quem for defender o número na reunião | §2, §5 e §6 — é lá que está a resposta para *"então é só arrumar o Postgres?"* |
| Quem for reproduzir o ambiente | §11, o checklist. Cada linha tem o comando |
| Quem for mexer em algum valor | A seção do serviço, coluna **"o que quebra sem isso"** |
| Quem for levar algo disto a produção | §10 primeiro. Boa parte destes números só faz sentido em 8 GiB |

**A seção mais importante é a [§2](#2-o-modo-de-falha-dominante).** Ela nomeia o que deu errado em quase
todos os incidentes desta POC, e o padrão se repete em serviços que não têm nada a ver entre si.

---

## 2. O modo de falha dominante

**Um ajuste que não tem efeito e não avisa.**

Dos treze defeitos registrados em [TESTES.md](TESTES.md#7-defeitos-encontrados), cinco são a mesma coisa: a
configuração foi declarada, aceita sem erro, e simplesmente ignorada. O serviço sobe verde. A conta só
aparece depois, num número errado ou num OOM.

| Onde | O que foi declarado | O que aconteceu de fato |
|---|---|---|
| [D5](TESTES.md#d5--podtemplate-declarado-mas-nunca-aplicado) | `podTemplates` no CR do ClickHouse | Ignorado inteiro: subiu `latest` **sem limite de memória**. A V1 rodou o benchmark assim |
| [D10](TESTES.md#d10--docker-entrypoint-initdbd-é-pulado-em-silêncio-num-pvc-reusado) | Script em `/docker-entrypoint-initdb.d` | Pulado sem log num PVC reusado; o cluster subiu sem os databases |
| [D11](TESTES.md#d11--rbac-por-database-não-basta-para-o-connector-spark) | RBAC por database | Suficiente para o `clickhouse-client`, insuficiente para o connector. O aceite passou com o RBAC quebrado |
| [D12](TESTES.md#d12--os-system-logs-do-clickhouse-derrubam-o-servidor) | Nada — o **default** | 7,9 M de linhas de instrumentação em 4h ociosas, e OOM |
| BRIN sem `ORDER BY` | Índice BRIN | Existe, é consultado, e não descarta bloco nenhum |

> A conclusão prática que atravessa este documento: **declarar não é aplicar**. Cada ajuste abaixo tem uma
> coluna de verificação porque, sem ela, metade deles poderia estar inerte agora mesmo e ninguém saberia.

---

## 3. Glossário de tuning

| Termo | Significado |
|---|---|
| **BRIN** | Block Range Index — índice que guarda o min/max de cada faixa de blocos. Minúsculo, e só funciona se a ordem física da tabela correlacionar com a coluna |
| **btree** | O índice padrão do Postgres: árvore balanceada, aponta linha a linha. Preciso e caro |
| **Partition pruning** | O planner descartar partições inteiras antes de ler qualquer bloco, olhando só o predicado |
| **`shared_buffers`** | Cache de páginas do próprio Postgres, dentro do processo |
| **`effective_cache_size`** | Estimativa do cache **total** (Postgres + sistema operacional). Não aloca nada; só informa o planner |
| **mark cache** | Cache de índice esparso do ClickHouse. Ele guarda as marcas que apontam para blocos de granularidade |
| **Sparse index** | O índice primário do ClickHouse: uma entrada a cada `index_granularity` linhas, não uma por linha |
| **`memoryOverhead`** | Memória fora da JVM que o Spark reserva por pod: buffers de rede, PySpark, off-heap |
| **WiredTiger** | Motor de armazenamento do MongoDB. O cache dele é a maior fatia da memória do processo |
| **cgroup** | Control group — o mecanismo do kernel que impõe o limite de memória do container |

---

## 4. PostgreSQL — servidor

Aplicados por `-c chave=valor` no `infra/postgres/postgres-statefulset.yaml`. Container com limite de
**1792Mi**.

| Setting | Valor | Default | Por quê | O que quebra sem isso |
|---|---|---|---|---|
| `shared_buffers` | **384MB** | 128MB | ~21% do limite do container. A regra usual é 25% da RAM; aqui o teto é o cgroup, não a RAM do nó | Cache pequeno demais: toda leitura repetida volta ao disco e o braço `pg` fica artificialmente lento |
| `effective_cache_size` | **1GB** | 4GB | Diz ao planner quanto cache existe **no total**. Com 4GB declarados num container de 1,75Gi, o planner superestima e prefere index scan onde seq scan seria melhor | Escolha de plano enviesada — e num benchmark isso é pior que lentidão, é medida errada |
| `work_mem` | **24MB** | 4MB | Memória por operação de sort/hash. As queries do painel fazem `GROUP BY`; com 4MB o agrupamento derrama para disco | `external merge Disk` no `EXPLAIN`, e a latência medida vira ruído de I/O temporário |
| `random_page_cost` | **1.1** | 4.0 | O default assume disco rotacional, onde acesso aleatório custa 4× o sequencial. Aqui é SSD | O planner evita index scan e escolhe seq scan. **Este é o setting que mais distorce comparação de datamart** |
| `max_connections` | **40** | 100 | Cada conexão reserva `work_mem` no pior caso. 100 × 24MB = 2,4 GiB, acima do limite do container | OOM sob concorrência, justamente no teste de 8 clientes simultâneos |
| `track_io_timing` | **on** | off | Habilita a contagem de tempo de I/O no `EXPLAIN (ANALYZE, BUFFERS)` | Não há como separar tempo de I/O de tempo de CPU, e o E3 perde metade da explicação |
| `shared_preload_libraries` | **pg_stat_statements** | vazio | Acumula estatística por query normalizada, entre execuções | Só dá para medir uma query por vez, na mão |

### Por que argumento e não arquivo

A especificação original mandava montar um `postgresql.conf` por ConfigMap com `include_dir`. Isso derrubou
o servidor e **envenenou o volume** — ver [D7](TESTES.md#d7--argumento-inválido-no-primeiro-boot-envenena-o-pgdata).
`include_dir` só é aceito **dentro** do arquivo de configuração, nunca por `-c`. E um `config_file` próprio
obrigaria a replicar `hba_file` e `ident_file`.

Pré-voo que valida os argumentos sem subir o servidor:

```bash
postgres -C shared_buffers -c shared_buffers=384MB -c random_page_cost=1.1 ...
```

---

## 5. PostgreSQL — braço `pg`

Este braço representa **o datamart como ele é hoje em produção**. O único ajuste é o índice que faltava.

```sql
CREATE INDEX IF NOT EXISTS ix_fact_200_cep_dash
    ON public.fact_200_cep (filial, banco, unidade_producao_id, "timestamp");
ANALYZE public.fact_200_cep;
```

### Por que exatamente essas colunas, nessa ordem

O painel filtra por `filial`, `banco` e `unidade_producao_id`, e recorta uma janela de `timestamp`. Num
índice btree composto, **a ordem das colunas decide o que ele consegue podar**: as colunas de igualdade vêm
primeiro, e a de intervalo por último. Invertendo — `timestamp` na frente — o índice só serviria para a
janela, e os três filtros de igualdade seriam resolvidos por filtro linha a linha.

`ANALYZE` logo depois não é cerimônia: sem estatística atualizada o planner estima a seletividade errado e
pode ignorar o índice recém-criado.

### O defeito que isso corrige (D3)

O honeycomb cria a tabela com **um** índice, o da chave primária — `andon_peso_id`, que não é filtro de
nada no painel. Medido sobre 300 mil linhas:

| | Plano | `Buffers` | Execução |
|---|---|---|---|
| Só com a PK, como hoje | `Parallel Seq Scan` | 3207 | — |
| Com `ix_fact_200_cep_dash` | `Index Scan` | **607** | 6,358 ms |

Sem esse índice o braço Postgres seria um espantalho, e o ganho do ClickHouse não sobreviveria à primeira
pergunta técnica.

---

## 6. PostgreSQL — braço `pg-tuned`

Schema `gold_tuned`. Este braço existe para responder **com número** à pergunta *"então é só arrumar o
Postgres?"*.

### Os quatro ajustes

**1. `PARTITION BY RANGE ("timestamp")`, uma partição por mês.**

O painel sempre recorta uma janela de tempo. Com particionamento mensal, o planner descarta as partições
fora da janela **antes de ler qualquer bloco** — não é filtro, é ausência de leitura. É de longe o ajuste
de maior efeito aqui.

**2. O mesmo índice do painel, repetido.**

```sql
CREATE INDEX ix_tuned_dash
    ON gold_tuned.fact_200_cep (filial, banco, unidade_producao_id, "timestamp");
```

Sem ele o braço tunado **perderia** para o braço simples: teria pruning, mas dentro da partição faria scan.
Seria o defeito D3 ao contrário, e a comparação viraria propaganda.

**3. BRIN sobre `timestamp`, `pages_per_range = 32`.**

```sql
CREATE INDEX ix_tuned_brin
    ON gold_tuned.fact_200_cep USING brin ("timestamp") WITH (pages_per_range = 32);
```

O BRIN guarda min/max por faixa de blocos. Com o default de 128 páginas por faixa, cada entrada cobre 1 MB
e a poda fica grossa; 32 dá quatro vezes mais resolução ainda ocupando quase nada. Medido: **240 kB** contra
2848 kB dos btree.

Ele não é escolhido na query do painel — ali o btree ganha. Rende na janela larga sem filtro seletivo, que
é o território da q05.

**4. `INSERT ... ORDER BY "timestamp"`.**

Este é o ajuste que quase ninguém escreve, e sem ele o item 3 é decoração. BRIN só descarta blocos se a
ordem **física** da tabela correlacionar com a coluna indexada. Um `INSERT SELECT` sem `ORDER BY` grava na
ordem de leitura da origem; o índice é criado, é consultado pelo planner, e não elimina bloco nenhum.

### Sem `PRIMARY KEY`, de propósito

Uma tabela particionada exige que a PK contenha a chave de partição — seria `(andon_peso_id, timestamp)`,
diferente da do braço `pg`. Como esta tabela é alvo de leitura e nunca de escrita concorrente, a PK só
custaria espaço e tempo de carga.

### Resultado medido

| Query | Braço | Plano | `Buffers` | Execução |
|---|---|---|---|---|
| Painel (3 igualdades + janela) | `pg` | `Index Scan` | 607 | 6,358 ms |
| | `pg-tuned` | pruning + `Bitmap Heap Scan` | **42** | **0,691 ms** |
| Janela larga, sem filtro seletivo | `pg` | `Parallel Seq Scan` | 3093 | — |
| | `pg-tuned` | `Bitmap Heap Scan` via BRIN | **136** | — |

**14× e 22× menos I/O.** O `Rows Removed by Index Recheck: 4016` no segundo plano é a assinatura do BRIN:
o bitmap é lossy por construção e o recheck descarta o excedente.

### O custo, que também é resultado

`Planning Time` subiu de 1,486 ms para 3,429 ms — avaliar dez partições custa. Com uma janela de dado real
maior, esse custo cresce com o número de meses. É o principal ponto a observar quando o E3 rodar.

---

## 7. ClickHouse — servidor

Container com limite de **3Gi**, em `infra/clickhouse/chi-datamart.yaml`.

| Setting | Valor | Default | Por quê | O que quebra sem isso |
|---|---|---|---|---|
| `max_server_memory_usage` | **2 GiB** | 90% da RAM **vista pelo processo** | O default olha a RAM do nó, não o cgroup. Num container de 3Gi ele autorizaria ~14 GiB | O kernel mata o container antes de o ClickHouse achar que passou do limite |
| `mark_cache_size` | **384 MiB** | 5 GiB | O default sozinho é maior que o container inteiro | O cache cresce até estourar o cgroup; OOM sem query nenhuma |
| `background_pool_size` | **8** | 16 | Com 3 vCPU, 16 threads de merge só disputam CPU | — |
| `background_merges_mutations_concurrency_ratio` | **4** | 2 | O produto dos dois (32) é validado na subida contra `..._optimize_entire_partition` (25) | Abaixo de 25 o servidor **recusa iniciar** com `Code 36 (BAD_ARGUMENTS)` — ver [D6](TESTES.md#d6--background_pool_size--ratio-abaixo-do-mínimo-de-sanidade) |
| `merge_tree/max_bytes_to_merge_at_max_space_in_pool` | **10 GiB** | ~150 GiB | Um merge precisa de espaço livre igual ao seu resultado. 150 GiB não cabem num PVC de 20Gi | `Code 243 (NOT_ENOUGH_SPACE)` no meio da carga |
| `merge_tree/parts_to_delay_insert` | 1000 | 150 | A carga por `REPLACE PARTITION` cria muitas partes de uma vez | `Code 252 (TOO_MANY_PARTS)` interrompendo a ingestão |
| `merge_tree/parts_to_throw_insert` | 3000 | 300 | idem | idem |
| `merge_tree/min_bytes_for_wide_part` | 10 MiB | 10 MiB | Mantido. Abaixo disso a parte é *compact* (um arquivo só); acima, *wide* (um arquivo por coluna) — e só a forma wide dá leitura colunar de verdade | Partes pequenas ficariam compact e a POC mediria I/O de row-store dentro do ClickHouse |
| `optimize_on_insert` (profile) | **0** | 1 | Evita merge síncrono dentro do `INSERT` | A latência de escrita passa a incluir merge, e mede a coisa errada |

### Os system logs, desligados

Este foi o incidente mais caro do épico ([D12](TESTES.md#d12--os-system-logs-do-clickhouse-derrubam-o-servidor)).
Em **quatro horas de cluster ocioso**, sem uma query de negócio:

| Tabela | Linhas acumuladas |
|---|---|
| `system.asynchronous_metric_log` | **7.936.382** |
| `system.trace_log` | 273.282 |
| `system.text_log` | 44.000 |
| `system.metric_log` | 13.797 |
| `system.query_log` | 278 |

O merge da primeira pediu **872 MiB** contra o teto então vigente de 1,5 GiB, e o container foi `OOMKilled`
seis vezes. Desligados por `configuration.files`:

```xml
<asynchronous_metric_log remove="1"/>
<metric_log remove="1"/>
<trace_log remove="1"/>
<text_log remove="1"/>
<error_log remove="1"/>
<processors_profile_log remove="1"/>
```

**`query_log` fica** — o E3 correlaciona `query_id` nele para ler `read_bytes` e `read_rows`, que é o que
*explica* a latência.

Efeito medido: uso do ClickHouse de **855Mi para 354Mi**, queda de 59% sem nenhuma carga.

> Consequência direta para o E3: qualquer medição de memória do ClickHouse feita antes desta correção
> estaria medindo, em boa parte, a instrumentação do próprio ClickHouse.

### O que ainda NÃO foi ajustado aqui

`ORDER BY` e `PARTITION BY` da tabela de fato são a decisão mais importante da POC, e vivem em
[ADR-002](decisoes/ADR-002-modelagem-clickhouse.md). A tabela ainda não existe — ela entra em
[E2](epicos/E2-execucao.md) T2.3.

---

## 8. ClickHouse — por tenant

No `ddl/rbac/10_tenant.sql.tpl`, aplicado ao papel de leitura.

| Setting | Valor | Por quê |
|---|---|---|
| `readonly` | **2** | O valor `1` proíbe alterar settings e quebra clientes que emitem `SET`. O `2` permite `SET` e continua bloqueando escrita |
| `join_use_nulls` | **1** | Sem ele um `LEFT JOIN` preenche o lado ausente com o **default do tipo** — `0` para número, string vazia — em vez de `NULL`. O painel mostra número diferente do Postgres **sem erro nenhum**. É a divergência mais difícil de rastrear depois |
| `max_memory_usage` | **667 MiB** | Teto por query. Com um servidor de 2 GiB, uma query sozinha não pode reservar o orçamento inteiro |
| `max_execution_time` | **30 s** | Corta query patológica antes que ela vire o problema do vizinho |
| `max_result_rows` | 200.000 | Impede que um `SELECT *` sem `LIMIT` traga a tabela pela rede |

### Quota por minuto

```sql
KEYED BY user_name FOR INTERVAL 1 MINUTE
    MAX queries = 120, errors = 20, result_rows = 5000000,
        read_rows = 500000000, execution_time = 60
```

Esta quota é o mecanismo que a demo de vizinho barulhento ([E3](epicos/E3-validacao.md) T3.3) vai exercitar.
Ela é **controle de consumo, não isolamento de recursos** — e o relatório precisa dizer isso com essas
palavras.

### Grants em `system`, e o que ficou de fora

O connector Spark lê seis tabelas de sistema antes de qualquer query
([D11](TESTES.md#d11--rbac-por-database-não-basta-para-o-connector-spark)): `clusters`, `macros`,
`databases`, `tables`, `columns` e `parts`.

**Não** foi concedido `SELECT ON system.*`. `system.query_log` não tem filtro por permissão: um tenant
leria as queries dos outros, furando exatamente o isolamento que a POC quer demonstrar. As quatro últimas
da lista já são filtradas por direito de acesso.

---

## 9. MongoDB, Spark e Airflow

| Serviço | Ajuste | Por quê |
|---|---|---|
| MongoDB | `--wiredTigerCacheSizeGB 0.25` | O default é `max(256MB, 0.5×(RAM−1GB))` e, sem consciência de cgroup, dimensiona contra a RAM do **nó**. O pod morre por OOM sem receber uma query. `0.25` é o **mínimo** aceito pelo WiredTiger — foi o que obrigou o limite do container a subir de 384Mi para 640Mi |
| MongoDB | probe `tcpSocket` | O probe com `mongosh` é Node.js e não sobe no `timeoutSeconds` default de 1 s. O pod nunca fica `Ready`, o Service headless para de publicar endpoint, e o DNS some — ver [D9](TESTES.md#d9--probe-lento-derruba-o-dns-do-service-headless) |
| Spark | `memoryOverhead: 384m` explícito | O default é `max(384Mi, 10% da memory)`. Declarar torna o request do pod previsível, e o orçamento do nó deixa de ser estimativa |
| Spark | executor `1g`, 1 instância | Cabe no envelope de 8 GiB. Um executor apertado **derrama para disco** e demora; não quebra. E a POC mede latência de leitura no ClickHouse, não velocidade de carga |
| Airflow | `LocalExecutor` | Produção usa `CeleryExecutor`. Aqui não há Redis nem workers: as tasks rodam **dentro do pod do scheduler**, o que muda para qual ServiceAccount vai o RoleBinding |
| Airflow | `dagbag_import_timeout: 60` | A DAG lê o Mongo em parse time com `serverSelectionTimeoutMS=5000`. Um valor alto esconderia indisponibilidade real; 60 s dá margem sem mascarar |
| Airflow | DAGs por initContainer + `emptyDir` | Montar a ConfigMap direto em `/opt/airflow/dags` quebra o walker com `Detected recursive loop` — ver [D13](TESTES.md#d13--configmap-montada-em-optairflowdags-quebra-o-walker-de-dags) |
| Nó | 4 vCPU / 8 GiB (`small`) | Ver [ARQUITETURA §5](ARQUITETURA.md#5-orçamento-de-recursos). O teto real é o **cgroup**; o kubelet anuncia 15,47 GiB e o scheduler acredita nisso |

---

## 10. O que NÃO foi ajustado, e por quê

| Não ajustado | Por quê |
|---|---|
| `max_parallel_workers_per_gather` no Postgres | Deixado no default. Mexer nele muda a escolha entre `Seq Scan` e `Parallel Seq Scan` e tornaria a comparação entre braços dependente de um número arbitrário meu |
| `CLUSTER` / `pg_repack` no braço `pg-tuned` | Daria correlação física ainda melhor, mas exige lock exclusivo e reescrita periódica. Levar isso a produção seria uma decisão operacional, não de tuning |
| `use_uncompressed_cache` no ClickHouse | Fica desligado, como no default. Ligado, ele mascararia I/O real e inflaria o ganho medido |
| `index_granularity` diferente de 8192 | O default é o que produção usaria. Mudar aqui otimizaria a POC contra si mesma |
| `SETTINGS` de compressão (ZSTD vs LZ4) | A razão de compressão é **resultado a medir**, não parâmetro a ajustar |
| Tuning do MinIO | É armazenamento de origem, não está no caminho crítico da leitura que a POC mede |

### O que este documento NÃO resolve

- **Não é tuning de produção.** Quase todo número aqui é derivado de um teto de 8 GiB. Em produção, o
  cálculo muda de base.
- **Não cobre a modelagem da tabela de fato no ClickHouse** — `ORDER BY` e `PARTITION BY` são a decisão
  mais cara da POC e vivem em [ADR-002](decisoes/ADR-002-modelagem-clickhouse.md).
- **Não prova latência.** Todos os números medidos aqui vêm de sonda sintética e provam **escolha de plano
  e razão de I/O**. Latência sai do dado real, em [E3](epicos/E3-validacao.md).
- **Não elimina o page cache do sistema operacional** da medição. Isso é uma limitação declarada do
  ambiente, não um ajuste pendente.

---

## 11. Checklist de conferência

Cada linha responde "este ajuste está mesmo aplicado?".

- [ ] `psql -c "SELECT name, setting FROM pg_settings WHERE name IN ('shared_buffers','effective_cache_size','work_mem','random_page_cost','max_connections','track_io_timing')"`
- [ ] `psql -c "\d+ public.fact_200_cep"` mostra `ix_fact_200_cep_dash`
- [ ] `EXPLAIN` da query do painel no braço `pg` **não** mostra `Seq Scan`
- [ ] `psql -c "SELECT count(*) FROM pg_inherits WHERE inhparent='gold_tuned.fact_200_cep'::regclass"` > 1
- [ ] `EXPLAIN` no braço `pg-tuned` mostra o nome de **uma** partição, não da tabela pai
- [ ] `psql -c "SELECT indexdef FROM pg_indexes WHERE schemaname='gold_tuned'"` mostra `USING brin`
- [ ] `clickhouse-client -q "SELECT name, value FROM system.server_settings WHERE name IN ('max_server_memory_usage','mark_cache_size','background_pool_size')"`
- [ ] Duas amostras de `system.parts` separadas por 90 s: só `query_log` cresce
- [ ] `clickhouse-client --user u_<t>_ro -q "SELECT value FROM system.settings WHERE name IN ('readonly','join_use_nulls')"` → `2` e `1`
- [ ] `bash scripts/verify-rbac.sh` → 4/4
- [ ] `kubectl -n datamart get pod mongodb-0 -o jsonpath='{.spec.containers[0].args}'` contém `0.25`
- [ ] `docker stats minikube` — e **não** `kubectl top node`, que mede contra a capacidade do host

---

## Referências

**Postgres — planejamento e índices**
- [Server Configuration: Resource Consumption](https://www.postgresql.org/docs/16/runtime-config-resource.html)
- [Planner Cost Constants](https://www.postgresql.org/docs/16/runtime-config-query.html)
- [BRIN Indexes](https://www.postgresql.org/docs/16/brin.html)
- [Table Partitioning](https://www.postgresql.org/docs/16/ddl-partitioning.html)

**ClickHouse — memória e merges**
- [Server Settings](https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings)
- [MergeTree Settings](https://clickhouse.com/docs/en/operations/settings/merge-tree-settings)
- [System Tables](https://clickhouse.com/docs/en/operations/system-tables)

**Kubernetes e operadores**
- [Altinity ClickHouse Operator](https://github.com/Altinity/clickhouse-operator)
- [Kubeflow Spark Operator — chart](https://github.com/kubeflow/spark-operator/tree/master/charts/spark-operator-chart)

**Dentro deste repositório**
- [ARQUITETURA.md §5](ARQUITETURA.md#5-orçamento-de-recursos) — o orçamento de recursos que restringe tudo
- [TESTES.md §7](TESTES.md#7-defeitos-encontrados) — os treze defeitos, com sintoma e correção
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — incidentes da V1, ainda relevantes

---

*Vive em `docs/TUNING.md`. Incrementar a versão sempre que um valor mudar, e registrar na seção do serviço
o que motivou a mudança. Um valor sem linha de "o que quebra sem isso" não deveria estar aqui.*
