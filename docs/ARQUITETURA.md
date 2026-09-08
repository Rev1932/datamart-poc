# Arquitetura da POC V2

| Campo | Valor |
|---|---|
| Versão | 1.0 |
| Data | 2026-09-04 |
| Status | Aprovada |
| Escopo | Decisões transversais aos três épicos |
| Depende de | [README.md](README.md) para glossário e premissas |

---

## Sumário

1. [Desenho alvo](#1-desenho-alvo)
2. [O que muda em relação à V1](#2-o-que-muda-em-relação-à-v1)
3. [Modelagem no destino — a seção mais importante](#3-modelagem-no-destino--a-seção-mais-importante)
4. [Defeitos verificados no ambiente](#4-defeitos-verificados-no-ambiente)
5. [Orçamento de recursos](#5-orçamento-de-recursos)
6. [Riscos transversais](#6-riscos-transversais)
7. [O que esta POC NÃO prova](#7-o-que-esta-poc-não-prova)

---

## 1. Desenho alvo

```
Usuário carrega Parquet real  →  s3a://datamart/data-bee_replication/data-bee_<filial>/<tabela>/
                                          │
   MongoDB Data_Catalog.k8s_<tenant>  ────┤  control plane: filiais, tables, schedule, versão
                                          ▼
   Airflow (LocalExecutor)  →  DAG k8s_<tenant>_bronze_silver
                                  SparkKubernetesOperator, 1 pod por filial
                                          ▼
                          Delta silver: business_datavault_data-bee/<tabela>
                                          │
                            Dataset("honeycomb://<tenant>/silver")
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    ▼                                           ▼
      DAG ..._gold_datamart_pg                    DAG ..._gold_datamart_ch
      --pipeline gold_datamart                    --pipeline gold_datamart_clickhouse
                    ▼                                           ▼
        Postgres (staging + ON CONFLICT)          ClickHouse dm_<tenant> (REPLACE PARTITION)
                    └─────────────────  benchmark/  ────────────┘
                            mesmas queries, mesmo hardware
```

O ponto de desenho que mais vale: **as duas DAGs gold disparam do mesmo Dataset**. O mesmo Delta silver
alimenta os dois braços automaticamente, sem intervenção manual. O rigor experimental — mesma origem,
mesma máquina, mesmo instante — sai de graça do padrão de orquestração que já é produtivo.

### Simetria experimental

`read()` e `transform()` são **compartilhados** entre os dois braços por herança. **Só o `write()` difere.**
É o que sustenta a afirmação mais forte da apresentação:

> *"Mesma leitura, mesma transformação, mesma máquina, mesmo dado. A única coisa diferente é para onde
> foi escrito."*

Quebrar essa simetria — limpar `NULL` em um braço e não no outro, converter tipo em um e não no outro —
faz as contagens divergirem, e a divergência **não é do motor**: é bug nosso. O portão de corretude (T3.1)
existe exatamente para pegar isso antes de qualquer número de performance ser publicado.

---

## 2. O que muda em relação à V1

| Aspecto | V1 | V2 | Por quê |
|---|---|---|---|
| Orquestração | `scripts/run-*.sh` | Airflow + MongoDB, padrão produtivo | A POC precisa parecer com produção para valer como proposta |
| Multi-tenant | DB única, user único | `dm_<tenant>` + ROLE/USER/QUOTA/PROFILE por tenant | É a pergunta central da gerência |
| Chave de ordenação | `ORDER BY (hk_business_id)` | `ORDER BY (filial, banco, unidade_producao_id, timestamp)` | Ver §3 |
| Partição | `PARTITION BY tuple()` | `PARTITION BY toYYYYMM(timestamp)` | Habilita poda e troca atômica |
| Tipos | Tudo `Nullable` | `LowCardinality` + `Decimal`, `Nullable` só onde preciso | `Nullable` custa uma coluna extra de máscara por coluna |
| Carga | `append` + `OPTIMIZE FINAL` | `REPLACE PARTITION` atômica | Idempotência sem depender de merge assíncrono |
| Benchmark | Stress de merge (escrita) | Latência de leitura, p50/p95, sequencial e concorrente | O problema declarado é leitura |
| Comparação | Só ClickHouse | Postgres **e** ClickHouse | Sem os dois não há número comparável |
| Código Spark | Fork defasado | Re-sync com `honeycomb@main` | Ver [ADR-001](decisoes/ADR-001-resync-honeycomb.md) |

---

## 3. Modelagem no destino — a seção mais importante

### 3.1 O modo de falha dominante

> **A chave de ordenação errada é o erro que mais custa e o único que não dá para corrigir sem recriar a
> tabela e recarregar tudo.**

A V1 usa `ORDER BY (hk_business_id)` — um SHA-256. Um hash tem distribuição uniforme por construção, logo
linhas da mesma filial, do mesmo mês, da mesma unidade de produção ficam espalhadas por todas as
granularidades do disco. O índice esparso do ClickHouse, que é o mecanismo pelo qual ele evita ler dado
irrelevante, deixa de podar qualquer coisa: toda query vira varredura.

O sintoma não é um erro. É um ganho de 2–3× onde deveria haver 10–50×, e alguém concluindo que "ClickHouse
não fez tanta diferença assim".

**A chave correta é derivável do que já existe.** Os índices da gold produtiva
(`journey-dashboards-pipeline/sql/gold/02_gold_indexes.sql`) declaram o padrão de acesso real do
dashboard: `(filial, banco, data_hora)` e `(filial, banco, unidade_producao_codigo)`. A chave de
ordenação segue esse padrão, do mais seletivo e mais usado em igualdade para o mais granular:

```sql
ORDER BY (filial, banco, unidade_producao_id, timestamp)
```

### 3.2 DDL alvo

```sql
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (filial, banco, unidade_producao_id, timestamp)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
```

| Decisão | Motivo |
|---|---|
| `MergeTree`, não `ReplacingMergeTree` | A idempotência vem da troca atômica de partição, não do merge assíncrono. Ver [ADR-004](decisoes/ADR-004-janela-de-carga.md) |
| `PARTITION BY toYYYYMM(timestamp)` | Granularidade mensal: poda por período e habilita `REPLACE PARTITION`. Diária geraria partes demais para o volume da POC |
| `hk_business_id` **fora** do `ORDER BY` | §3.1 |
| `ttl_only_drop_parts = 1` | Retenção descarta partes inteiras em vez de reescrevê-las |
| `index_granularity = 8192` | Default; mudar sem medir é ruído |

### 3.3 Tipos

| Coluna | Tipo | Motivo |
|---|---|---|
| `filial`, `banco`, `unidade_producao_nome`, `atributo_nome_pai`, `atributo_tipo`, `unidade_medida`, `nome_limite_*` | `LowCardinality(String)` | Poucos valores distintos; dicionarização corta disco e acelera `GROUP BY` |
| `valor`, `valor_limite_superior`, `valor_limite_inferior` | `Decimal(18,4)` | A query hoje faz `CAST(... AS DOUBLE)`. Trocar para `Decimal` **nos dois braços**, senão o Postgres mapeia para `DOUBLE PRECISION` e a comparação numérica desalinha |
| `timestamp`, `filial`, `banco`, `unidade_producao_id` | **não** `Nullable` | Exigência do `PARTITION BY` e da chave de ordenação |
| Demais colunas de negócio | `Nullable` só se o dado exigir | Cada coluna `Nullable` carrega uma coluna extra de máscara |

> `timestamp` não pode ser `Nullable`, mas
> `to_timestamp(substring(dap.data_hora, 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS')` devolve `NULL` em string
> malformada. Sem uma limpeza explícita no `transform` **compartilhado**, a carga quebra com
> `Code 349 CANNOT_CONVERT_TO_NULLABLE` no meio do job. A limpeza precisa estar no código comum aos dois
> braços — ver §1, simetria experimental.

`timestamp` é nome legal de coluna nos dois motores, mas colide com o nome do tipo em alguns contextos:
usar crase no ClickHouse e aspas duplas no Postgres nas queries do benchmark.

---

## 4. Defeitos verificados no ambiente

Três defeitos foram confirmados por leitura dos arquivos, não inferidos. Cada um tem uma correção atribuída
a uma task.

### D1 — `REPLACE PARTITION` destruiria dado real

`spark-source-code/resources/queries/fact_200_cep.sql:44`:

```sql
WHERE to_timestamp(substring(dap.data_hora, 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS')
      >= current_timestamp() - INTERVAL 10 DAYS
```

Com `PARTITION BY toYYYYMM(timestamp)`, trocar a partição `202609` com um DataFrame que contém só os dias
10–20 **apaga os dias 1–9**. `REPLACE PARTITION` descarta o conteúdo anterior da partição inteira — é
exatamente isso que o torna idempotente, e exatamente isso que o torna perigoso com incremento parcial.

**Correção:** T2.2 — query parametrizada por janela alinhada a fronteira de partição, mais uma guarda de
cobertura integral que aborta antes da troca.

### D2 — colisão de variáveis de ambiente

`CLICKHOUSE_URL`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SERVICE_NAME` e
`CLICKHOUSE_VERIFY_SSL` já existem no `_ENV_SCHEMA` do honeycomb e pertencem ao `handler_logger` de
auditoria, que em produção aponta para `api-logs.datawake.cloud`. Reusar o prefixo faz o log de auditoria
escrever no datamart, ou o datamart escrever na instância de logs.

**Correção:** T2.1 — prefixo próprio `DATAMART_CH_{URL,USER,PASSWORD,DATABASE,CATALOG}`.

### D3 — o braço Postgres seria um espantalho

`GeneratePostgresQueryUtils` cria a tabela com um único índice, o único da chave primária — que para
`fact_200_cep` é `andon_peso_id`, e **não** é o filtro do dashboard. Toda query do benchmark faria
varredura sequencial.

Comparar ClickHouse contra um Postgres sem índice não é um benchmark, é um espantalho, e é o primeiro
ataque que virá da sala.

**Correção:** T1.4 — índice `(filial, banco, unidade_producao_id, "timestamp")`, `ANALYZE`, e tuning de
memória. Um terceiro braço `pg-tuned` no schema `gold_tuned` — particionado por mês mais BRIN — antecipa o contra-argumento
"então é só arrumar o Postgres" com uma barra medida em vez de uma opinião.

---

## 5. Orçamento de recursos

Dimensionado contra a máquina real, não contra o desejável: **15,5 GiB de RAM e 12 vCPU no WSL2**.

**A POC roda no perfil `small` — 4 vCPU / 8 GiB.** É o único perfil que será exercitado; a decisão é de
limite de infraestrutura local, não de desenho. O `full` fica no script como opção, sem nunca ter sido
testado.

| Perfil | minikube | Teto real (cgroup) | Situação |
|---|---|---|---|
| `small` | `--cpus=4 --memory=8g` | **8,00 GiB / 4 cores** | **o perfil da POC**; verificado em [TESTES.md](TESTES.md) |
| `full` | `--cpus=6 --memory=10g` | 10,0 GiB / 6 cores | disponível, **nunca exercitado** |

O teto é o do **cgroup**, não o que o nó anuncia. O kubelet reporta 15,47 GiB e o scheduler acredita
nisso — ver [D4](TESTES.md#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup). Toda conta abaixo é
feita contra os 8,00 GiB reais.

### Teto por serviço

Contrato de dimensionamento para T1.4, T1.6 e T1.7. Não são sugestões: são o que faz a soma fechar.

| Serviço | requests (mem / cpu) | limits (mem / cpu) | Trava adicional |
|---|---|---|---|
| MinIO | 512Mi / 250m | 1Gi / 1 | valor já em uso e testado; não foi reduzido |
| ClickHouse | 1Gi / 500m | 3Gi / 3 | `max_server_memory_usage` 2 GiB; `mark_cache_size` 384 MiB; system logs desligados |
| PostgreSQL (braço) | 640Mi / 300m | 1792Mi / 2 | `shared_buffers` 384MB, `effective_cache_size` 1GB |
| MongoDB | 256Mi / 100m | 640Mi / 500m | `--wiredTigerCacheSizeGB 0.25` fixo — é o **mínimo** aceito pelo WiredTiger |
| Airflow scheduler | 640Mi / 300m | 1280Mi / 2 | LocalExecutor: as tasks rodam neste pod. É **StatefulSet**, não Deployment |
| Airflow webserver | 320Mi / 100m | 768Mi / 1 | — |
| Airflow metadata PG | 192Mi / 100m | 384Mi / 500m | subchart Bitnami desligado; StatefulSet próprio |
| spark-operator | 128Mi / 100m | 256Mi / 500m | — |
| clickhouse-operator | 128Mi / 100m | 256Mi / 500m | — |
| **soma dos permanentes** | **3,75 GiB / 1850m** | 9,25 GiB / 11,5 | — |
| kube-system | ~0,4 GiB / 600m | — | coredns, provisioner, metrics-server |

Spark é transitório e só existe durante a carga:

| Pod | requests |
|---|---|
| driver | 1g + 384m de overhead = 1408Mi / 1 core |
| executor ×1 | 1g + 384m de overhead = 1408Mi / 1 core |

O executor caiu de 1536m para 1g ao fixar o perfil `small`. Um executor apertado **derrama para disco** e
demora mais; ele não quebra. E a POC mede latência de leitura no ClickHouse, não velocidade de carga — o
custo cai onde não afeta a tese.

> **A soma dos `limits` (9,25 GiB) passa do teto do cgroup de propósito.** Limite não é reserva: serve
> para matar quem fugir do envelope, não para alocar. O que precisa caber é a soma dos **requests**.

### Os dois picos

Ingestão e benchmark nunca acontecem juntos — é o que torna 8 GiB suficiente.

| Fase | O que está de pé | requests | Folga sobre 8,00 GiB |
|---|---|---|---|
| Ingestão | permanentes + Spark | **6,9 GiB / 4450m** | 1,1 GiB |
| Benchmark | permanentes − Airflow − MongoDB, Spark parado | **3,0 GiB / 1950m** | 5,0 GiB |

O teto do ClickHouse subiu de 2304Mi para 3Gi depois que os system logs do próprio servidor o
derrubaram por OOM — ver [D12](TESTES.md#d12--os-system-logs-do-clickhouse-derrubam-o-servidor). Com os
logs desligados o uso caiu de 855Mi para 354Mi; a folga fica para os merges do benchmark.

O teto do MongoDB subiu de 384Mi para 640Mi ao ser construído: `wiredTigerCacheSizeGB` tem **mínimo de
0,25 GB**, e 256 MiB de cache mais o heap do mongod não cabem em 384Mi. O número anterior era estimativa
minha, não medição.

A CPU do pico de ingestão (4450m) passa dos 4 cores do cgroup. É deliberado: CPU é recurso **compressível**
— o overcommit atrasa a carga, não a quebra. Memória não é compressível, e por isso a folga de 1,2 GiB
existe. É a mesma razão de todo corte recair sobre CPU.

> Com [D4](TESTES.md#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup) em jogo, **não há rede de
> proteção**: o kubelet não faz eviction porque acredita ter 15,47 GiB. Se a soma real passar de 8 GiB,
> quem age é o OOM killer do kernel e o alvo é o container do nó inteiro — o cluster some. A folga de
> 1,2 GiB e o `max_server_memory_usage` do ClickHouse são a proteção real.

O que `small` **não** sacrifica: RBAC multi-tenant, `REPLACE PARTITION`, a razão de latência de leitura e a
razão de compressão em disco. Ou seja, todas as afirmações arquiteturais da apresentação continuam de pé.

### Regra dura de metodologia

Durante o benchmark, Airflow e MongoDB ficam escalados a zero e nenhuma `SparkApplication` pode estar em
estado não-terminal. Sem isso o p95 mede o scheduler do Airflow, não o banco. Isso é imposto por
`scripts/profile.sh quiesce`, que **falha** se encontrar um job ativo.

### WSL2

- `--memory` maior que a RAM do WSL2 não é recusado: o `minikube start` **avisa e continua**, e a falha
  aparece depois como pods `OOMKilled` e nó `NotReady`. Por isso a pré-checagem de T1.1 aborta.
- **O nó anuncia a capacidade do host, não a do cgroup.** Medido: cgroup de 8 GiB, `kubectl get node` e
  `kubectl top` reportando 15,47 GiB. O scheduler admite o dobro do que o kernel permite, e o estouro
  não vira eviction de pod — vira OOM do container inteiro do nó. Detalhe em
  [TESTES.md](TESTES.md#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup).
- `--disk-size` é praticamente ignorado no driver `docker`: a restrição real é o espaço livre no vhdx.
  Soma de claims: 76 Gi, mais ~6 GiB de imagens. O vhdx cresce mas **não encolhe**.

---

## 6. Riscos transversais

| Risco | Sinal observável | Mitigação |
|---|---|---|
| Dado real fora do layout esperado | `bronze_silver` loga "nenhum arquivo" e **conclui com SUCESSO** | `load-bronze.sh --validate` falha antes, nomeando o caminho |
| Duas filiais na mesma tabela ao mesmo tempo | Contagem na silver menor que a soma das bronzes, **sem erro** | `max_active_tis_per_dag=1`, como em produção |
| Connector ClickHouse-Spark incompatível (incidente #11, LZ4) | `IllegalArgumentException: Magic is not correct` no driver | Jars pinados; fallback `option.compress=false`; fallback final por Parquet + `s3()` |
| Ruído de fundo contamina o p95 | Desvio > 30% entre rodadas idênticas | `profile.sh quiesce` obrigatório |
| Contagens divergem entre braços | `delta ≠ 0` no portão T3.1 | Causa mais provável: limpeza aplicada em só um braço |

Riscos específicos de cada task estão no épico correspondente.

---

## 7. O que esta POC NÃO prova

Esta seção existe para ser lida antes da apresentação. Um benchmark que exagera é exatamente o que a
gerência vai atacar, e declarar a limitação vale mais que o número.

- **Isolamento de recursos entre tenants.** A pesquisa base (§10.1) classifica database-por-tenant no mesmo
  cluster como *sem* isolamento de recursos, e recomenda instância dedicada por tenant. A demo de vizinho
  barulhento (T3.3) mostra a degradação e depois o controle por `QUOTA` — o que prova que **o mecanismo de
  controle existe e funciona**, não que há isolamento forte. A apresentação não pode afirmar o segundo.
- **Comportamento em volume produtivo.** minikube, uma réplica, amostra de dado. As conclusões são de
  **razão entre motores**, não de latência absoluta.
- **I/O físico frio.** "Cold" aqui significa caches do motor derrubados. O page cache do sistema
  operacional não é derrubável neste ambiente — o driver `docker` compartilha o page cache com o host.
- **Que o framework de dashboard vai projetar colunas.** A query q05 do benchmark **mede** o custo de não
  projetar (`SELECT *`), mas alterar o framework está fora do domínio de desenvolvimento do usuário.
- **Durabilidade, alta disponibilidade ou recuperação.** Uma réplica, sem Keeper, sem backup.
