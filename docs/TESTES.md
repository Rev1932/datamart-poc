# TESTES — registro de execução por épico

| Campo | Valor |
|---|---|
| Versão | 1.10 |
| Data da execução | 2026-09-08 (E1) · 2026-09-09 a 2026-09-11 (E2 e início do E3) |
| Branch | `feat/v2-olap` (E1) · `feat/v2-olap-e2-execucao` (E2, E3) |
| Escopo | **Épicos 1 e 2 completos** — T1.1 a T1.8 e T2.1 a T2.6. **E3:** preparação, T3.0 e T3.1 |
| Resultado | E1: **48 testes** · 40 verdes · 7 falharam e passaram após correção · 1 teve o critério substituído. E2 e E3 registram por seção (§5, §6). Defeitos D4 a D18; **1 aberto** (D4) |
| Progresso das tasks | [TODO.md](TODO.md) — este arquivo registra **execução**, não estado |

Este arquivo é o registro de **execução de teste**. Cada rodada é organizada por épico, e dentro do épico
por task. Ele não substitui o [TODO.md](TODO.md): o TODO diz o que está feito, este diz o que foi provado
e como. Uma task marcada ✅ no TODO tem obrigatoriamente uma linha verde aqui.

---

## Sumário

1. [Como usar](#1-como-usar)
2. [Convenções](#2-convenções)
3. [Ambiente da execução](#3-ambiente-da-execução)
4. [Épico 1 — Infraestrutura](#4-épico-1--infraestrutura)
5. [Épico 2 — Execução](#5-épico-2--execução)
6. [Épico 3 — Validação](#6-épico-3--validação)
7. [Defeitos encontrados](#7-defeitos-encontrados)
8. [O que NÃO foi testado](#8-o-que-não-foi-testado)
9. [Artefatos deixados no cluster](#9-artefatos-deixados-no-cluster)
10. [Ações executadas fora da bateria](#10-ações-executadas-fora-da-bateria)
11. [Auditoria de encerramento do Épico 1](#12-auditoria-de-encerramento-do-épico-1)

---

## 1. Como usar

| Papel | O que ler |
|---|---|
| Quem vai continuar a implementação | §7 primeiro — os defeitos abertos mudam decisões de desenho |
| Quem vai revisar a POC | §3 e §8: sem o ambiente e sem o que ficou de fora, nenhum resultado significa nada |
| Quem vai reproduzir | §4, na ordem: os comandos estão completos e são idempotentes |
| Gerência | §7 e §8. Os testes verdes são pré-requisito, não argumento |

**A seção mais importante é a [§7](#7-defeitos-encontrados).** Os testes que passaram confirmam o esperado; os
que falharam mudaram o desenho. O defeito [D4](#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup) em
particular invalida um critério de aceite que estava escrito e afeta todo o orçamento de recursos.

---

## 2. Convenções

| Estado | Significado |
|---|---|
| ✅ | passou; a saída bateu com o esperado |
| ❌ | falhou na primeira execução |
| 🔧 | falhou, foi corrigido, e passou na reexecução — o defeito fica registrado em §7 |
| ⬜ | não executável ainda; a dependência está nomeada |

ID do teste: `T-<épico>-<n>`. Testes **estáticos** (`ST-*`) não precisam de cluster e rodam em segundos;
testes **de integração** exigem o cluster de pé.

Um teste só conta como passado se a **saída** foi conferida. "O comando não deu erro" não é resultado.

---

## 3. Ambiente da execução

| Componente | Versão / valor |
|---|---|
| Host | WSL2 sobre Windows, Ubuntu 24.04 |
| RAM do WSL2 | 15,5 GiB (`MemTotal` 16221360 KiB) |
| vCPU | 12 |
| Disco livre em `/var/lib/docker` | 832 GiB |
| minikube | v1.36.0, driver `docker` |
| Kubernetes | v1.33.1 |
| kubectl | v1.33.3 |
| Helm | v3.18.4 |
| Perfil usado | `small` (4 vCPU / 8 GiB) |
| ClickHouse | `clickhouse/clickhouse-server:24.8` (após correção — ver [D5](#d5--podtemplate-declarado-mas-nunca-aplicado)) |
| Operador ClickHouse | Altinity `release-0.24.0` |
| MinIO | `quay.io/minio/mc:RELEASE.2024-06-12T14-34-03Z` (provisionamento) |

### O perfil é `small`, por decisão

`small` (4 vCPU / 8 GiB) é o perfil da POC — decisão do dono da máquina, por limite de infraestrutura
local. `full` fica no script como opção e **está fora do escopo de teste**; não é dívida.

Conveniência: o cluster `minikube` já existia, parado, criado há 47 dias com exatamente 4 vCPU / 8192 MB.
Não foi preciso recriar nada para testar o perfil escolhido.

### Estado prévio do cluster

Havia um namespace `dw-dados` com um Spark Operator de outro projeto, com 47 dias. Nenhum recurso da POC
existia: sem namespace `datamart`, sem MinIO, sem ClickHouse. Todos os `kubectl apply` desta rodada
**criaram** recursos; nenhum substituiu nada de terceiros.

O Spark Operator preexistente em `dw-dados` (release Helm `spark-operator` 2.5.0, de estudo) foi
declarado obsoleto e sai em favor do da POC. Não havia nenhuma `SparkApplication` no cluster, então a
remoção não descarta carga. **Executada** — ver [§10](#10-ações-executadas-fora-da-bateria).

> Os CRD `sparkoperator.k8s.io` **não** saem com o `helm uninstall`: um chart nunca remove o que instalou
> em `crds/`. Ficam na versão 2.5.0 e serão reaproveitados pela instalação de T1.3. Se T1.3 usar um chart
> mais novo, o CRD **não** é atualizado junto — conferir a compatibilidade lá.

---

## 4. Épico 1 — Infraestrutura

### 4.1 Testes estáticos

| ID | O que prova | Comando | Resultado |
|---|---|---|---|
| ST-1 | Todo script tem sintaxe válida | `bash -n` em `minikube-up.sh`, `profile.sh`, `verify-rbac.sh`, `bootstrap.sh` | ✅ |
| ST-2 | Todo manifesto é YAML parseável | `yaml.safe_load_all` em `infra/**/*.yaml` | ✅ |
| ST-3 | O template RBAC renderiza sem sobra | `sed` dos 3 placeholders → `grep -c '{{'` = 0 | ✅ |
| ST-4 | Nenhum link interno quebrado em `docs/` | varredura de `](*.md)` contra o disco | ✅ |

Sintaxe válida não é o mesmo que semântica válida: ST-2 aprovou o `chi-datamart.yaml` que depois falhou
em D5 e D6. **Manifesto que parseia ainda pode estar errado** — é por isso que os testes de integração
abaixo existem.

### 4.2 T1.1 — Repositório e cluster base

| ID | O que prova | Resultado |
|---|---|---|
| T-E1-01 | Pré-checagem de RAM aprova `full` (15 ≥ 15) e `small` (15 ≥ 13) | ✅ |
| T-E1-02 | Pré-checagem de disco aprova (832 GiB ≥ 82 GiB) | ✅ |
| T-E1-03 | `bash cluster/minikube-up.sh --profile small` sobe o cluster e habilita os 3 addons | ✅ |
| T-E1-04 | **Aceite antigo:** `kubectl get node` mostra a capacidade do perfil | ❌ → [D4](#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup) |
| T-E1-20 | **Aceite novo:** `docker inspect` mostra o cgroup do perfil `small` | ✅ |
| T-E1-05 | `profile.sh quiesce` degrada sem erro quando o workload não existe | ✅ |
| T-E1-06 | `profile.sh resume` cai para as réplicas padrão sem arquivo de estado | ✅ |
| T-E1-07 | `profile.sh <inválido>` sai com código 1 | ✅ |
| T-E1-08 | O filtro de estado só libera `quiesce` em estado terminal | ✅ |

T-E1-08, saída completa — os oito estados possíveis de uma `SparkApplication`:

```
  COMPLETED          -> permite quiesce
  FAILED             -> permite quiesce
  SUBMISSION_FAILED  -> permite quiesce
  RUNNING            -> BLOQUEIA quiesce
  SUBMITTED          -> BLOQUEIA quiesce
  PENDING_RERUN      -> BLOQUEIA quiesce
  FAILING            -> BLOQUEIA quiesce
  <vazio>            -> BLOQUEIA quiesce
```

O caso `<vazio>` importa: uma `SparkApplication` recém-criada ainda não tem `.status`, e tratá-la como
terminal deixaria o benchmark começar em cima de uma carga que está subindo.

T-E1-20, o aceite que substituiu o de T-E1-04:

```
$ docker inspect minikube --format '{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}'
8589934592 4000000000        # 8,00 GiB / 4 cores — exatamente o perfil small
```

**Estado da task: ✅.** O aceite antigo caiu com D4 e foi substituído por um que mede o cgroup, que é o
que o kernel de fato aplica.

### 4.3 T1.2 — MinIO

| ID | O que prova | Resultado |
|---|---|---|
| T-E1-09 | StatefulSet fica `Ready` | ✅ |
| T-E1-10 | **Aceite:** os 4 prefixos existem, incluindo o `stage/` novo | ✅ |

```
[2026-09-08 11:26:21 UTC]     3B STANDARD business_datavault_data-bee/.keep
[2026-09-08 11:26:21 UTC]     3B STANDARD data-bee_replication/.keep
[2026-09-08 11:26:21 UTC]     3B STANDARD gold_datavault/.keep
[2026-09-08 11:26:21 UTC]     3B STANDARD stage/.keep
```

O MinIO pede `250m`/`512Mi`, não os `200m`/`384Mi` que a tabela de orçamento previa. **O orçamento foi
corrigido para o valor real**, não o contrário: não faz sentido apertar um serviço que já funciona para
casar com um número que eu estimei.

**Estado da task: ✅.**

### 4.4 T1.5 — ClickHouse multi-tenant

| ID | O que prova | Resultado |
|---|---|---|
| T-E1-11 | Operador Altinity 0.24.0 sobe | ✅ |
| T-E1-12 | O CHI chega a `Completed` e o pod fica `Ready` | 🔧 [D5](#d5--podtemplate-declarado-mas-nunca-aplicado) + [D6](#d6--background_pool_size--ratio-abaixo-do-mínimo-de-sanidade) |
| T-E1-13 | A imagem e os limites declarados chegam ao pod | 🔧 após D5 |
| T-E1-14 | `k8s_secret_password` resolve e `dm_admin` autentica | ✅ |
| T-E1-15 | O Job de RBAC aplica os 2 tenants | ✅ |
| T-E1-16 | **Aceite:** `verify-rbac.sh` — 4 asserções negativas | ✅ |
| T-E1-17 | Reaplicar o mesmo SQL não dá erro (idempotência) | ✅ |
| T-E1-18 | Os tetos de memória estão em vigor no servidor | ✅ |
| T-E1-19 | O `loader` executa a sequência completa de `REPLACE PARTITION` | ✅ |

T-E1-13, depois da correção de D5:

```
clickhouse/clickhouse-server:24.8
{"limits":{"cpu":"3","memory":"2304Mi"},"requests":{"cpu":"500m","memory":"1Gi"}}
```

T-E1-16, o aceite da task:

```
  [OK]    1. u_acme_ro não escreve no próprio database — negado com ACCESS_DENIED (497)
  [OK]    2. u_acme_ro não lê o database de outro tenant — negado com ACCESS_DENIED (497)
  [OK]    3. perfil do leitor — readonly = 2
  [OK]    4. perfil do leitor — join_use_nulls = 1
RBAC: 4/4 asserções passaram.
```

T-E1-18:

```
max_server_memory_usage    1.50 GiB
mark_cache_size            384.00 MiB
background_pool_size       8
background_merges_mutations_concurrency_ratio    4
```

T-E1-19 é o teste mais valioso desta rodada, porque exercita **antes de E2 existir** a operação em que a
arquitetura toda se apoia. Como `u_acme_loader`, com exatamente os `GRANT` do template e nada mais:

```sql
CREATE TABLE dm_acme.t_probe (d Date, v UInt32)
  ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d;
CREATE TABLE dm_acme.t_probe_stg AS dm_acme.t_probe;
INSERT INTO dm_acme.t_probe     VALUES ('2026-09-01', 1);
INSERT INTO dm_acme.t_probe_stg VALUES ('2026-09-01', 99);
ALTER TABLE dm_acme.t_probe REPLACE PARTITION '202609' FROM dm_acme.t_probe_stg;
```

Antes: `1`. Depois: `99`. O conjunto `SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER MOVE PARTITION,
ALTER DELETE` está correto e completo — nenhum privilégio faltando, e a troca de partição funciona sem
acesso administrativo. **Isso valida o desenho de T2.3 antes de escrever T2.3.**

**Estado da task: ✅.**

### 4.5 T1.4 — PostgreSQL, o braço de comparação

| ID | O que prova | Resultado |
|---|---|---|
| T-E1-21 | StatefulSet fica `Ready` | 🔧 [D7](#d7--argumento-inválido-no-primeiro-boot-envenena-o-pgdata) + [D8](#d8--statefulset-com-pod-nunca-ready-não-sai-do-lugar-com-kubectl-apply) |
| T-E1-22 | O tuning declarado chega ao servidor | ✅ |
| T-E1-23 | Um database por tenant, com `pg_stat_statements` | 🔧 [D10](#d10--docker-entrypoint-initdbd-é-pulado-em-silêncio-num-pvc-reusado) |
| T-E1-24 | **Aceite:** a query do painel deixa de fazer `Seq Scan` | ✅ |

T-E1-22 — os sete settings em vigor (`shared_buffers` em blocos de 8kB = 384MB):

```
shared_buffers = 491528kB      effective_cache_size = 1310728kB
work_mem = 24576kB             random_page_cost = 1.1
max_connections = 40           track_io_timing = on
```

T-E1-24 é o aceite e a prova do defeito **D3**. Tabela-sonda de 300 mil linhas
(`ddl/postgres/00_probe_indice.sql`), mesma query dos dois lados — filtro por `filial`, `banco` e janela
de um mês, com `GROUP BY`:

| | Plano escolhido | `Buffers: shared` |
|---|---|---|
| Só com a PK, como o honeycomb entrega hoje | `Parallel Seq Scan` | **3207** |
| Com `ix_fact_200_cep_dash` | `Index Scan` | **607** |

**5,3× menos I/O**, e o `Index Cond` cobre os quatro predicados. Sem esse índice o braço Postgres seria um
espantalho: toda query do benchmark varreria a tabela inteira, e a comparação não sobreviveria à primeira
pergunta técnica na sala.

> A sonda usa dado sintético **de propósito**. Ela prova a escolha de plano, não a latência — latência sai
> do dado real, em E3. Medir performance aqui seria inventar número.

**Estado da task: ✅.**

### 4.6 T1.6 — MongoDB, o control plane

| ID | O que prova | Resultado |
|---|---|---|
| T-E1-25 | StatefulSet fica `Ready` | 🔧 [D9](#d9--probe-lento-derruba-o-dns-do-service-headless) |
| T-E1-26 | O Job semeia as 4 coleções | 🔧 `cat()` não existe no mongosh |
| T-E1-27 | **Aceite:** `findOne()` devolve o documento | ✅ |
| T-E1-28 | O documento satisfaz o contrato lido nas DAGs produtivas | ✅ |

T-E1-27:

```json
{"filiais":["filial_01","filial_02"],
 "tables":[{"name":"andon_peso","chave_pk":["andon_peso_id"]}, ...],
 "schedule_interval":null,"honeycomb_version":"poc"}
```

T-E1-28 valida pelo **mesmo caminho das DAGs** — `find().sort({_id:-1}).limit(1)`, que é o
`find_one(sort=[("_id", -1)])` do pymongo — e confere as guardas que elas aplicam:

```
  acme:   filiais=2 tables=3 chave_pk_ok=true | gold tables=1 gold_type_ok=true | versao_valida=true
  globex: filiais=2 tables=3 chave_pk_ok=true | gold tables=1 gold_type_ok=true | versao_valida=true
```

T-E1-26 falhou na primeira execução por erro meu: usei `cat()` no `--eval`, helper do shell legado `mongo`
que o `mongosh` removeu. O JSON passou a ser interpolado pelo shell antes do `--eval`.

**Estado da task: ✅.**

#### Duas correções ao contrato especificado

O formato do documento saiu da **leitura das DAGs produtivas**, não de suposição:

1. São **duas coleções por tenant**, não uma. `k8s_<tenant>` alimenta o `bronze_silver`;
   `k8s_<tenant>_gold` alimenta o gold e carrega `gold_type` por tabela.
2. `source_tenants` **não entra**. É exclusivo de `k8s_lakatos_silver_super_tenant`, que a POC não replica.

> Armadilha para [E2](epicos/E2-execucao.md) T2.6: a DAG de gold produtiva exige ao menos uma tabela de
> **cada** `gold_type` (`dimension`, `fact_delta`, `fact_postgres`) — grupo vazio seria uma etapa verde que
> não materializa nada. As DAGs de gold da POC são de destino único, então essa validação precisa sair na
> cópia, ou elas falham antes de subir qualquer pod.

### 4.11 Verificações de encerramento do épico

Rodadas para responder pendências abertas, não para fechar task.

| ID | O que prova | Resultado |
|---|---|---|
| T-E1-38 | Os system logs desligados estão de fato congelados | ✅ |
| T-E1-39 | O CRD instalado (2.5.0) é compatível com o chart 2.5.2 | ✅ |
| T-E1-40 | O console do MinIO responde de fora do cluster | ✅ |

T-E1-38 — duas amostras de `system.parts` separadas por 90 s. `modification_time` não serve para isso:
ele muda em **merge**, não só em insert.

```
tabela                         antes    depois   estado
asynchronous_metric_log      7940432   7940432   congelada
error_log                        282       282   congelada
metric_log                     13813     13813   congelada
part_log                           4         4   congelada
processors_profile_log           199       199   congelada
text_log                       44234     44234   congelada
trace_log                     291309    291309   congelada
query_log                        455       457   CRESCENDO (+2)
```

Só o `query_log` cresce, que é o que se queria manter. Total do database `system` em disco: **11,67 MiB**,
num PVC com 819 GiB livres — 7,9 milhões de linhas cabem em 3 MiB porque o ClickHouse comprime a
instrumentação tão bem quanto comprime dado de negócio.

> Isso encerra a pendência de `TRUNCATE`: o risco era de **memória durante merge**, não de disco, e esse
> foi eliminado ao desligar a escrita. Limpar as tabelas antigas virou opcional.

T-E1-39 — comparação direta dos três CRD instalados contra os que o chart 2.5.2 empacota, em vez de
confiar em changelog:

```
sparkapplications              linhas divergentes: 0
scheduledsparkapplications     linhas divergentes: 0
sparkconnects                  linhas divergentes: 0
```

O `spec.versions` é idêntico nos três. **Não há risco neste par de versões.** O risco existe como classe —
Helm nunca atualiza CRD preexistente, e um campo novo do manifesto seria ignorado em silêncio — mas se
materializa só num salto de minor. O chart oferece `hook.upgradeCrd=true` para esse caso.

T-E1-40 — `infra/minio/console-nodeport.yaml`, Service separado do `minio` ClusterIP para que nada do
caminho de dados dependa dele:

```
http://192.168.49.2:30901  -> HTTP 200   (console)
http://192.168.49.2:30900  -> HTTP 200   (api)
```

Do Windows a rede do minikube (`192.168.49.0/24`) **não é roteável** — ela vive dentro do WSL2.
`scripts/minio-ui.sh --forward` cobre esse caso por `port-forward` em `localhost`.

### 4.12 T1.4 (complemento) — o braço `pg-tuned`

| ID | O que prova | Resultado |
|---|---|---|
| T-E1-41 | `03_pg_tuned.sql` cria o schema, as partições mensais e carrega | 🔧 `:origem` não é substituído dentro de `DO $$` |
| T-E1-42 | Na query do painel, o braço tunado vence o simples | ✅ |
| T-E1-43 | O BRIN é de fato usado, e ganha onde deveria | ✅ |

T-E1-41 falhou na primeira execução com `syntax error at or near ":"`. O psql **não** interpola variáveis
dentro de dollar-quoting, então `:'origem'` dentro de um bloco `DO $$ ... $$` chega literal ao servidor.
A geração das partições passou a usar `\gexec`, que é SQL puro e recebe a substituição normalmente.
Resultado: 10 partições (9 meses mais a `DEFAULT`), 300 mil linhas.

T-E1-42 — a query do painel, os três predicados e uma janela de um mês, sobre a mesma sonda:

| Braço | Plano | `Buffers` | Execução |
|---|---|---|---|
| `pg` (índice do painel) | `Index Scan` | 607 | 6,358 ms |
| `pg-tuned` (`gold_tuned`) | pruning para `fact_200_cep_202603` + `Bitmap Heap Scan` | **42** | **0,691 ms** |

**14× menos I/O e ~9× mais rápido.** Quem faz o trabalho é o partition pruning: o planner descarta nove
das dez partições antes de ler qualquer bloco.

O `Planning Time` subiu de 1,486 ms para 3,429 ms — é o custo de avaliar dez partições. Com dezenas de
meses esse custo cresce, e vira um item a observar quando a janela de dado real for maior.

T-E1-43 — na query do painel o BRIN **não** é escolhido: o btree da partição é melhor. Ele rende na
janela larga sem filtro de alta seletividade, que é o território da q05:

| Braço | Plano | `Buffers` |
|---|---|---|
| `pg` | `Parallel Seq Scan` | 3093 |
| `pg-tuned` | `Bitmap Heap Scan` via BRIN, com `Rows Removed by Index Recheck: 4016` | **136** |

**22× menos I/O.** O `Rows Removed by Index Recheck` é a assinatura do BRIN: o bitmap é lossy por
construção, e o recheck descarta o excedente.

```
CREATE INDEX fact_200_cep_202603_timestamp_idx ON gold_tuned.fact_200_cep_202603
  USING brin ("timestamp") WITH (pages_per_range='32')
```

Custo em disco dos dois métodos, sobre as dez partições. Medido com os índices criados antes da carga,
o que infla o btree e pode deixar o BRIN sem resumo — ver [D17](#d17--o-gold_tuned-criava-os-índices-antes-da-carga):

| Método | Índices | Tamanho |
|---|---|---|
| btree | 10 | 2848 kB |
| brin | 10 | **240 kB** |

> Este braço torna a comparação **mais difícil** para o ClickHouse, e é exatamente por isso que vale. Um
> ganho medido contra um Postgres levado ao limite sobrevive à pergunta *"então é só arrumar o Postgres?"*;
> medido contra o Postgres de hoje, não sobrevive.

A sonda é sintética: ela prova escolha de plano e razão de I/O, não latência absoluta. Latência sai do dado
real, em [E3](epicos/E3-validacao.md).

### 4.13 T-E1-44 — `bootstrap.sh` de ponta a ponta num cluster limpo

O item que faltava do Go/No-Go, e a afirmação mais forte que o épico faz.

`minikube delete` seguido de `bash scripts/bootstrap.sh`, sem nenhuma intervenção manual:

```
==> 1/12 cluster local                 ==> 7/12 RBAC multi-tenant
==> 2/12 imagens                         RBAC: 4/4 asserções passaram.
==> 3/12 namespaces                    ==> 8/12 PostgreSQL
==> 4/12 MinIO                         ==> 9/12 MongoDB
==> 5/12 Spark Operator + RBAC          ==> 10/12 Airflow
==> 6/12 ClickHouse operator            ==> 11/12 smoke do connector
                                          >> smoke-clickhouse COMPLETED
                                        ==> 12/12 tabela de fato por tenant
==> stack pronta.                       [exited with code 0]
```

Go/No-Go rodado em seguida, no mesmo cluster:

| # | Item | Resultado |
|---|---|---|
| 1 | `bootstrap.sh` termina sem erro | ✅ exit 0, 12/12 |
| 2 | Nenhum pod fora de `Running`/`Completed` | ✅ |
| 3 | 4 prefixos no MinIO | ✅ |
| 4 | Smoke do connector | ✅ `COMPLETED` |
| 5 | `verify-rbac.sh` | ✅ 4/4 |
| 6 | Control plane, 2 tenants × 2 coleções | ✅ 4 coleções |
| 7 | `airflow dags list-import-errors` | ✅ `No data found` |
| 8 | `profile.sh quiesce` e `resume` | ✅ nos dois sentidos |

Consumo com a stack recém-construída: **3,22 GiB de 8,00 GiB (40%)**.

Duas coisas que só esta execução podia provar:

- **O RBAC passou 4/4 de primeira.** Nas rodadas anteriores ele foi construído incrementalmente, com o
  template ganhando grants a cada falha do connector ([D11](#d11--rbac-por-database-não-basta-para-o-connector-spark)).
  Aqui o template foi aplicado uma vez, num ClickHouse virgem, e bastou.
- **A correção de idempotência do [A1](#12-auditoria-de-encerramento-do-épico-1) funcionou.** O
  `submit_spark` submeteu o smoke sem tropeçar em CR antigo — que era o cenário exato do defeito.

> A stack tinha sido construída passo a passo ao longo de quatro entregas, cada passo com seu aceite.
> Isso prova que **as peças funcionam**, não que a sequência funciona. São afirmações diferentes, e só a
> segunda sustenta "qualquer um reproduz este ambiente".

### 4.14 Consumo medido ao fim da rodada 2

| Pod | Uso | Limite |
|---|---|---|
| ClickHouse | 855Mi | 2304Mi |
| PostgreSQL | 243Mi | 1792Mi |
| MongoDB | 202Mi | 640Mi |
| MinIO | 109Mi | 1Gi |
| **nó** | **3,49 GiB** | **8,00 GiB (43,6%)** |

O MongoDB em 202Mi confirma que 384Mi teria sido apertado e que 640Mi tem folga — o número antigo era
estimativa minha, este é medição.

### 4.7 T1.3 — Spark Operator e imagem honeycomb

| ID | O que prova | Resultado |
|---|---|---|
| T-E1-29 | O overlay entrega os jars do connector sem inchar a imagem | ✅ |
| T-E1-30 | A aplicação do honeycomb sai intacta do overlay | ✅ |
| T-E1-31 | Spark Operator 2.5.2 reconcilia `SparkApplication` no namespace `datamart` | ✅ |
| T-E1-32 | **Aceite:** o smoke do connector chega a `COMPLETED` | 🔧 [D11](#d11--rbac-por-database-não-basta-para-o-connector-spark) + [D12](#d12--os-system-logs-do-clickhouse-derrubam-o-servidor) |

T-E1-29 — sete jars no classpath, uma cópia de cada, e o custo do overlay:

```
com.clickhouse.spark_clickhouse-spark-runtime-3.5_2.12-0.10.0.jar
com.clickhouse_clickhouse-client-0.9.8.jar
com.clickhouse_clickhouse-data-0.9.8.jar
com.clickhouse_clickhouse-http-client-0.9.8.jar
org.apache.httpcomponents.client5_httpclient5-5.4.4.jar
org.apache.httpcomponents.core5_httpcore5-5.3.4.jar
org.apache.httpcomponents.core5_httpcore5-h2-5.3.4.jar

base honeycomb:latest  1,73 GB  ->  honeycomb:poc  1,75 GB
```

Duas iterações até chegar nesses 20 MB. Na primeira, `find /tmp/.ivy2 -name '*.jar'` copiou **cada jar
duas vezes** — o diretório `cache/` guarda os mesmos artefatos com outro nome. Na segunda, um
`chown -R 185:185 /opt/spark/jars` reescreveu os 257 jars da base numa camada nova e a imagem foi para
2,4 GB. O `chown` não era necessário: `cp` como root já cria jars 644, legíveis por qualquer uid.

T-E1-32 é o aceite, e vale mais que contar arquivos. Como `u_acme_loader`, exercitando catálogo,
autenticação, RBAC e leitura:

```
[smoke] catalogo=clickhouse database=dm_acme host=clickhouse-datamart.datamart.svc.cluster.local:8123
[smoke] SHOW TABLES devolveu 3 tabela(s):
[smoke]   Row(namespace='dm_acme', tableName='__rbac_probe', isTemporary=False)
[smoke]   Row(namespace='dm_acme', tableName='t_probe', isTemporary=False)
[smoke]   Row(namespace='dm_acme', tableName='t_probe_stg', isTemporary=False)
[smoke] SELECT count() em clickhouse.dm_acme.__rbac_probe = 0
[smoke] OK
```

**O incidente #11 não reproduziu.** `ClickHouseCatalog.initialize` completou contra o ClickHouse 24.8 com
o conjunto de jars alinhado — nenhum `Magic is not correct`. Era o risco número um desta task.

Foram **seis execuções** até o verde, e cada falha foi um achado diferente:

| # | Onde parou | Causa |
|---|---|---|
| r1 | `system.clusters` | [D11](#d11--rbac-por-database-não-basta-para-o-connector-spark) |
| r2 | `system.macros` | D11 |
| r3 | `UnknownHostException` | [D12](#d12--os-system-logs-do-clickhouse-derrubam-o-servidor) — o ClickHouse estava OOMKilled |
| r4 | `count()` | sintaxe ClickHouse num `spark.sql()`, que passa pelo parser do Spark |
| r5 | `system.parts` | D11 |
| r6 | — | **COMPLETED** |

**Estado da task: ✅.**

### 4.8 Consumo depois de desligar os system logs

| Pod | Antes | Depois |
|---|---|---|
| ClickHouse | 855Mi | **354Mi** |
| PostgreSQL | 243Mi | 126Mi |
| MongoDB | 202Mi | 204Mi |
| MinIO | 109Mi | 120Mi |
| nó | 3,49 GiB | 3,73 GiB (com o Spark Operator a mais) |

O ClickHouse caiu **59%** só por parar de instrumentar a si mesmo.

### 4.9 T1.7 — Airflow

| ID | O que prova | Resultado |
|---|---|---|
| T-E1-33 | A imagem de produção tem Airflow 2.11.2, pymongo e todos os imports da DAG | ✅ |
| T-E1-34 | Chart 1.16.0 sobe com `LocalExecutor` e metadata DB próprio | ✅ |
| T-E1-35 | **Aceite:** `airflow dags list-import-errors` devolve `No data found` | 🔧 [D13](#d13--configmap-montada-em-optairflowdags-quebra-o-walker-de-dags) |
| T-E1-36 | A `smoke_control_plane` roda e valida o control plane dos 2 tenants | ✅ |
| T-E1-37 | O scheduler cria e lê `SparkApplication` no namespace `datamart` | ✅ |

T-E1-33 — verificado **antes** de escrever qualquer Dockerfile:

```
airflow 2.11.2 · Python 3.12.6 · pymongo 4.10.1 · uid 50000(airflow) presente
OK  pymongo, yaml, pendulum, airflow.datasets, airflow.decorators,
    airflow.providers.cncf.kubernetes.operators.spark_kubernetes,
    airflow.models, airflow.utils.state
```

A especificação pedia uma imagem custom de três linhas (`FROM apache/airflow:2.11.2` + `pip install
pymongo`). Ela já existe publicada, com exatamente esse conteúdo. Mesma correção de T1.3.

T-E1-35, o aceite:

```
$ airflow dags list-import-errors
No data found

$ airflow dags list
dag_id              | fileloc                                  | owners  | is_paused
smoke_control_plane | /opt/airflow/dags/smoke_control_plane.py | airflow | True
```

T-E1-36 e T-E1-37 — a DAG rodou com `success` e as duas tasks passaram:

```
[smoke] 6 SparkApplication em datamart: ['smoke-clickhouse', ..., 'smoke-clickhouse-r6']
[smoke] acme:   {'filiais': 2, 'tables': 3, 'gold_tables': 1, 'versao': 'poc'}
[smoke] globex: {'filiais': 2, 'tables': 3, 'gold_tables': 1, 'versao': 'poc'}
```

T-E1-37 é o que fecha o E1: **de dentro do pod do scheduler**, com a SA `airflow-scheduler`, listando CRs
do spark-operator em **outro namespace**. É a permissão exata que E2/T2.6 vai usar para submeter as
SparkApplication — provada antes de existir DAG que dependa dela.

> Um DagBag vazio também devolve `No data found`. Sem a `smoke_control_plane`, o aceite desta task seria
> vacuidade: aprovaria um Airflow que não consegue importar nada de útil.

**Estado da task: ✅. Épico 1 completo.**

### 4.10 Consumo com a stack inteira de pé

| Pod | Uso | Limite |
|---|---|---|
| Airflow scheduler | 505Mi | 1280Mi |
| ClickHouse | 355Mi | 3Gi |
| Airflow webserver | ~~135Mi~~ **824Mi** | ~~768Mi~~ **1280Mi** | 
| MinIO | 110Mi | 1Gi |
| PostgreSQL (braço) | 103Mi | 1792Mi |
| MongoDB | 169Mi | 640Mi |
| Airflow metadata PG | 60Mi | 384Mi |
| **requests agendados** | **3,86 GiB** | — |
| **nó** | **4,52 GiB** | **8,00 GiB (56,5%)** |

Os sete serviços simultâneos, sem Spark rodando. Sobram 3,5 GiB para o pico de ingestão, que pede
2,75 GiB de driver mais executor — cabe, com folga de ~0,7 GiB.

---

### 4.15 T1.8 — Acesso aos serviços e portabilidade de shell

Rodada 3, 2026-09-09. A task nasceu com duas partes: portabilidade para fish (parte A) e acesso por nome
via Ingress/DNS (parte B). A parte B foi **descartada por decisão do usuário** depois da pesquisa
registrada em [pesquisa-ingress-dns-wsl2.md](pesquisa-ingress-dns-wsl2.md): o addon `ingress-dns` está
abandonado upstream e a metade que falta — o resolvedor do WSL2 — exigiria alteração com risco à
resolução de nome corporativa. Entrou no lugar o `scripts/ports.sh`, um gerenciador de `port-forward`.

**Portabilidade (parte A)**

| ID | Verificação | Estado |
|---|---|---|
| T-E1-45 | `bash -n` nos 4 scripts alterados | ✅ |
| T-E1-46 | `check-shell-portability.sh` sai 0 | ✅ |
| T-E1-47 | Teste negativo: linha `PROFILE=small bash ...` injetada num `.md` faz o detector sair 1; removida, volta a 0 | ✅ |
| T-E1-48 | `fish -c 'bash cluster/minikube-up.sh --help'` e `submit.sh --help` saem 0 | ✅ |

O T-E1-47 é o que dá valor ao detector: sem ele, um script que sempre imprime `OK` passaria por teste.

**`ports.sh` (substituto da parte B)**

| ID | Verificação | Estado |
|---|---|---|
| T-E1-49 | `fish -c 'bash scripts/ports.sh'` sobe os 7 encaminhamentos e sai 0 | ✅ |
| T-E1-50 | `console` 200, `s3` 200, `clickhouse` `Ok.`, TCP 5432/27017/9010 abertos | ✅ |
| T-E1-51 | `airflow` responde | ❌ → [D14](#d14--webserver-do-airflow-em-oomkill-cíclico), ✅ após a correção |
| T-E1-52 | Query real pela porta encaminhada: `SELECT version(), currentUser()` → `24.8.14.39  dm_admin` | ✅ |
| T-E1-53 | Idempotência: segundo `--start` informa "ja no ar" e não duplica processo | ✅ |
| T-E1-54 | Resiliência: matar o `kubectl` filho; o supervisor reabre em < 6 s e o console volta a 200 | ✅ |
| T-E1-55 | Órfão: matar o supervisor deixa o `kubectl` segurando a porta; `--start` reapa o grupo e recupera | ✅ (após correção) |
| T-E1-56 | `--stop` derruba os 7 e deixa **0** processos de encaminhamento | ✅ (após correção) |
| T-E1-57 | `--only naoexiste` sai 2; `--xpto` sai 2 imprimindo a ajuda | ✅ |
| T-E1-58 | `--creds` lê as credenciais dos 5 serviços dos Secrets | ✅ |
| T-E1-59 | `minio-ui.sh` | ❌ → [D15](#d15--minio-uish-aponta-para-um-service-que-não-existe) |

Dois defeitos do próprio script foram encontrados **e corrigidos** durante a bateria, e ficam registrados
porque nenhum dos dois aparece em teste de caminho feliz:

- `--stop` abortava com `chave: unbound variable`. A causa é uma armadilha do bash: em
  `local a="$1" b="$ESTADO/$a.pid"`, o `$a` da segunda atribuição resolve contra a variável local ainda
  não atribuída, e com `set -u` isso mata o script. Corrigido separando as declarações.
- Matar o supervisor deixava o `kubectl` órfão segurando a porta, e o `--start` seguinte recusava para
  sempre com "porta ocupada por outro processo". Corrigido reapando o grupo de processos antes de julgar
  a porta.

> O T-E1-54 é o teste que justifica a existência do script. Um `kubectl port-forward` solto morre a cada
> restart de pod e não volta; o supervisor reabre sozinho, que é exatamente a dor que originou a task.

---

## 5. Épico 2 — Execução

✅ **Encerrado em 2026-09-11.** T2.1 a T2.3 executadas em 2026-09-09, T2.4 e T2.5 em 2026-09-10,
T2.6 fechada em 2026-09-11 sobre o dado real — [§5.8](#58-t26--aceite-sobre-o-snapshot-fixado).
Código: honeycomb **3.3.0** (`a5f2fa4`) mais o patch da POC. Testes de unidade e de integração em pod
efêmero a partir de `honeycomb:poc`, com `pytest` instalado em `--user`.

### 5.1 Resultados

| Aceite | Comando | Resultado |
|---|---|---|
| T2.1 — a suíte passa sem alteração após o rsync | `pytest -q` | **173 passed, 28 deselected** |
| T2.1 — controle: a tag pura dá o mesmo | `pytest -q` sobre `3.3.0` intocada | **173 passed, 28 deselected** |
| T2.2 — as queries parseiam com a janela | parser real do Spark sobre as 2 `.sql` | **0 falhas** |
| T2.2 — o build recusa sem janela | `build_query` sem runtime | `ValueError: fact_200_cep.sql exige JANELA_INICIO` |
| T2.2 — sem regressão | `pytest -q` | **179 passed** (+6) |
| T2.3 — troca de partição | `pytest -m integration ...datamart_clickhouse.py` | **5 passed** em 33s |

Os dois valores de T2.1 serem idênticos é o que prova que o rsync e as edições não introduziram nada.

### 5.2 Casos de T2.3, contra o ClickHouse real

Executados com `u_acme_loader`, não com `dm_admin` — então validam também o `GRANT ALTER MOVE PARTITION`
do template de RBAC (T1.5).

| Caso | O que prova |
|---|---|
| T-E2-01 sequência completa | staging criada, carregada, conferida, trocada e **descartada** |
| T-E2-02 recarga da mesma janela | contagem não muda — é o **D1** fechado |
| T-E2-03 janela fora da fronteira do mês | recusa antes de tocar o destino |
| T-E2-04 janela ausente | recusa — não há fallback para `current_timestamp()` |
| T-E2-05 dado anterior à janela | recusa: o DataFrame extrapola o que declarou |

### 5.3 Por que a base é `3.3.0` e não `main`

Apurado ao executar T2.1. Em `main` (`b0d7472`) a `fact_200_cep.sql` virou `UNION ALL` de 9 tabelas silver
mais a gold `dim_limites`, **sem filtro temporal**, com `data_hora` em 6 casas decimais. O dado ingerido
tem 4 tabelas e 3 casas. Detalhamento em [ADR-001](decisoes/ADR-001-resync-honeycomb.md).

### 5.4 O que falta no épico

Nada. Esta seção registrava o que bloqueava a T2.6. O último bloqueio foi a cópia da silver, fechada
em [§5.8](#58-t26--aceite-sobre-o-snapshot-fixado).

Três bloqueios de ambiente foram levantados e fechados em 2026-09-10, nenhum deles visível no código:

| Bloqueio | Como se manifestava | Correção |
|---|---|---|
| `honeycomb:poc` não carregava o patch | `--pipeline datamart_ch` morreria em `KeyError` na factory | Base passa a ser `honeycomb:3.3.0-local`, do `Dockerfile` da release |
| `minikube image load` não substitui tag | Pod roda o código velho, **verde** | `docker save \| docker exec -i minikube docker load` — [incidente #12](TROUBLESHOOTING.md#12-minikube-image-load-não-substitui-tag-existente) |
| `job-init` do Postgres nunca invocado | `dm_acme`/`dm_globex` inexistentes | ConfigMap + Job no passo 8/12 do `bootstrap.sh` |

### 5.5 T2.4 — braço Postgres e simetria

| Verificação | Resultado |
|---|---|
| Suíte unitária | **190 passed, 34 deselected** (era 179) |
| `RepositoryGoldDatamart` contra o Postgres real (`dm_acme`, schema próprio) | **6 passed** |
| `RepositoryDatamartClickhouse` contra o ClickHouse real | **6 passed** |
| Queries no parser real do Spark, com `DECIMAL(9,3)` | ambas parseiam; `build_query` recusa sem janela |

A simetria virou asserção executável: `read` e `transform` são o **mesmo objeto de função** nos dois
braços, e só `write` difere. Uma cópia colada que divergisse depois passaria numa conferência de olho e
falha nesse assert.

O sexto caso do ClickHouse é novo e cobre a razão de a limpeza existir: `timestamp` nulo — que
`to_timestamp(substring(...))` devolve em string malformada — é descartado pelo `transform`
compartilhado, e a troca de partição aceita a carga. Sem isso o destino recusa: `timestamp` é coluna de
partição e de ordenação, e não pode ser `Nullable`.

#### D16 — o fixture do braço Postgres estava quebrado desde antes da 3.3.0

**Severidade: média. Cobertura ausente sem sinal.**

`tests/integration/conftest.py` construía `PipelineConfig` com `pipeline_type`, `config_name`, `topic` e
`chave_pk` — nomes de uma versão anterior da dataclass. Todo teste do braço Postgres erra no setup com
`TypeError: unexpected keyword argument 'pipeline_type'`.

Não aparecia porque `addopts = -m "not integration"` deselecionava os 6, e um teste deselecionado não é
um teste falhando: a suíte fica verde. Era a única cobertura de `RepositoryGoldDatamart.write()`.

**Correção:** campos atualizados, mais duas mudanças para que a suíte possa rodar onde existe um
Postgres real — `DATAMART_PG_*` no ambiente dispensa o testcontainers (que exige daemon Docker,
indisponível dentro do cluster), e as consultas passaram a usar o schema do fixture em vez de `public`
fixo. O fixture `spark` só pede o driver JDBC por `spark.jars.packages` quando o jar não está no
classpath: resolver por ivy exige rede, e a imagem já traz o jar.

### 5.6 T2.6 — a DAG, e a primeira execução real

Duas DAGs (`k8s_acme_datamart`, `k8s_globex_datamart`) importam sem erro e serializam com 5 tasks.
Disparada a do acme com `logical_date=2025-08-21`, o CR renderizado traz a janela alinhada ao mês
(`2025-08-01` a `2025-09-01`), `VERSION`→`poc`, `TENANT`→`acme` nas três referências de `envFrom`, e
`colunas_obrigatorias`/`primary_key` vindos da seed.

Três defeitos apareceram nessa primeira execução, nenhum detectável em teste:

| # | Defeito | Onde se esconderia |
|---|---|---|
| [13](TROUBLESHOOTING.md#13-spark-defaultsconf-da-imagem-não-chega-ao-driver-sob-o-operator) | O `spark-defaults.conf` da imagem é sombreado pelo Spark-on-K8s: sem Delta no `sparkConf` a query morre em `UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY` | Roda local na mesma imagem |
| [14](TROUBLESHOOTING.md#14-dagrun-verde-sem-executar-nenhuma-task) | `logical_date` antes do `start_date`: DagRun **verde** em 63 ms, zero task instance | O estado do run diz `success` |
| [15](TROUBLESHOOTING.md#15-sparkfilenotfoundexception-na-silver) | 2 dos 5 arquivos do snapshot de `dw_andon_peso` não estão no bucket | Contar parquet na listagem dá 52 — dez vezes o que o snapshot referencia |

O #15 é dado, não código, e foi o que segurou o aceite: a carga morria em
`SparkFileNotFoundException` depois de 15 stages. Resolvido em [§5.8](#58-t26--aceite-sobre-o-snapshot-fixado).

#### Distribuição de `data_hora` na silver

> **Medição corrigida em 2026-09-10.** A primeira leitura varreu os 52 parquet do diretório e deu
> 85,8 M de linhas. Está errada por **7,7×**: o diretório guarda versões superadas do Delta, não só
> o snapshot vivo. A contagem válida é sobre os arquivos que o `_delta_log` referencia — 4 deles,
> um por filial. **Contar arquivo em bucket de tabela Delta não mede a tabela.**

> **Substituída pela medição da v3321**, com as 5 filiais, em
> [§5.8](#58-t26--aceite-sobre-o-snapshot-fixado). Esta fica como registro da cópia antiga.

Snapshot v3254 de `dw_andon_peso`, 4 filiais: **11,17 M de linhas em 14 meses**, de 2025-08 a 2026-09.

| Mês | Linhas | | Mês | Linhas |
|---|---:|---|---|---:|
| 2025-08 | 30.486 | | 2026-03 | 422.293 |
| 2025-09 | 227.166 | | 2026-04 | 642.638 |
| 2025-10 | 445.919 | | 2026-05 | 551.796 |
| 2025-11 | 497.617 | | 2026-06 | 897.336 |
| 2025-12 | 541.897 | | 2026-07 | 2.459.718 |
| 2026-01 | 188.120 | | 2026-08 | **3.332.735** |
| 2026-02 | 303.276 | | 2026-09 | 633.310 |

Uma execução de um mês toca **uma** partição — é o insumo que faltava para
[T3.0](epicos/E3-validacao.md#t30--portão-de-janela) decidir entre `REPLACE PARTITION` e
`ReplacingMergeTree`. A medição está aqui, a decisão continua sendo daquela task.

#### Por que o segundo mirror piorou o quadro

Réplica do log de `dw_andon_peso` da versão 2860 à 3307 — 448 versões — conferindo o conjunto vivo
de cada uma contra os 53 arquivos de dados do bucket:

| | |
|---|---|
| Versões com snapshot **completo** | **0 de 448** |
| Melhor caso | 1 arquivo ausente, em 9 versões; a mais recente é a v3254 |
| Estado corrente (v3307) | 5 de 5 ausentes |

Duas causas independentes:

1. **`source=data-bee_uberaba` nunca foi copiada.** Zero arquivos, nas duas execuções do mirror. É o
   único ausente na v3254 — as outras quatro filiais estão íntegras.
2. **A tabela é reescrita a cada commit.** São 5 arquivos vivos, um por filial, e cada commit troca o
   de uma delas. O `mc mirror` copia `_delta_log/` **antes** de `source=.../` — ordem lexical, `_`
   vem antes de `s` — então o log chega já apontando para arquivos que a cópia ainda não trouxe. Numa
   tabela em escrita contínua, repetir o mirror não converge: o segundo trouxe 27 versões de log e
   **um** arquivo de dados.

### 5.7 O que continua sem cobertura

A junção das quatro tabelas completou sobre o dado real, e com isso o aceite #2 de T2.2 e o
`count(distinct hk_business_id) == count(*)` passaram a ter evidência ([§5.8](#58-t26--aceite-sobre-o-snapshot-fixado)).
Seguem sem cobertura:

| Não coberto | Consequência |
|---|---|
| ~~Recarga da mesma janela sobre o dado real~~ | **Coberto em [§6.2](#62-recarga-sobre-dado-existente--o-custo-do-merge)**: contagem, soma e chave inalteradas nos dois destinos |
| `k8s_globex_datamart` | A DAG importa, mas nunca rodou. O código é o mesmo do acme; o que muda é `spark-globex-config` / `-secret`, que nunca foram exercidos |
| Comparação **linha a linha** entre os destinos | Contagem e chave batem. Valores de coluna não foram comparados — é o `sum(valor)` da T3.1. `load_dts` diverge por construção e fica de fora |
| Mais de um mês carregado | Só `202609` existe. A troca de uma partição sem tocar a vizinha não foi exercida sobre dado real |

### 5.8 T2.6 — aceite sobre o snapshot fixado

#### A cópia da silver

O `mc mirror` da tabela viva não converge ([§5.6](#por-que-o-segundo-mirror-piorou-o-quadro)). Em
2026-09-11 a cópia passou a ser de **uma versão**, não da tabela:

1. Baixar o `_delta_log/` da origem (`datawake-unipac`, 889 arquivos, 16 MiB) e fixar a versão pelo
   último `.json`: **v3321**, checkpoint na 3320;
2. Calcular o snapshot: as ações `add` do checkpoint, mais o `add`/`remove` do commit 3321;
3. Baixar **só** os arquivos que o snapshot referencia;
4. Subir os dados para o MinIO do cluster e o log **por último**.

| Filial | Arquivo vivo | Bytes no log | Bytes no cluster |
|---|---|---:|---:|
| limeira | `part-00000-cfcc05ec-…` | 1 115 676 971 | 1 115 676 971 |
| maracanau | `part-00000-b6d4e547-…` | 247 587 379 | 247 587 379 |
| paulinia | `part-00000-96539de7-…` | 397 136 290 | 397 136 290 |
| pompeia | `part-00000-e4559ed7-…` | 14 849 991 | 14 849 991 |
| uberaba | `part-00000-aafca157-…` | 56 557 984 | 56 557 984 |

Nenhum dos 5 usa deletion vector. A conferência foi feita pelo tamanho registrado no log e pelo rodapé
`PAR1`, não por checksum. O que essa cópia não permite é time travel: o log lista versões anteriores,
mas os arquivos delas não vieram. A referência local fica em `~/silver-ref/dw_andon_peso-v3321/`.

O checkpoint foi lido com `clickhouse local` no pod do ClickHouse, pela entrada padrão. Nem o host
nem a imagem `honeycomb:poc` têm `pyarrow`, e instalar só para isso não se justificava.

#### A carga

| | |
|---|---|
| Run | `manual__2026-09-11T12:33:58.641922+00:00` — 5 tasks `success` |
| Janela | `2026-09-01 00:00:00` → `2026-10-01 00:00:00` |
| `carga_postgres` | 12:34:08 → 12:41:41 (7,5 min) |
| `carga_clickhouse` | 12:41:51 → 12:50:32 (8,7 min) |

| Filial | Postgres | ClickHouse | Último `timestamp` |
|---|---:|---:|---|
| LIMEIRA | 710 774 | 710 774 | 2026-09-11 03:28:43 |
| MARACANAU | 68 632 | 68 632 | 2026-09-11 02:57:26 |
| PAULINIA | 212 264 | 212 264 | 2026-09-10 15:59:15 |
| POMPEIA | 7 463 | 7 463 | 2026-09-11 02:11:02 |
| UBERABA | 40 549 | 40 549 | 2026-09-11 02:59:21 |
| **Total** | **1 039 682** | **1 039 682** | |

`count(distinct hk_business_id)` = 1 039 682 nos dois lados, e a tupla `(filial, banco, andon_peso_id)`
também. O ClickHouse tem uma partição ativa, `202609`, em 6 parts. O último `timestamp` (03:28) é anterior
ao commit 3321 da origem (03:50), o que bate com a versão copiada.

#### Distribuição de `data_hora` na v3321

Lida só a coluna `data_hora` dos 5 arquivos vivos, pelo `s3()` do ClickHouse. **11 948 786 linhas em
14 meses**, de 2025-08 a 2026-09:

| Mês | Linhas | Filiais | | Mês | Linhas | Filiais |
|---|---:|---:|---|---|---:|---:|
| 2025-08 | 30 486 | 1 | | 2026-03 | 422 293 | 4 |
| 2025-09 | 227 166 | 2 | | 2026-04 | 642 638 | 4 |
| 2025-10 | 445 919 | 3 | | 2026-05 | 559 426 | 5 |
| 2025-11 | 497 617 | 3 | | 2026-06 | 963 773 | 5 |
| 2025-12 | 541 897 | 4 | | 2026-07 | 2 600 527 | 5 |
| 2026-01 | 188 120 | 4 | | 2026-08 | **3 480 843** | 5 |
| 2026-02 | 303 276 | 4 | | 2026-09 | 1 044 805 | 5 |

A uberaba começa em 2026-05. Setembro tem 1 044 805 linhas na silver e 1 039 682 na fato: as
**5 123** que faltam (0,49 %) caíram nos `INNER JOIN` com as outras três tabelas ou no `.na.drop` do
`transform`. Como os dois braços executam o mesmo `transform`, a perda é a mesma dos dois lados. É
filtro da query, não divergência entre motores.

## 6. Épico 3 — Validação

🟨 **Preparação completa e os dois portões fechados**: T3.1 ([§6.3](#63-t31--portão-de-corretude)) e
T3.0 ([§6.4](#64-t30--portão-de-janela)). T3.2, T3.3 e T3.4 ainda não foram executadas. O estado de cada uma está no
[estado de partida](TODO.md#estado-de-partida--o-que-o-e2-entrega-ao-e3) do TODO.

### 6.1 Preparação — carga do recorte e índice do dashboard

Recorte decidido pelo usuário em 2026-09-11: **de 2026-07 a 2026-09**. Setembro já estava carregado
([§5.8](#58-t26--aceite-sobre-o-snapshot-fixado)); julho e agosto entraram por dois triggers.

| Run | `carga_postgres` | `carga_clickhouse` |
|---|---|---|
| `manual__2026-07-15T00:00:00+00:00` | 9,7 min | 9,5 min |
| `manual__2026-08-15T00:00:00+00:00` | 12,0 min | 9,5 min |

Paridade conferida à mão, com a mesma saída que o `compare-counts.sh` da T3.1 vai produzir:

| Mês | Filial | Postgres | ClickHouse | Δ `count` | Δ `sum(valor)` |
|---|---|---:|---:|---:|---:|
| 202607 | LIMEIRA | 1 680 734 | 1 680 734 | 0 | 0 |
| 202607 | MARACANAU | 365 170 | 365 170 | 0 | 0 |
| 202607 | PAULINIA | 388 920 | 388 920 | 0 | 0 |
| 202607 | POMPEIA | 21 422 | 21 422 | 0 | 0 |
| 202607 | UBERABA | 140 809 | 140 809 | 0 | 0 |
| 202608 | LIMEIRA | 2 447 664 | 2 447 664 | 0 | 0 |
| 202608 | MARACANAU | 365 831 | 365 831 | 0 | 0 |
| 202608 | PAULINIA | 503 271 | 503 271 | 0 | 0 |
| 202608 | POMPEIA | 12 790 | 12 790 | 0 | 0 |
| 202608 | UBERABA | 148 108 | 148 108 | 0 | 0 |
| 202609 | LIMEIRA | 710 774 | 710 774 | 0 | 0 |
| 202609 | MARACANAU | 68 632 | 68 632 | 0 | 0 |
| 202609 | PAULINIA | 212 264 | 212 264 | 0 | 0 |
| 202609 | POMPEIA | 7 463 | 7 463 | 0 | 0 |
| 202609 | UBERABA | 40 549 | 40 549 | 0 | 0 |
| **Total** | | **7 114 401** | **7 114 401** | **0** | **0** |

`count(distinct hk_business_id)` é igual ao `count` em todas as linhas, nos dois lados. O ClickHouse
tem 3 partições ativas, uma por mês. Da silver para a fato caem 11 774 linhas (0,17 %), a maior parte
da pompeia: em julho, 24 894 na silver viram 21 422 na fato. É filtro da query, igual nos dois braços.

> **Isto não fecha a T3.1.** O aceite dela é o `compare-counts.sh` executável, que ainda não existe. A
> conferência aqui garante que a suíte de leitura vai começar sobre dado íntegro.

`02_indices.sql` aplicado depois da última carga: `ix_fact_200_cep_dash` com 401 MB, criado em 20 s,
mais `ANALYZE`. `public.fact_200_cep` ocupa 3 336 MB no total, contando o índice da chave única
(849 MB).

`03_pg_tuned.sql` aplicado em seguida, com autorização do usuário para o `DROP TABLE IF EXISTS` (que
não encontrou tabela): 1 min 57 s.

| Conferência de `gold_tuned` contra `public` | Resultado |
|---|---|
| Partições | `202607` 2 597 055 · `202608` 3 477 664 · `202609` 1 039 682 · `default` vazia |
| `count(*)` e `sum(valor)` | 7 114 401 nos dois; somas iguais |
| `EXCEPT ALL` sobre `(hk_business_id, valor)` | 0 linhas |
| Tamanho | 2 804 MB — sem o índice da chave única, que o `LIKE` não copia |

A primeira leitura dos tamanhos de índice revelou o
[D17](#d17--o-gold_tuned-criava-os-índices-antes-da-carga): o B-tree do painel saiu com 706 MB e o
BRIN de dois meses sem resumo. Após `REINDEX TABLE`, o B-tree ficou com 401 MB, igual ao do `public`.

RBAC com o dado real: `verify-rbac.sh` **4/4** (escrita do leitor negada, leitura cruzada entre
tenants negada com `Code 497`, `readonly = 2`, `join_use_nulls = 1`). No sentido positivo, que é o que
a T3.2 usa, `u_acme_ro` lê as 7 114 401 linhas de `dm_acme.fact_200_cep`, nos 3 meses.

#### Tenant globex

`k8s_globex_datamart` rodou pela primeira vez, com o mesmo recorte: três DagRuns, cargas de 5,9 a
10,4 min. As 15 combinações `(filial, mês)` batem entre Postgres e ClickHouse em `count` e `sum(valor)`,
`hk_business_id` é único, e o conjunto é **idêntico ao do `dm_acme`**: 7 114 401 linhas, mesmas somas.
Isso exercita pela primeira vez `spark-globex-config` e `spark-globex-secret`.

O run de setembro foi disparado primeiro com `-e 2026-09-15`, data futura, e ficou parado em `queued`
([incidente #16](TROUBLESHOOTING.md#16-dagrun-com-logical_date-futura-fica-queued-até-a-data-chegar)).
Refeito com `-e 2026-09-10`; o run futuro, sem nenhuma task instance, foi marcado `failed`.

`u_globex_ro` lê as 7 114 401 linhas do próprio tenant e recebe `ACCESS_DENIED (497)` em
`dm_acme.fact_200_cep`: o isolamento vale nos dois sentidos, não só no que o `verify-rbac.sh` testa.

#### Script de `gold_tuned` corrigido, executado de ponta a ponta

Rodado em `dm_globex` entre `BEGIN` e `ROLLBACK`, com a medição dos índices antes do rollback: 2 min 23 s,
7 114 401 linhas, e os índices saem **iguais aos do `dm_acme` reconstruído** — B-tree de 146, 196 e
59 MB e BRIN de 120, 160 e 56 kB. Após o rollback, `dm_globex` segue sem o schema `gold_tuned`.

#### Limpeza

Schemas `teste_t24`, `teste_t24_1789041162`, `teste_t24_1789041185` e `teste_t24_1789041219` removidos
de `dm_acme` com autorização do usuário. Continham só as tabelas de 16 a 32 kB da suíte de T2.4.

### 6.2 Recarga sobre dado existente — o custo do merge

Pedido do usuário: medir o merge quando o dado **já existe** no datamart. Recarregado o mês de menor
volume do recorte, 2026-09 (1 039 682 linhas), em `dm_acme`: run `manual__2026-09-10T00:00:00+00:00`,
5 tasks `success`. A linha de base é a primeira carga do mesmo mês, com o destino vazio
([§5.8](#58-t26--aceite-sobre-o-snapshot-fixado)).

Fontes: marcações de tempo do `AppLogger` no log da task, `pg_stat_statements` para o comando de merge,
`pg_stat_user_tables` e `system.parts`. O ClickHouse não tem `query_log` (desligado em
[D12](#d12--os-system-logs-do-clickhouse-derrubam-o-servidor)), então a troca de partição só tem a
resolução do log da aplicação.

| Postgres | Primeira carga | Recarga | Razão |
|---|---:|---:|---:|
| Staging via JDBC (inclui recomputar a query no Spark) | 75,8 s | 69,5 s | — |
| **Merge**, `ON CONFLICT DO UPDATE`, medido no banco | **34,7 s** | **121,5 s** | **3,5×** |
| Blocos lidos do disco | 26 390 | 345 251 | 13× |
| Blocos sujos | 81 721 | 405 549 | 5,0× |
| WAL | 0,60 GB | 2,99 GB | 5,0× |
| Atualizações HOT | — | 0 de 1 039 682 | |
| Índices mantidos | 1 (chave única) | 2 (chave única + painel) | |
| Task inteira | 7,5 min | 8,1 min | |

| ClickHouse | Primeira carga | Recarga |
|---|---:|---:|
| Staging (inclui recomputar a query no Spark) | 120,5 s | 138,0 s |
| **`REPLACE PARTITION`** | **0,1 s** | **0,07 s** |
| Partição `202609` em disco | 76,11 MiB, 6 parts | 76,10 MiB, 6 parts |
| Parts inativas ou staging sobrando | — | nenhuma |
| Task inteira | 8,7 min | 8,8 min |

**O que o Postgres deixa para trás.** O `ON CONFLICT DO UPDATE SET` reescreve todas as colunas, sem
`WHERE`, então cada uma das 1 039 682 linhas ganhou uma versão nova, mesmo idêntica (o `load_dts`
muda sempre). Nenhuma foi HOT: as páginas estavam cheias, e a versão nova foi para o fim da tabela.

| Depois da recarga | Antes | Depois |
|---|---:|---:|
| Tabela | 2 188 MB | 2 506 MB (+318 MB, +14,5 %) |
| Índice da chave única | 890 MB | 929 MB (+39 MB) |
| Índice do painel | 420 MB | 482 MB (+62 MB, +14,6 %) |
| Versões mortas | 0 | 1 045 790 |

O autovacuum **não** dispara sozinho aqui: o limiar padrão é 50 + 20 % da tabela, cerca de 1,42 M de
versões mortas. O `VACUUM (PARALLEL 0, ANALYZE)` manual levou **14,7 s** e mais 0,63 GB de WAL, e tirou
as 1 039 682 entradas mortas dos dois índices. Os arquivos não encolhem: o espaço fica reutilizável, e
a próxima recarga do mesmo mês tende a caber nele. O `VACUUM` paralelo, que é o padrão, falhou —
[D18](#d18--o-devshm-de-64-mib-derruba-o-vacuum-paralelo-do-postgres).

**Idempotência sobre o dado real**, a lacuna de [§5.7](#57-o-que-continua-sem-cobertura): nos dois
destinos, 7 114 401 linhas no total e 1 039 682 em setembro, antes e depois. O `sum(valor)` do mês é
525 212 594 dos dois lados, e o `hk_business_id` segue único. O `load_dts` foi renovado em todas as
linhas, ou seja, a recarga reescreveu tudo sem duplicar nada.

Leitura dos números:

- **O merge do Postgres fica 3,5× mais caro quando o dado existe**, e o custo continua depois: versões
  mortas, índices maiores e um `VACUUM` que o autovacuum não faria sozinho. O ClickHouse troca a
  partição em ~0,1 s nos dois casos. O custo dele é o staging, igual com o destino cheio ou vazio.
- **Duas variáveis mudaram juntas no Postgres**: o conflito e o índice do painel, criado depois da
  primeira carga. Os números não separam uma da outra. Como referência, as cargas de julho e agosto
  (inserção pura, 1 índice) custaram 37 e 48 µs por linha; a recarga custou 117 µs por linha.
- **O tempo da task quase não muda** (7,5 → 8,1 min e 8,7 → 8,8 min), porque a maior parte dele é o
  Spark recomputando a query: os `count()` de log e as guardas executam a junção de novo a cada ação,
  3 vezes no braço Postgres e 5 no braço ClickHouse. O staging dos dois inclui mais uma recomputação,
  por isso não separa escrita de leitura.
- O staging do ClickHouse é mais lento que o JDBC do Postgres (138 × 70 s). A causa não foi isolada.

Os dois portões duros continuam fechados: **T3.0** (distribuição de `timestamp`, que decide a estratégia
de carga e fecha o [ADR-004](decisoes/ADR-004-janela-de-carga.md)) e **T3.1** (`delta = 0` entre os
braços, sem o qual nenhum número de performance pode ser publicado).

### 6.3 T3.1 — portão de corretude

✅ **Passou em 2026-09-11.** `benchmark/compare-counts.sh` substitui a conferência manual da
[§6.1](#61-preparação--carga-do-recorte-e-índice-do-dashboard) e passa a ser o aceite formal. Ele
compara `count(*)` e `sum(valor)` por `(filial, mês)` em três braços, com a diferença sempre em relação
ao `pg`:

| Braço | Tabela | Leitura |
|---|---|---|
| `pg` | `dm_<tenant>` `public.fact_200_cep` | `psql` como `dm_app`, no `postgres-0` |
| `pgt` | `dm_<tenant>` `gold_tuned.fact_200_cep` | idem |
| `ch` | `dm_<tenant>.fact_200_cep` | `clickhouse-client` como `u_<tenant>_ro`, o mesmo usuário da T3.2 |

O `pgt` entra porque a T3.2 mede os três braços. Sai com código 0 só se o conjunto de chaves
`(filial, mês)` for o mesmo nos três braços **e** toda diferença for zero. Chave presente em só um
braço é falha. Divergência sai com 1 e nomeia a linha; erro de uso, de extração ou de formato do
extrato sai com 2. Extrato vazio também sai com 2, porque senão três braços vazios passariam como
"iguais".

**A armadilha da comparação.** O Postgres imprime `numeric` com 3 casas (`285638509.000`), e o
ClickHouse imprime o `Decimal` sem os zeros à direita (`285638509`). Comparar como texto reprova dado
íntegro. O script converte as duas somas para milésimos inteiros antes de comparar, o que é exato: o
maior total, 3,85 × 10¹², fica bem abaixo dos 2⁵³ que o `awk` representa sem perda.

#### Tenant acme — os três braços

```bash
bash benchmark/compare-counts.sh
```

```text
T3.1 — portão de corretude · origem: bancos ao vivo, tenant acme · braços: pg pgt ch
diferenças em relação ao pg; '-' marca chave ausente no braço

count(*) por (filial, mês)
mes     filial           n_pg      n_pgt       n_ch delta_pgt  delta_ch  status
202607  LIMEIRA       1680734    1680734    1680734         0         0  ok
202607  MARACANAU      365170     365170     365170         0         0  ok
202607  PAULINIA       388920     388920     388920         0         0  ok
202607  POMPEIA         21422      21422      21422         0         0  ok
202607  UBERABA        140809     140809     140809         0         0  ok
202608  LIMEIRA       2447664    2447664    2447664         0         0  ok
202608  MARACANAU      365831     365831     365831         0         0  ok
202608  PAULINIA       503271     503271     503271         0         0  ok
202608  POMPEIA         12790      12790      12790         0         0  ok
202608  UBERABA        148108     148108     148108         0         0  ok
202609  LIMEIRA        710774     710774     710774         0         0  ok
202609  MARACANAU       68632      68632      68632         0         0  ok
202609  PAULINIA       212264     212264     212264         0         0  ok
202609  POMPEIA          7463       7463       7463         0         0  ok
202609  UBERABA         40549      40549      40549         0         0  ok
total                 7114401    7114401    7114401         0         0

sum(valor) por (filial, mês)
mes     filial               sum_pg          sum_pgt           sum_ch     dif_pgt      dif_ch  status
202607  LIMEIRA       799524195.000    799524195.000    799524195.000           0           0  ok
202607  MARACANAU     256850778.000    256850778.000    256850778.000           0           0  ok
202607  PAULINIA      303209526.000    303209526.000    303209526.000           0           0  ok
202607  POMPEIA        55025634.000     55025634.000     55025634.000           0           0  ok
202607  UBERABA       151383172.000    151383172.000    151383172.000           0           0  ok
202608  LIMEIRA       980475565.000    980475565.000    980475565.000           0           0  ok
202608  MARACANAU     200058471.000    200058471.000    200058471.000           0           0  ok
202608  PAULINIA      382664700.000    382664700.000    382664700.000           0           0  ok
202608  POMPEIA        41876145.000     41876145.000     41876145.000           0           0  ok
202608  UBERABA       157240475.000    157240475.000    157240475.000           0           0  ok
202609  LIMEIRA       285638509.000    285638509.000    285638509.000           0           0  ok
202609  MARACANAU      38847923.000     38847923.000     38847923.000           0           0  ok
202609  PAULINIA      137298502.000    137298502.000    137298502.000           0           0  ok
202609  POMPEIA        20162314.000     20162314.000     20162314.000           0           0  ok
202609  UBERABA        43265346.000     43265346.000     43265346.000           0           0  ok
total                3853521255.000   3853521255.000   3853521255.000           0           0

RESULTADO: OK — 15 chaves (filial, mês) presentes nos 3 braços; delta = 0 em count(*) e sum(valor) em toda linha.
```

Código de saída **0**, em 16,5 s. Os números batem com a §6.1 linha a linha, com o `sum(valor)` de
setembro da §6.2 (525 212 594) e com a conferência de `gold_tuned` da §6.1.

#### Tenant globex — sem o braço `pgt`

O `dm_globex` não tem o schema `gold_tuned`: o script foi exercitado ali só entre `BEGIN` e `ROLLBACK`
([§6.1](#script-de-gold_tuned-corrigido-executado-de-ponta-a-ponta)). O comando padrão recusa com
código 2 em vez de pular o braço em silêncio. Se o schema sumisse do acme, um salto automático faria o
portão passar com dois braços:

```text
ERRO: gold_tuned.fact_200_cep não existe em dm_globex; aplique ddl/postgres/03_pg_tuned.sql ou use --sem-pgt
```

Com o braço declarado de fora:

```bash
bash benchmark/compare-counts.sh --tenant globex --sem-pgt
```

```text
T3.1 — portão de corretude · origem: bancos ao vivo, tenant globex · braços: pg ch
diferenças em relação ao pg; '-' marca chave ausente no braço

count(*) por (filial, mês)
mes     filial           n_pg       n_ch  delta_ch  status
202607  LIMEIRA       1680734    1680734         0  ok
202607  MARACANAU      365170     365170         0  ok
202607  PAULINIA       388920     388920         0  ok
202607  POMPEIA         21422      21422         0  ok
202607  UBERABA        140809     140809         0  ok
202608  LIMEIRA       2447664    2447664         0  ok
202608  MARACANAU      365831     365831         0  ok
202608  PAULINIA       503271     503271         0  ok
202608  POMPEIA         12790      12790         0  ok
202608  UBERABA        148108     148108         0  ok
202609  LIMEIRA        710774     710774         0  ok
202609  MARACANAU       68632      68632         0  ok
202609  PAULINIA       212264     212264         0  ok
202609  POMPEIA          7463       7463         0  ok
202609  UBERABA         40549      40549         0  ok
total                 7114401    7114401         0

sum(valor) por (filial, mês)
mes     filial               sum_pg           sum_ch      dif_ch  status
202607  LIMEIRA       799524195.000    799524195.000           0  ok
202607  MARACANAU     256850778.000    256850778.000           0  ok
202607  PAULINIA      303209526.000    303209526.000           0  ok
202607  POMPEIA        55025634.000     55025634.000           0  ok
202607  UBERABA       151383172.000    151383172.000           0  ok
202608  LIMEIRA       980475565.000    980475565.000           0  ok
202608  MARACANAU     200058471.000    200058471.000           0  ok
202608  PAULINIA      382664700.000    382664700.000           0  ok
202608  POMPEIA        41876145.000     41876145.000           0  ok
202608  UBERABA       157240475.000    157240475.000           0  ok
202609  LIMEIRA       285638509.000    285638509.000           0  ok
202609  MARACANAU      38847923.000     38847923.000           0  ok
202609  PAULINIA      137298502.000    137298502.000           0  ok
202609  POMPEIA        20162314.000     20162314.000           0  ok
202609  UBERABA        43265346.000     43265346.000           0  ok
total                3853521255.000   3853521255.000           0

RESULTADO: OK — 15 chaves (filial, mês) presentes nos 2 braços; delta = 0 em count(*) e sum(valor) em toda linha.
```

Código de saída **0**, com os mesmos números do acme. Isso basta para o uso do globex, que só entra na
T3.3 como vizinho barulhento.

#### Teste negativo — o portão falha quando deve

Nenhum dado foi alterado nos bancos. A extração e a comparação são estágios separados:
`--save-dir DIR` grava os extratos brutos de cada braço em `DIR/<braço>.tsv`, e `--from-dir DIR`
compara extratos gravados sem abrir conexão. Os extratos reais do acme foram gravados uma vez, e cada
caso abaixo é uma cópia com **uma** adulteração, conferida por `diff` contra o original:

| Caso | Adulteração na cópia | Saída | Esperado |
|---|---|---|---|
| Controle | nenhuma — o extrato real | exit 0, `RESULTADO: OK` | passa |
| `count` | `ch`: 202609 LIMEIRA, 710 774 → 710 775 | exit 1, `FALHA: 202609 LIMEIRA: count(*) ch - pg = +1` | falha |
| `sum` com `count` igual | `pgt`: 202607 POMPEIA, soma + 0,001 | exit 1, `FALHA: 202607 POMPEIA: sum(valor) pgt - pg = +0.001` | falha |
| Chave ausente | `ch`: linha 202608 UBERABA removida | exit 1, `FALHA: 202608 UBERABA: chave ausente no braço ch` | falha |
| Chave a mais | `pg`: linha 202609 JUNDIAI acrescentada | exit 1, chave ausente em `pgt` e em `ch`, 16 chaves | falha |
| Casas decimais | 202609 LIMEIRA com `.500` no `pg` e no `pgt` e `.5` no `ch` | exit 0 | passa: a comparação é numérica |
| Extrato vazio | `ch.tsv` vazio | exit 2, `ERRO: o braço ch não devolveu nenhuma linha` | falha |

O caso da soma é o que mais importa: 0,001 é a menor diferença que um `Decimal(9,3)` expressa, e ela é
detectada com o `count` intacto. É o sintoma de `DOUBLE` num braço e `Decimal` no outro, da tabela de
diagnóstico da [T3.1](epicos/E3-validacao.md#diagnóstico-quando-falha).

Saída completa do caso `count`, com as linhas iguais da tabela de somas omitidas:

```text
count(*) por (filial, mês)
mes     filial           n_pg      n_pgt       n_ch delta_pgt  delta_ch  status
...
202609  LIMEIRA        710774     710774     710775         0        +1  DIVERGE
...
total                 7114401    7114401    7114402         0        +1

sum(valor) por (filial, mês)
...
total                3853521255.000   3853521255.000   3853521255.000           0           0

FALHA: 202609 LIMEIRA: count(*) ch - pg = +1
RESULTADO: FALHA — 1 divergência(s) em 15 chave(s) (filial, mês).
```

Para reproduzir um caso, a partir do fish:

```bash
bash benchmark/compare-counts.sh --save-dir /tmp/t31/orig
mkdir /tmp/t31/a-count
cp /tmp/t31/orig/*.tsv /tmp/t31/a-count/
awk -F'\t' -v OFS='\t' '$1=="LIMEIRA" && $2=="202609" {$3=$3+1} 1' /tmp/t31/orig/ch.tsv > /tmp/t31/a-count/ch.tsv
bash benchmark/compare-counts.sh --from-dir /tmp/t31/a-count
```

O `--save-dir` não sobrescreve extrato existente: numa segunda gravação no mesmo diretório, sai com 2.

`scripts/check-shell-portability.sh` segue saindo 0 com o script novo.

#### O que o portão não prova

- **Igualdade linha a linha.** `count` e `sum` por `(filial, mês)` não pegam erros que se compensam
  dentro do grupo: duas linhas com `valor` trocado entre si, ou uma linha no `banco` errado da mesma
  filial. As colunas fora de `valor` não são comparadas. Entre `public` e `gold_tuned` isso já foi
  coberto pelo `EXCEPT ALL` da §6.1; entre Postgres e ClickHouse, não.
- **Estado futuro.** O resultado vale para o dado de hoje. Qualquer recarga invalida o portão, e ele
  precisa rodar de novo antes de a T3.2 medir.
- **O `load_dts`** difere entre os destinos por construção (E2, T2.3) e fica de fora de propósito.

### 6.4 T3.0 — portão de janela

Fechado em 2026-09-11 com o [ADR-004](decisoes/ADR-004-janela-de-carga.md) preenchido. As medições
completas, com o método, estão na
[análise de estratégia de carga](analise-estrategia-carga-clickhouse.md). O essencial:

| Medição | Resultado |
|---|---|
| Meses tocados por execução da POC | 1, por construção da janela mensal |
| Atraso de chegada na silver v3321, 5 filiais, desde 2026-03 (9 714 305 linhas) | p50 7,9 h · p99 213 h · p999 287 h · 12,3 % > 3 dias · 3,8 % > 7 dias · 0,016 % > 14 dias |
| `REPLACE PARTITION` de 2026-08 (3 477 664 linhas), 3 repetições, só ClickHouse | staging 3,05 a 3,31 s; troca 11 a 37 ms |
| RMT: 4 execuções de 6 h reenviando 3 dias (281 052 linhas) | 0,28 a 0,80 s por `INSERT`; 8 a 24 % de duplicatas em agosto depois dos merges |
| Leitura do painel, 1 filial × 1 mês | `MergeTree` 55 ms; RMT com `FINAL` 196 a 385 ms com várias parts, 44 a 53 ms com uma part |
| `final = 1` no perfil, ClickHouse 24.8 | Ignorado em `MergeTree`; aplicado em RMT; correto em junção dos dois |

Decisão do usuário: POC com `REPLACE PARTITION`; produção futura com B2. As tabelas de experimento
ficaram no database `bench_carga` ([§9](#9-artefatos-deixados-no-cluster)).

---

## 7. Defeitos encontrados

| ID | Severidade | Origem | Situação |
|---|---|---|---|
| [D4](#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup) | alta | limitação do driver `docker` | **aberto** — não corrigível, mitigado |
| [D5](#d5--podtemplate-declarado-mas-nunca-aplicado) | alta | herdado da V1 | corrigido, verificado em T-E1-13 |
| [D6](#d6--background_pool_size--ratio-abaixo-do-mínimo-de-sanidade) | média | introduzido nesta branch | corrigido, verificado em T-E1-12 |
| [D7](#d7--argumento-inválido-no-primeiro-boot-envenena-o-pgdata) | alta | introduzido nesta branch | corrigido, verificado em T-E1-21 |
| [D8](#d8--statefulset-com-pod-nunca-ready-não-sai-do-lugar-com-kubectl-apply) | média | comportamento do Kubernetes | contornado, documentado |
| [D9](#d9--probe-lento-derruba-o-dns-do-service-headless) | média | introduzido nesta branch | corrigido, verificado em T-E1-25 |
| [D10](#d10--docker-entrypoint-initdbd-é-pulado-em-silêncio-num-pvc-reusado) | média | padrão sugerido na especificação | corrigido, verificado em T-E1-23 |
| [D11](#d11--rbac-por-database-não-basta-para-o-connector-spark) | alta | lacuna do template de T1.5 | corrigido, verificado em T-E1-32 |
| [D12](#d12--os-system-logs-do-clickhouse-derrubam-o-servidor) | alta | default do ClickHouse | corrigido, verificado em T-E1-32 |
| [D13](#d13--configmap-montada-em-optairflowdags-quebra-o-walker-de-dags) | média | padrão sugerido na especificação | corrigido, verificado em T-E1-35 |
| [D14](#d14--webserver-do-airflow-em-oomkill-cíclico) | alta | introduzido nesta branch | corrigido, verificado em T-E1-51 |
| [D15](#d15--minio-uish-aponta-para-um-service-que-não-existe) | alta | introduzido nesta branch | corrigido, verificado em §4.15 |
| [D16](#d16--o-fixture-do-braço-postgres-estava-quebrado-desde-antes-da-330) | média | herdado do honeycomb | corrigido, verificado em §5.5 |
| [D17](#d17--o-gold_tuned-criava-os-índices-antes-da-carga) | média | introduzido nesta branch (T1.4) | corrigido, verificado em §6.1 — script de ponta a ponta e índices do `dm_acme` reconstruídos |
| [D18](#d18--o-devshm-de-64-mib-derruba-o-vacuum-paralelo-do-postgres) | alta | default do runtime de container | corrigido, verificado — hash join paralelo com pico de 96 MiB em `/dev/shm` |

### D4 — O nó anuncia a capacidade do host, não a do cgroup

**Severidade: alta.** Afeta o orçamento inteiro de [ARQUITETURA.md §5](ARQUITETURA.md#5-orçamento-de-recursos).

Medido:

| Fonte | Valor |
|---|---|
| `docker inspect` → `HostConfig.Memory` | 8589934592 (8,00 GiB) |
| cgroup dentro do nó (`/sys/fs/cgroup/memory.max`) | 8589934592 (8,00 GiB) |
| `kubectl get node` → `status.allocatable.memory` | 16221360Ki (15,47 GiB) |
| `kubectl top node` → `MEMORY(%)` | 16% — calculado sobre 15,47 GiB, não sobre 8 |

O kubelet lê `/proc/meminfo`, que dentro do container mostra a memória do **host**. O scheduler acredita
ter quase o dobro do que o kernel permite.

**O que quebra:** não há eviction. O kubelet nunca percebe pressão de memória, porque pelo cálculo dele
sobra espaço. Quem age é o OOM killer do kernel, e o alvo é o **container inteiro do nó** — não um pod.
O sintoma é o cluster sumir, não um pod reiniciar.

**Consequências aceitas:**
1. O aceite de T1.1 foi trocado: `docker inspect`, não `kubectl get node`.
2. A soma de `requests` (7,3 GiB no pico de ingestão) precisa ser lida contra os **8 GiB do cgroup**, não
   contra o que o nó anuncia. A folga real é de 0,7 GiB, não de 8.
3. `kubectl top` subestima o uso pela metade. Para julgar pressão real, usar
   `docker stats minikube`.

### D5 — `podTemplate` declarado mas nunca aplicado

**Severidade: alta. Herdado da V1** — o `chi-datamart.yaml` original tinha o mesmo erro.

`spec.defaults.templates` listava apenas `dataVolumeClaimTemplate`. Sem `podTemplate: default`, o operador
Altinity **ignora o bloco `podTemplates` inteiro** e usa o dele. Sem aviso, sem evento, sem erro.

Resultado real antes da correção:

```
image:     clickhouse/clickhouse-server:latest     (subiu 26.8.2.7, não a 24.8 fixada)
resources: {}                                       (nenhum limite de memória)
```

Ou seja: **a V1 rodou o benchmark inteiro com o ClickHouse sem limite de memória e numa versão não
fixada.** Qualquer número de performance medido lá é irreproduzível.

**Correção aplicada:** `podTemplate: default` em `spec.defaults.templates`. Verificado em T-E1-13.

> O padrão vale para todo o resto do CR: no operador Altinity, um template só existe se estiver
> **referenciado**. Declarar não basta, e o silêncio na falha é o que torna isso caro.

### D6 — `background_pool_size` × ratio abaixo do mínimo de sanidade

**Severidade: média. Introduzido por mim** ao reduzir o consumo de recursos.

Baixei `background_pool_size` de 16 para 8 e o ratio de 4 para 2. O produto (16) ficou abaixo de
`number_of_free_entries_in_pool_to_execute_mutation` (20, default). O servidor recusa iniciar:

```
Code: 36. DB::Exception: The value of 'number_of_free_entries_in_pool_to_execute_mutation'
setting (20) ... is greater than the value of 'background_pool_size' *
'background_merges_mutations_concurrency_ratio' (16) ... (BAD_ARGUMENTS)
```

**Correção aplicada:** ratio de volta para 4, produto 32. O piso real é **25**, não 20 — quem manda é
`number_of_free_entries_in_pool_to_execute_optimize_entire_partition`, cujo default é 25.

| Setting | Default | Piso que impõe ao produto |
|---|---|---|
| `number_of_free_entries_in_pool_to_execute_mutation` | 20 | > 20 |
| `number_of_free_entries_in_pool_to_execute_optimize_entire_partition` | 25 | **> 25** |

Ao mexer nesses dois settings para caber em memória, conferir o produto contra 25. Falha na subida, não
em runtime — barulhenta, ao menos.

### D7 — Argumento inválido no primeiro boot envenena o PGDATA

**Severidade: alta. Introduzido por mim**, seguindo a especificação ao pé da letra.

Escrevi o tuning como ConfigMap montada mais `-c include_dir=/etc/postgresql/conf.d`. `include_dir` só é
aceito **dentro** do `postgresql.conf`; por `-c`, o servidor morre com
`unrecognized configuration parameter`.

O estrago não parou aí. O entrypoint da imagem oficial roda, nesta ordem:

```
initdb  →  pg_setup_hba_conf "$@"  →  temp server  →  cria POSTGRES_DB  →  /docker-entrypoint-initdb.d  →  exec postgres "$@"
```

`pg_setup_hba_conf` consulta o próprio binário com os args recebidos. Com o argumento inválido ela falha, e
o `set -e` mata o entrypoint **ali** — depois do `initdb`, antes da linha de autenticação remota. Como o
`PGDATA` ficou não-vazio, toda inicialização seguinte imprime *"Skipping initialization"* e pula tudo.

Resultado: um volume permanentemente meio-inicializado — `pg_hba.conf` no default cru do `initdb`, sem
`host all all all`, e sem nenhum database de tenant. Corrigir o argumento **não conserta o volume**.

| Sintoma | Onde aparece |
|---|---|
| `unrecognized configuration parameter "include_dir"` | log do pod, na subida |
| `no pg_hba.conf entry for host ..., no encryption` | qualquer cliente remoto, depois |
| `database "dm_acme" does not exist` | qualquer cliente, depois |

**Correção:** cada setting como `-c chave=valor`, e o PVC recriado para provar o caminho limpo.

> Pré-voo que teria evitado tudo, e que custa um comando:
> `postgres -C shared_buffers <os mesmos -c>` — ele valida os argumentos sem subir o servidor.

### D8 — StatefulSet com pod nunca-`Ready` não sai do lugar com `kubectl apply`

**Severidade: média. Comportamento do Kubernetes**, não bug de ninguém — mas custa tempo se não se conhece.

Um Deployment cria um ReplicaSet novo e sobe o pod corrigido **em paralelo**. Um StatefulSet atualiza no
lugar, um ordinal por vez, e só avança quando o pod atual está `Running` **e** `Ready`. Com um pod que
nunca fica pronto, o rollout fica parado para sempre e o `apply` só troca o template:

```
current=postgres-df5978f67   update=postgres-659bcbf45b     ← nunca convergem
```

Aconteceu duas vezes nesta rodada: com o Postgres (D7) e com o MongoDB (D9).

**Como reconhecer:** `status.currentRevision != status.updateRevision` com o pod não pronto. O único
remédio é remover o pod — `delete pod` ou `scale --replicas=0/1`. O PVC não é afetado.

### D9 — Probe lento derruba o DNS do Service headless

**Severidade: média. Introduzido por mim.** A causa fica a três saltos do sintoma.

O readiness probe do MongoDB era `mongosh --quiet --eval "db.adminCommand('ping').ok"`. O mongosh é uma
aplicação Node.js e não sobe dentro do `timeoutSeconds` default, que é **1 segundo**:

```
Readiness probe failed: command timed out ... timed out after 1s
```

A cadeia a partir daí:

```
probe estoura  →  pod nunca Ready  →  Service headless não publica endpoint não-pronto
               →  mongodb.datamart.svc.cluster.local não resolve  →  Job de seed morre em "aguardando"
```

O `mongod` estava íntegro o tempo inteiro — `ping.ok = 1` respondendo em `localhost` dentro do pod. Nada no
log do MongoDB apontava para o problema, porque não havia problema no MongoDB.

**Correção:** `tcpSocket` na 27017. Além de não estourar o timeout, evita gastar CPU subindo um runtime
Node a cada 10s num pod de 100m. O `mongod` só abre a porta depois de inicializar, então a prova é
equivalente.

> Um Service headless publica **apenas** endereços prontos. Readiness num StatefulSet não é cosmético: é
> pré-requisito para o nome DNS existir.

### D10 — `/docker-entrypoint-initdb.d` é pulado em silêncio num PVC reusado

**Severidade: média. Herdado do padrão que a especificação sugeria.**

Os databases de tenant eram criados por script em `/docker-entrypoint-initdb.d`. Esse diretório só é
processado quando o `PGDATA` está vazio. Num PVC reusado — reinstalação, troca de imagem, o próprio D7 —
os scripts são pulados **sem log e sem erro**, e o cluster sobe verde sem os databases.

É a mesma classe do [D5](#d5--podtemplate-declarado-mas-nunca-aplicado): a coisa declarada é ignorada em
silêncio, e o serviço fica saudável.

**Correção:** `infra/postgres/job-init.yaml`, Job idempotente, simétrico ao `job-rbac.yaml` do ClickHouse.
O SQL vive em `ddl/postgres/01_tenants.sql` — arquivo único, montado por ConfigMap gerada dele. Roda em
qualquer estado do volume e pode ser reaplicado.

> **A correção ficou escrita e nunca foi chamada.** O `bootstrap.sh` subia o StatefulSet e parava ali:
> nem a ConfigMap `pg-init-sql` nem o Job eram aplicados, e o defeito seguiu vivo até 2026-09-10. O
> mesmo padrão do defeito que ele corrige — a coisa declarada é ignorada em silêncio. Fechado agora:
> `CREATE DATABASE` ×2, `pg_stat_statements` nos dois, conferido em `pg_database`.

### D11 — RBAC por database não basta para o connector Spark

**Severidade: alta. Lacuna do template de T1.5**, invisível para o aceite daquela task.

O template concedia privilégios apenas em `dm_<tenant>.*`. O connector ClickHouse-Spark lê tabelas de
`system` **antes** de qualquer query do usuário, e falha com `Code 497` na primeira delas:

```
DB::Exception: u_acme_loader: Not enough privileges. To execute this query, it's necessary
to have the grant SELECT(cluster, shard_num, ...) ON system.clusters. (ACCESS_DENIED)
```

Descobertas uma por execução, em três rodadas do smoke: `system.clusters`, `system.macros` e
`system.parts`. Concedidas junto, por vir do mesmo caminho: `system.databases`, `system.tables` e
`system.columns`.

> **O aceite de T1.5 passou com o RBAC quebrado.** As quatro asserções de `verify-rbac.sh` usam
> `clickhouse-client`, que nunca toca essas tabelas. O caminho que a POC de fato usa — o connector — só
> foi exercitado em T1.3. Um teste que não percorre o caminho real aprova um sistema que não funciona.

**Correção:** seis `GRANT SELECT ON system.<tabela>` no template, aplicados ao papel de loader.

**Por que não `GRANT SELECT ON system.*`:** `system.query_log` **não** tem filtro por permissão. Um tenant
leria as queries dos outros — vazamento de isolamento, justamente o que a POC quer demonstrar. As seis
tabelas concedidas são de topologia e metadados; `tables`, `columns`, `databases` e `parts` já são
filtradas por direito de acesso.

### D12 — Os system logs do ClickHouse derrubam o servidor

**Severidade: alta. Comportamento default do ClickHouse**, agravado por container pequeno.

Em cerca de quatro horas de cluster **ocioso**, sem uma única query de negócio:

| Tabela | Linhas acumuladas |
|---|---|
| `system.asynchronous_metric_log` | **7.936.382** |
| `system.trace_log` | 273.282 |
| `system.text_log` | 44.000 |
| `system.metric_log` | 13.797 |
| `system.query_log` | 278 |

O merge de background da primeira pediu **872 MiB** contra o teto de 1,5 GiB:

```
Code: 241. Memory limit (total) exceeded: would use 1.53 GiB, maximum: 1.50 GiB
MergeTreeBackgroundExecutor: {...::202609_1_1758_308}
```

O container foi `OOMKilled` seis vezes (`exitCode 137`). O efeito colateral fecha o círculo com
[D9](#d9--probe-lento-derruba-o-dns-do-service-headless): o pod perde a prontidão, o Service headless
para de publicar endpoint, e o smoke do Spark falha com `UnknownHostException` — a três saltos da causa.

**Correção:** `configuration.files` no CHI removendo `asynchronous_metric_log`, `metric_log`, `trace_log`,
`text_log`, `error_log` e `processors_profile_log`. **`query_log` fica** — o E3 correlaciona `query_id`
nele. Teto do container de 2304Mi para 3Gi e `max_server_memory_usage` de 1,5 para 2 GiB, como folga.

Efeito medido: uso do ClickHouse de **855Mi para 354Mi**, uma queda de 59% sem nenhuma carga.

> Isto tem consequência direta para o E3: qualquer medição de memória do ClickHouse feita antes desta
> correção estaria medindo, em boa parte, a instrumentação do próprio ClickHouse.

**Encerramento:** o congelamento foi confirmado por amostragem e o total em disco medido em **11,67 MiB**
([T-E1-38](#411-verificações-de-encerramento-do-épico)). Limpar as tabelas antigas é opcional — o risco era
de memória em merge, e esse acabou com a escrita.

### D13 — ConfigMap montada em `/opt/airflow/dags` quebra o walker de DAGs

**Severidade: média. Padrão sugerido na especificação.**

A especificação mandava montar as DAGs por ConfigMap em `/opt/airflow/dags`. Feito assim, qualquer
comando do Airflow que percorra o diretório morre:

```
RuntimeError: Detected recursive loop when walking DAG directory /opt/airflow/dags:
/opt/airflow/dags/..2026_09_08_16_15_53.3344774768 has appeared more than once.
```

O kubelet monta ConfigMap com atualização atômica: os arquivos reais ficam em `..<timestamp>/`, um symlink
`..data` aponta para lá, e cada chave vira outro symlink. O walker do Airflow segue os symlinks, encontra
o mesmo diretório duas vezes e aborta — **antes** de olhar qualquer arquivo `.py`.

Não é erro de import: é o comando inteiro falhando com exit 1. `list-import-errors`, `list` e o próprio
scheduler param juntos.

**Correção:** initContainer que copia com `cp -L` da ConfigMap para um `emptyDir`, e é o `emptyDir` que
vai montado em `/opt/airflow/dags`. Resolve também os subdiretórios — `manifests/`, que
[E2](epicos/E2-execucao.md) T2.6 vai precisar, e que um mount por `subPath` obrigaria a listar arquivo
por arquivo.

> Custo: as DAGs não recarregam sozinhas quando a ConfigMap muda; é preciso reiniciar o pod. Aceitável
> numa POC, e o motivo de produção usar git-sync.

---

### D14 — Webserver do Airflow em OOMKill cíclico

**Severidade: alta. Introduzido por mim ao reduzir a memória da stack.**

O pod do webserver acumulou **160 restarts em 17 h** — um a cada ~6 minutos. O motivo é inequívoco:

```
lastState.terminated.reason  → OOMKilled
resources.limits.memory      → 768Mi
```

O chart do Airflow sobe o webserver com **4 workers gunicorn** por padrão (`AIRFLOW__WEBSERVER__WORKERS=4`),
cada um na casa de 250 MiB. Quando reduzi o teto para 768Mi, não reduzi o número de workers junto: o
processo sobe, os workers carregam, o cgroup estoura e o kernel mata o container. O ciclo se repete
indefinidamente.

O sintoma visível de fora é enganoso. `kubectl get pod` mostra `Running`, e o encaminhamento de porta
falha com `connection refused` **de dentro do pod**:

```
an error occurred forwarding 8080 -> 8080: ... socat E connect(5, AF=2 127.0.0.1:8080, 16):
Connection refused
```

> **Isto contamina a medição de consumo da [§4.10](#410-consumo-com-a-stack-inteira-de-pé).** Os 135Mi
> registrados ali para o webserver são o vale logo depois de um restart, não o regime permanente. O
> número real, no pico, é o que estoura 768Mi. As demais linhas daquela tabela não são afetadas.

**Primeira tentativa de correção, insuficiente.** Baixei para `config.webserver.workers: "1"` e o pod
continuou indisponível — agora sem reiniciar o container, o que despistou por alguns minutos. O kernel do
nó deu a resposta exata:

```
oom-kill:constraint=CONSTRAINT_MEMCG ... task=gunicorn: maste
Killed process ... anon-rss:480012kB
memory.peak = 805306368        # exatamente 768 MiB, o teto
```

E a tabela de processos dentro do container:

| PID | RSS | Processo |
|---|---|---|
| 13 | 577 MiB | o **único** worker gunicorn |
| 7 | 165 MiB | master |

**Um worker sozinho ocupa 577 MiB** — ele carrega o DagBag inteiro. Com o master dá 742 MiB contra um
teto de 768 MiB: não sobra margem para pico nenhum. Com `RESTARTS 0` porque o OOM killer do cgroup
escolhia o worker, não o PID 1, então o container sobrevivia e só o serviço morria.

> A lição corrige meu diagnóstico inicial: o número de workers era **parte** da causa, não a causa. O teto
> de 768Mi que eu mesmo apertei estava abaixo do que um webserver do Airflow 2.11 precisa com **qualquer**
> número de workers.

**Correção aplicada:** `workers: "1"` **e** teto de `1280Mi`, com requests de `768Mi`.

Medido depois, com a stack inteira de pé:

| Métrica | Antes | Depois |
|---|---|---|
| `rss` anônima (não reclaimável) | 849 MiB | **659 MiB** |
| `cache` (reclaimável) | 162 MiB | 153 MiB |
| Teto do cgroup | 768 MiB | **1280 MiB** |
| Restarts | 160 em 17 h | **0** |
| Nó | — | 4,87 GiB / 8 GiB (60,9%) |

O orçamento continua fechando: 4364 Mi de requests agendados, 3828 Mi livres, e o pico do Spark pede
2816 Mi. Some-se que `profile.sh quiesce` escala o webserver para 0 antes de carga pesada, então esse teto
não disputa com o Spark na prática.

Custo declarado: com um worker, as requisições da UI são serializadas. Perceptível só se duas páginas
pesadas forem abertas ao mesmo tempo.

**Estado: ✅ resolvido.** `helm upgrade` aplicado (revisão 4), autorizado pelo usuário.

---

### D15 — `minio-ui.sh` aponta para um Service que não existe

**Severidade: alta pelo bloqueio, trivial na correção.**

```
$ bash scripts/minio-ui.sh
Error from server (NotFound): services "minio-console" not found
exit=1
```

O script consultava o NodePort `minio-console` para montar a URL do console. Esse Service era da V1;
ao reescrever `infra/minio/minio-standalone.yaml` em T1.2 deixei só o headless `minio`, e não atualizei o
script. Com `set -euo pipefail`, a busca falha e o script morre antes de imprimir qualquer coisa.

O impacto não é cosmético: `minio-ui.sh` é o caminho documentado para o usuário carregar os Parquet
reais, que é a pendência que bloqueia T2.5 e todo o Épico 3.

Passou despercebido porque nenhum teste do Épico 1 executava o script — ele não tem aceite próprio, e o
teste de T1.2 verifica os prefixos do bucket por outro caminho.

**Correção:** o script deixa de encaminhar porta e de consultar NodePort. Passa a imprimir credenciais e
o contrato de layout, delegando o acesso ao `ports.sh`; `--forward` virou `--abrir`, que chama
`ports.sh --only console`.

### D17 — O `gold_tuned` criava os índices antes da carga

**Severidade: média. Viés silencioso contra o braço que deveria ser o teto do Postgres.**

`03_pg_tuned.sql` criava o B-tree do painel e o BRIN na tabela particionada **vazia** e só depois fazia
o `INSERT ... ORDER BY "timestamp"`. As duas consequências apareceram ao montar o braço sobre o dado
real, com 7,1 M de linhas:

| Índice | Criado antes do `INSERT` | Reconstruído | Por quê |
|---|---:|---:|---|
| B-tree do painel, 3 partições | 706 MB | **401 MB** — o mesmo do `public` | Linhas chegam em ordem de `timestamp`, mas a chave começa por `filial`: inserção fora de ordem, páginas meio vazias |
| BRIN de 2026-08 | 24 kB | 160 kB | Faixas de blocos preenchidas depois da criação ficam sem resumo até um `VACUUM`, e faixa sem resumo é sempre varrida |
| BRIN de 2026-09 | 24 kB | 56 kB | Idem |

Nada falha: o índice existe, o planner o usa, e o braço tunado só fica mais lento do que deveria. Numa
comparação cujo argumento é *"mesmo o Postgres no limite perde"*, isso é viés a favor da tese.

**Correção:** os dois `CREATE INDEX` foram movidos para depois do `INSERT`, no script. No `dm_acme`,
`REINDEX TABLE gold_tuned.fact_200_cep` (22 s) reconstruiu os índices sem recriar a tabela. O script
corrigido, executado em `dm_globex` numa transação desfeita, produz exatamente os mesmos tamanhos.

**Alcance no E1:** a sonda de T-E1-41 a T-E1-43 ([§4.12](#412-t14-complemento--o-braço-pg-tuned)) usou
a ordem antiga, então os tamanhos de índice registrados ali estão inflados. A direção dos resultados
não muda: a correção só diminui os índices do braço que já tinha vencido.

### D18 — O `/dev/shm` de 64 MiB derruba o VACUUM paralelo do Postgres

**Severidade: alta para o E3. Falha real hoje, e risco de falha no meio do benchmark.**

```
INFO:  vacuuming "dm_acme.public.fact_200_cep"
ERROR:  could not resize shared memory segment "/PostgreSQL.1168348932" to 67128960 bytes: No space left on device
```

O `/dev/shm` do container é o padrão do runtime, **64 MiB**. Com `dynamic_shared_memory_type = posix`, o
Postgres aloca ali a memória dinâmica compartilhada dos processos paralelos. O `VACUUM` manual de uma
tabela com 2 índices usa workers paralelos, e o segmento tem o tamanho do `maintenance_work_mem`
(64 MB), mais do que cabe.

O autovacuum não é afetado, porque nunca roda em paralelo, e foi por isso que nada apareceu até aqui.
O risco para a T3.2 é o **hash join paralelo**, que coloca a tabela de hash no mesmo lugar: até
`work_mem` × `hash_mem_multiplier` × (workers + 1) = 24 MB × 2 × 3 ≈ 144 MB. Uma query do benchmark
pode falhar com o mesmo erro, ou o planner pode ser forçado a outro plano, e o Postgres sairia medido
em desvantagem por uma limitação de infraestrutura que produção não tem.

**Contorno usado em §6.2:** `VACUUM (PARALLEL 0, ...)`, que não aloca o segmento.

**Correção:** um `emptyDir` com `medium: Memory` e `sizeLimit: 256Mi` montado em `/dev/shm`, no
`infra/postgres/postgres-statefulset.yaml`. A memória usada ali conta dentro do limite de 1792Mi do
pod. Aplicada em 2026-09-11 com autorização do usuário: o pod foi recriado e o dado ficou no PVC.

| Verificação depois do reinício | Resultado |
|---|---|
| `df -h /dev/shm` | `tmpfs 256.0M` |
| Dado | 7 114 401 linhas em `dm_acme` e `dm_globex`, mesmo `sum(valor)`; `gold_tuned` com 7 114 401 |
| `VACUUM (PARALLEL 2, INDEX_CLEANUP ON)` | 1 worker paralelo lançado, sem erro |
| Hash join paralelo `public` × `gold_tuned` por `hk_business_id`, com `work_mem = 24MB` | 2 workers, 32 lotes, 21 s, **pico de 96 MiB em `/dev/shm`** |

O pico de 96 MiB é a prova: com o limite antigo de 64 MiB essa query falharia com o mesmo erro do
`VACUUM`. O teto novo cobre o orçamento calculado de ~144 MB. Se a T3.2 subir o `work_mem`, o teto
precisa subir junto, na proporção de `work_mem` × 2 × 3.

---

## 8. O que NÃO foi testado

Sem isto, os resultados acima valem menos do que parecem.

| Não testado | Por quê | Risco que fica aberto |
|---|---|---|
| Perfil `full` (6 vCPU / 10 GiB) | **Fora de escopo** por decisão: a POC roda em `small` | Nenhum. Os números de `full` no orçamento são aritmética e estão marcados como tal |
| Pré-checagem **abortando** | Nesta máquina os dois perfis passam | O caminho de erro — a razão de a pré-checagem existir — nunca executou |
| `profile.sh quiesce` com workload real | Airflow e MongoDB não existem | O `scale --replicas=0` e a restauração nunca escalaram nada de verdade |
| `quiesce` bloqueando por Spark ativo | Não há Spark Operator utilizável | Só a lógica de filtro foi testada, sinteticamente |
| Comportamento sob pressão de memória | Nada consumiu o nó | D4 foi medido, mas o OOM que ele prevê não foi provocado |
| Lacunas de E2 | Ver [§5.7](#57-o-que-continua-sem-cobertura) | Recarga sobre dado real, tenant globex, comparação de valores |
| Qualquer coisa de E3 | Não iniciado | Tudo |
| Recuperação do RBAC em réplica nova | 1 réplica só | O item 3 do [ADR-003](decisoes/ADR-003-rbac-multi-tenant.md) segue sendo teoria |

---

## 9. Artefatos deixados no cluster

Criados pelos testes e **não removidos** — remover exige `DROP`/`delete`, que não faço sem autorização por
ocasião. Nenhum atrapalha uma reexecução: todos os caminhos testados são idempotentes.

| Objeto | Origem |
|---|---|
| `dm_acme.__rbac_probe`, `dm_globex.__rbac_probe` | `verify-rbac.sh` — recriados a cada execução |
| `dm_acme.t_probe`, `dm_acme.t_probe_stg` | T-E1-19, o teste de `REPLACE PARTITION` |
| `probe.fact_200_cep` em `dm_acme` (Postgres) | T-E1-24, a sonda do índice — 300 mil linhas sintéticas |
| SparkApplication `smoke-clickhouse` e `-r2` a `-r6` | T-E1-32. Os pods `Error` das 5 tentativas falhas **foram removidos**; os CR continuam, todos em estado terminal |
| Seis tabelas de system log no ClickHouse | 11,67 MiB, congeladas. Limpeza opcional |
| Job `ch-rbac` | Tem `ttlSecondsAfterFinished: 300`; some sozinho |
| Job `minio-provision` | Sem TTL; precisa de remoção manual para reexecutar |
| `~/.datamart-poc-profile.state` | `profile.sh`, vazio |
| `~/.cache/datamart-poc/ports/` | `ports.sh` — arquivos de PID e log; fora do cluster |
| Pods `pytest-t21`, `pytest-t23`, `pytest-t24`, `pytest-t26` | E2 — pods efêmeros das suítes, todos `Completed` |
| ~~Schemas `teste_t24*` em `dm_acme`~~ | Removidos em 2026-09-11, com autorização |
| Database `bench_carga` no ClickHouse | Análise de carga: `fato_rp`, `fato_rmt_dash`, `fato_rmt_id`, `fato_rmt_pk` e 6 stagings `stg_rp_*`, 3,2 GiB — [análise](analise-estrategia-carga-clickhouse.md) |
| `~/silver-ref/dw_andon_peso/` | E2 — cópia local incompleta, 20 GiB; fora do cluster. A referência válida é `~/silver-ref/dw_andon_peso-v3321/` |

O `minio-provision` sem TTL é uma aspereza: `kubectl apply` num Job concluído com spec alterada falha por
imutabilidade. Vale copiar o `ttlSecondsAfterFinished` do `ch-rbac` para ele em T1.2.

---

---

## 10. Ações executadas fora da bateria

### Remoção do Spark Operator de estudo — ✅ executada pelo usuário

```bash
helm uninstall spark-operator -n dw-dados     # release "spark-operator" uninstalled
```

Autorizada explicitamente: era ambiente de estudo, obsoleto, e sai em favor do operador da POC. Não havia
nenhuma `SparkApplication` no cluster, então nenhuma carga foi descartada.

**O que saiu, conferido depois:** os 2 deployments e seus ReplicaSet, o service
`spark-operator-webhook-svc`, os ClusterRole/ClusterRoleBinding `spark-operator-controller` e
`spark-operator-webhook`, e **as 3 ServiceAccounts — incluindo `spark`**.

**O que ficou:**

| Objeto | Por quê |
|---|---|
| CRD `sparkapplications`, `scheduledsparkapplications`, `sparkconnects` | Helm nunca remove o que instalou em `crds/`. Continuam na **2.5.0** |
| `secret/spark-operator-webhook-certs` | Gerado em runtime pelo operador, não pelo chart — o release não o possuía |
| `configmap/spark-unipac-config`, `secret/spark-unipac-secret`, `secret/spark-secrets` | Não pertenciam ao release. Podem ter credenciais do estudo; decisão do dono |
| namespace `dw-dados` | Não removido |

Duas previsões minhas saíram erradas na conferência: eu esperava que `serviceaccount/spark` ficasse (saiu,
era do chart) e que `spark-operator-webhook-certs` saísse (ficou, é gerado em runtime). O registro acima é
o **medido**, não o previsto.

> Os CRD permanecem na versão 2.5.0. Se T1.3 instalar um chart mais novo, o CRD **não** é atualizado
> junto — Helm não toca em CRD preexistente. Conferir a compatibilidade antes de instalar, ou remover os
> três CRD primeiro (não há nenhum CR a perder).

Memória do nó depois da remoção: **2,81 GiB de 8,00 GiB (35%)**. Note que `docker stats` mede contra o
cgroup real, ao contrário de `kubectl top` — é a ferramenta certa sob [D4](#d4--o-nó-anuncia-a-capacidade-do-host-não-a-do-cgroup).

---
---

### Limpeza dos artefatos da pesquisa de ingress-dns — ✅ autorizada e executada

```bash
kubectl -n datamart delete deploy dnsprobe
kubectl -n datamart delete cm dnsprobe-corefile
kubectl -n datamart delete ingress probe-console teste-dns
kubectl -n airflow  delete ingress probe-airflow
minikube addons disable ingress-dns
```

Seis objetos criados por mim durante a pesquisa. Conferido depois: `kubectl get ingress -A` devolve
`No resources found`, e não há mais pod `kube-ingress-dns-minikube`. O addon `ingress` (o nginx) **não**
foi tocado — continua habilitado e é o que responde na porta 80 do nó.

### `helm upgrade` do Airflow — ✅ autorizada e executada

```bash
helm upgrade --install airflow apache-airflow/airflow --version 1.16.0 \
  -n airflow -f infra/airflow/values.yaml --timeout 15m
```

Três revisões até acertar, e o registro do erro do meio importa: a revisão 2 aplicou só
`workers: "1"` e **não resolveu** — foi preciso o kernel do nó dizer que um único worker já ocupa 577 MiB
para eu entender que o teto de 768Mi era a causa principal. A revisão 4 fechou com `768Mi/1280Mi`. Ver
[D14](#d14--webserver-do-airflow-em-oomkill-cíclico).

Nenhum dado foi perdido: o metadata DB do Airflow é um StatefulSet próprio
(`infra/airflow/postgres-metadata.yaml`), fora do release, e não foi tocado em nenhuma das revisões.

---

## 12. Auditoria de encerramento do Épico 1

Varredura em busca de pendência **perdida** — não do que está aberto no TODO, mas do que ninguém está
olhando. Quatro achados, todos corrigidos.

| # | Achado | Como apareceu |
|---|---|---|
| A1 | `bootstrap.sh` **não é idempotente** no passo do smoke | Leitura do script contra o estado real do cluster |
| A2 | Manifestos e scripts da V1 apontam para imagem inexistente | Varredura de arquivos versionados que nenhum documento menciona |
| A3 | Checklist Go/No-Go do E1 todo desmarcado, com M1 já ✅ | Comparação entre `TODO.md` e `E1-infraestrutura.md` |
| A4 | Referência morta a `infra/postgres/postgres-config.yaml` | Varredura de caminhos citados nos docs contra o disco |

### A1 — `bootstrap.sh` abortaria num cluster saudável

O passo 11 fazia `kubectl apply` da `SparkApplication` do smoke e depois lia o estado. Num ambiente onde
o CR já existe em estado terminal — que é o caso agora, `smoke-clickhouse` em `FAILED` desde a primeira
tentativa —, o `apply` vira **no-op** e a espera lê o estado **velho**. O bootstrap abortaria com
"smoke do connector falhou" num ambiente perfeitamente saudável.

**Correção:** o passo passou a usar `submit_spark` de `scripts/_lib.sh`, que faz `delete --ignore-not-found`
antes do `apply`. Efeito colateral bem-vindo: `_lib.sh` deixou de ser órfão na V2.

> É a mesma família dos defeitos D5, D10 e D11: o comando é aceito, não dá erro, e não faz o que parece.

### A2 — Legado da V1 apontando para imagem que não existe

`infra/spark/sparkapplication-{ingest,gold,normalize}.yaml` referenciam `datamart-spark:poc`. A V2
substituiu essa imagem por `honeycomb:poc` em T1.3, e eu ajustei a **memória** desses manifestos sem
corrigir a **imagem**. Os `scripts/run-*.sh` que os submetem herdaram o problema.

**Correção:** cabeçalho de legado nos seis arquivos, e os `run-*.sh` **abortam com exit 1** e uma mensagem
que explica o quê e o porquê, em vez de estourar num `ErrImageNeverPull` obscuro. Saem quando
[E2](epicos/E2-execucao.md) T2.6 entregar os manifestos novos.

### A3 — Duas verdades entre TODO e especificação

`TODO.md` marcava **M1 ✅** enquanto o checklist Go/No-Go do E1 estava com os oito itens em aberto. Sete
deles eu havia executado e registrado aqui; o oitavo, não.

**Correção:** sete marcados com o ID do teste que os provou. O primeiro — `bootstrap.sh` de ponta a ponta —
**fica aberto de propósito**, porque de fato nunca rodou: a stack subiu passo a passo. É a afirmação de
reprodutibilidade da POC, e ela segue não verificada.

### A4 — Referência morta

A lista de artefatos de T1.4 ainda citava `infra/postgres/postgres-config.yaml`, a ConfigMap de tuning
abandonada quando o [D7](#d7--argumento-inválido-no-primeiro-boot-envenena-o-pgdata) obrigou a trocar por
argumentos `-c`. Substituída por `infra/postgres/job-init.yaml`, que existe.

### O que a varredura confirmou estar íntegro

| Verificação | Resultado |
|---|---|
| Checkboxes abertos no TODO | 41, **todos de E2 e E3** — nenhum de E1 |
| `TODO`/`FIXME`/`XXX` no código | nenhum |
| Âncoras internas em todos os documentos | todas resolvem |
| Caminhos citados nos docs contra o disco | só os de E2/E3, esperados |
| Arquivos versionados sem menção em doc | só o legado da V1, agora marcado |

*Vive em `docs/TESTES.md`. Incrementar a versão a cada nova rodada de execução, acrescentando uma seção
por épico exercitado. Um defeito só sai de §7 quando existir um teste verde que prove a correção.*
