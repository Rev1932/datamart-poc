# TESTES — registro de execução por épico

| Campo | Valor |
|---|---|
| Versão | 1.5 |
| Data da execução | 2026-09-08 |
| Branch | `feat/v2-olap` |
| Escopo | **Épico 1 completo** — T1.1 a T1.7, em quatro rodadas |
| Resultado | **44 testes** · 37 verdes · 6 falharam e passaram após correção · 1 teve o critério substituído · 1 defeito aberto |
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
| T-E1-03 | `PROFILE=small bash cluster/minikube-up.sh` sobe o cluster e habilita os 3 addons | ✅ |
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

### 4.12 Consumo medido ao fim da rodada 2

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
| Airflow webserver | 135Mi | 768Mi |
| MinIO | 110Mi | 1Gi |
| PostgreSQL (braço) | 103Mi | 1792Mi |
| MongoDB | 169Mi | 640Mi |
| Airflow metadata PG | 60Mi | 384Mi |
| **requests agendados** | **3,86 GiB** | — |
| **nó** | **4,52 GiB** | **8,00 GiB (56,5%)** |

Os sete serviços simultâneos, sem Spark rodando. Sobram 3,5 GiB para o pico de ingestão, que pede
2,75 GiB de driver mais executor — cabe, com folga de ~0,7 GiB.

---

## 5. Épico 2 — Execução

⬜ **Nenhum teste executado.** Nenhuma das 6 tasks foi implementada.

| Task | Bloqueado por |
|---|---|
| T2.1 — Re-sync com `honeycomb@main` | Acesso ao repositório do honeycomb |
| T2.2 — Janela de carga | T2.1 |
| T2.3 — Repositório ClickHouse | T2.1, T1.3 |
| T2.4 — Braço Postgres | T2.1, T1.4 |
| T2.5 — Carga bronze | Dado real do usuário |
| T2.6 — DAGs | T1.6, T1.7 |

O único elemento de E2 já exercitado é o `REPLACE PARTITION` (T-E1-19), pelo caminho de privilégios.

---

## 6. Épico 3 — Validação

⬜ **Nenhum teste executado.** E3 depende de E2 inteiro e do dado real.

Os dois portões duros continuam fechados: **T3.0** (distribuição de `timestamp`, que decide a estratégia
de carga e fecha o [ADR-004](decisoes/ADR-004-janela-de-carga.md)) e **T3.1** (`delta = 0` entre os
braços, sem o qual nenhum número de performance pode ser publicado).

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

## 8. O que NÃO foi testado

Sem isto, os resultados acima valem menos do que parecem.

| Não testado | Por quê | Risco que fica aberto |
|---|---|---|
| Perfil `full` (6 vCPU / 10 GiB) | **Fora de escopo** por decisão: a POC roda em `small` | Nenhum. Os números de `full` no orçamento são aritmética e estão marcados como tal |
| Pré-checagem **abortando** | Nesta máquina os dois perfis passam | O caminho de erro — a razão de a pré-checagem existir — nunca executou |
| `profile.sh quiesce` com workload real | Airflow e MongoDB não existem | O `scale --replicas=0` e a restauração nunca escalaram nada de verdade |
| `quiesce` bloqueando por Spark ativo | Não há Spark Operator utilizável | Só a lógica de filtro foi testada, sinteticamente |
| Comportamento sob pressão de memória | Nada consumiu o nó | D4 foi medido, mas o OOM que ele prevê não foi provocado |
| `bootstrap.sh` de ponta a ponta | Passos 2, 5 e 8 dependem de tasks não implementadas | A ordem entre passos nunca foi exercitada em sequência |
| Qualquer coisa de E2 e E3 | Nada implementado | Tudo |
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

*Vive em `docs/TESTES.md`. Incrementar a versão a cada nova rodada de execução, acrescentando uma seção
por épico exercitado. Um defeito só sai de §7 quando existir um teste verde que prove a correção.*
