# Épico 3 — Validação

| Campo | Valor |
|---|---|
| Versão | 1.3 |
| Data | 2026-09-15 |
| Status | **Encerrado em 2026-09-15** — as 5 tasks fechadas. A T3.3 saiu do escopo em 2026-09-14 e voltou no mesmo dia, por decisão do usuário |
| Objetivo | Produzir `benchmark/results/RESULTADO.md` — o material da apresentação |
| Depende de | [E2](E2-execucao.md) completo ✅ |
| Bloqueia | — |
| Progresso | [../TODO.md](../TODO.md) |

---

## Objetivo

Medir, não afirmar. Este épico produz o único artefato que a gerência vai ler, e contém dois **portões**
que interrompem o trabalho se o dado não estiver íntegro.

## Premissas

1. Os dois braços estão carregados pela **mesma query sobre a silver, com a mesma janela explícita**,
   no mesmo DagRun. Ver [E2 v3.0](E2-execucao.md) — a gold não é materializada.
   Corolário: a silver esteve parada durante a carga. Se não esteve, T3.1 pode falhar por dado, não por
   motor. **Hoje ela é parada por construção:** a silver do cluster é o snapshot da v3321 de
   `dw_andon_peso`, copiado de uma vez, que nada reescreve. Uma recópia da origem desfaz isso.
2. O dado é uma amostra real — as conclusões são de **razão entre motores**, não de latência absoluta.
   O recorte do experimento é **de 2026-07 a 2026-09**, 7,13 M de linhas na silver, com as 5 filiais
   nos três meses. Decisão do usuário em 2026-09-11. A v3321 inteira tem 11,95 M em 14 meses.
3. Existem dois tenants **carregados** no ClickHouse. O `dm_globex` tem o mesmo recorte do `dm_acme` e é
   idêntico a ele, porque os dois leem a mesma silver. Para a T3.3 isso basta: ela mede disputa por
   recurso, não conteúdo.
4. O Postgres é medido **com** o índice do dashboard (`02_indices.sql`) e o terceiro braço existe
   (`03_pg_tuned.sql`). Os dois rodam depois da última carga. Medir sem eles compara o ClickHouse com um
   Postgres que ninguém usaria em produção.

## Fora de escopo

Projeção de custo em produção, dimensionamento de cluster produtivo, plano de migração.

---

## Tasks

| Task | Entrega | Tipo |
|---|---|---|
| [T3.0](#t30--portão-de-janela) | Distribuição de `timestamp` no incremento real | **Portão** |
| [T3.1](#t31--portão-de-corretude) | `delta = 0` entre os braços | **Portão** |
| [T3.2](#t32--suíte-de-leitura) | Latência p50/p95, sequencial e concorrente | Medição |
| [T3.3](#t33--vizinho-barulhento) | Degradação e recuperação por `QUOTA` | Medição |
| [T3.4](#t34--relatório) | `RESULTADO.md` | Entrega |

---

## T3.0 — Portão de janela

A ordem prevista não se cumpriu: a DDL do ClickHouse foi fechada em [E2](E2-execucao.md) T2.3 com
`REPLACE PARTITION` antes desta medição, e a DAG de T2.6 deriva uma janela de **um mês** por DagRun.
Com isso a pergunta abaixo tem resposta por construção, igual a 1. Este portão passa a confirmar
ou reverter uma escolha já implementada, e o escopo que sobra é decisão do usuário: fechar com esse
número, ou medir o **atraso de chegada** do dado.

O atraso **é** mensurável no snapshot: a silver guarda `load_dts` por linha, com 160 dias distintos na
limeira, e não o regrava a cada commit. Sondagem só da limeira, com `data_hora` a partir de 2026-03:
nenhuma linha chega 2 meses ou mais depois do seu mês, mas até **16 %** de um mês chega no mês
seguinte (2026-08: 395 043 de 2 447 664). A DAG atual, com um mês por execução, perderia essas
linhas em produção se não reprocessasse o mês anterior.

A medição completa, nas 5 filiais, e a comparação medida entre `REPLACE PARTITION` e
`ReplacingMergeTree` para a POC e para produção estão em
[analise-estrategia-carga-clickhouse.md](../analise-estrategia-carga-clickhouse.md).

**Fechado em 2026-09-11** com a decisão do usuário, registrada no
[ADR-004](../decisoes/ADR-004-janela-de-carga.md): a POC fica com `REPLACE PARTITION`; produção terá o
B2, `ReplacingMergeTree` carregado por watermark de `load_dts`.

### O que medir

Quantos meses distintos uma execução típica do pipeline toca. Sem gold materializada, a medição é sobre a
**silver**, na coluna que origina o `timestamp` da fato (`dap.data_hora` de `dw_andon_peso`):

```sql
SELECT count(DISTINCT date_format(
         to_timestamp(substring(data_hora, 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS'), 'yyyyMM')) AS meses_tocados
FROM delta.`s3a://datamart/business_datavault_data-bee/dw_andon_peso`
WHERE to_timestamp(substring(data_hora, 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS') >= 'JANELA_INICIO'
```

O `INNER JOIN` da query só reduz linhas, nunca acrescenta mês — então a contagem aqui é o **limite
superior** dos meses tocados, que é exatamente o número de que a decisão precisa.

### A decisão

| Resultado | Estratégia de carga |
|---|---|
| Poucos meses por execução | `REPLACE PARTITION` — atômica, idempotente, sem depender de merge assíncrono |
| Muitos meses por execução | Volta para `ReplacingMergeTree` com partição única (pesquisa base §9.4) — o custo de reescrever N partições inteiras supera o benefício |

Registrar o resultado e a decisão em [ADR-004](../decisoes/ADR-004-janela-de-carga.md).

> Este portão existe porque a escolha entre as duas estratégias **não é derivável do desenho** — depende da
> distribuição temporal do dado real, que ninguém mediu ainda. Fechar a DDL antes de medir é apostar.

### Aceite

`ADR-004` preenchido com o número medido e a decisão tomada.

---

## T3.1 — Portão de corretude

### Por que vem antes da performance

> Uma tabela de performance sem tabela de corretude não vale nada, e é a segunda coisa que qualquer DBA vai
> pedir. Número de performance sobre dado divergente é pior do que nenhum número: destrói a credibilidade
> de tudo o mais na apresentação.

### Ações

1. `benchmark/compare-counts.sh` produz, por `(filial, mês)`:
   - `n_pg` — `count(*)` no Postgres
   - `n_ch` — `count()` no ClickHouse
   - `delta` — a diferença
   - `sum(valor)` nos dois, com a diferença

### Aceite

```bash
bash benchmark/compare-counts.sh
```
**`delta = 0` em toda linha.** Sem isso, T3.2 não roda.

### Diagnóstico quando falha

| Sintoma | Causa provável |
|---|---|
| `delta` constante e negativo no CH | Limpeza `.na.drop` aplicada em só um braço ([E2](E2-execucao.md) T2.4) |
| `delta` pequeno e sem padrão | Janela não propagada, ou silver alterada entre as duas cargas — conferir a query resolvida no log dos dois jobs |
| `delta` só em um mês | `REPLACE PARTITION` com incremento parcial — o defeito D1 não foi corrigido |
| `sum(valor)` diverge mas `count` bate | `DOUBLE` em um braço e `Decimal` no outro |
| `delta` positivo no CH | Recarga sem troca de partição: linhas duplicadas |

---

## T3.2 — Suíte de leitura

### Estrutura

```
benchmark/
  queries/  q01_cep_por_unidade.{pg,ch}.sql
            q02_serie_temporal_dia.{pg,ch}.sql
            q03_top_produtos_fora_limite.{pg,ch}.sql
            q04_dashboard_view_filtro.{pg,pgt,ch}.sql
            q05_select_star_view.{pg,pgt,ch}.sql
  views/    00_views.{pg,ch}.sql
  read-bench.sh
  report.sh
  results/*.csv
  results/RESULTADO.md
```

São **três** braços: `.pg` (Postgres como hoje), `.pgt` (`gold_tuned`, particionado + BRIN) e `.ch`.
O `.pgt` é o `.pg` com outro schema no `FROM` — mesmo dialeto, mesma query. A distância entre `.pgt` e
`.ch` é o argumento real da apresentação: mostra quanto sobra **depois** de esgotar o tuning relacional.

Pares `.pg.sql` / `.ch.sql` porque o dialeto diverge de verdade. Tentar um SQL único gasta a POC em
compatibilidade em vez de medição.

### As duas queries que importam

**q04 — a central.** Imita o que o framework de dashboard faz: colunas **nomeadas** de uma view,
`WHERE filial = ? AND banco = ? AND timestamp >= ? AND timestamp < ?`, `GROUP BY`, `ORDER BY`, `LIMIT`.

**q05 — a politicamente mais valiosa.** `SELECT *` da mesma view. Mostra o ganho caindo de 10–50× para
2–5×.

> A q05 põe a decisão sobre o framework na mesa da gerência **com um número**, em vez de com uma opinião.
> Sem essa barra, alguém migra, não muda o framework, e depois declara o projeto fracassado pela razão
> errada.

### Metodologia

1. **Cronometragem e instrumentação são passos separados.** O passo de tempo não roda `EXPLAIN` — senão
   mede a instrumentação. `query_id` explícito por execução, para correlação exata.
2. **Instrumentação**, uma execução à parte por query:
   - ClickHouse: `SYSTEM FLUSH LOGS` e depois
     `SELECT query_duration_ms, read_rows, read_bytes, result_rows, memory_usage FROM system.query_log
      WHERE type='QueryFinish' AND query_id='<id>'`
   - Postgres: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` → `Execution Time`, `Shared Hit Blocks`,
     `Shared Read Blocks`. Bytes lidos = `(hit + read) × 8192`
3. **Cold**: ClickHouse `SYSTEM DROP MARK CACHE; SYSTEM DROP UNCOMPRESSED CACHE;`. Postgres: `DISCARD ALL`
   mais restart do pod — é o único jeito de esvaziar `shared_buffers`.
4. **Warm**: execuções 2..R.
5. **Concorrência**: modo `-c N` via `xargs -P N`. Rodar com `-c 1` e `-c 8`.
6. **Agregação**: `clickhouse-local` sobre os CSVs — já está na imagem do ClickHouse. Zero Python, zero
   pandas.

### Formato do CSV

Uma linha por execução:
```
ts,perfil,engine,tenant,query,repeticao,modo,concorrencia,duracao_ms,read_rows,read_bytes,result_rows
```

### Por que a concorrência não é opcional

"p95 com uma query por vez" não responde à pergunta que a gerência realmente tem, que é *quantos usuários
simultâneos isso aguenta*. Com `N=8` o comportamento dos dois motores já diverge visivelmente.

### Pré-condição dura

```bash
bash scripts/profile.sh quiesce
```
Airflow e MongoDB escalados a zero, nenhuma `SparkApplication` em estado não-terminal. Sem isso o p95 mede
o scheduler do Airflow, não o banco.

### Limitação a declarar

O page cache do sistema operacional **não** é derrubado: `drop_caches` exige privilégio, e no driver
`docker` o page cache é compartilhado com o host. Portanto "cold" significa **caches do motor derrubados**,
não I/O físico frio. Isso vai no relatório.

### Aceite

```bash
bash scripts/profile.sh quiesce
bash benchmark/read-bench.sh -r 10 -c 1
bash benchmark/read-bench.sh -r 10 -c 8
bash benchmark/report.sh
```
Desvio de p95 < 30% entre rodadas idênticas. Desvio maior significa ruído de fundo — investigar antes de
publicar.

> **Revisto em 2026-09-14** (decisão do usuário, ver a tabela abaixo): o critério passou a ser **desvio de
> p50 < 30 % entre rodadas com `-c 1`**, com o p95 publicado como faixa e o `-c 8` como indicativo. O
> critério original foi investigado, como pede o parágrafo acima, e a causa do ruído é o ambiente, não o
> motor.

### Desvios da implementação em relação a esta especificação

Registrados em 2026-09-14, com as decisões do usuário. A execução está em
[TESTES §6.5](../TESTES.md#65-t32--suíte-de-leitura).

| Especificado | Implementado | Por quê |
|---|---|---|
| `.pgt` só em q04 e q05, em arquivo próprio | Os três braços rodam as 5 consultas; `pgt` é o `.pg` com `search_path = gold_tuned` | Mesma query, sem arquivo duplicado, e mais evidência pelo mesmo custo |
| Instrumentação do ClickHouse por `system.query_log` | Eventos `SelectedRows`, `SelectedBytes` e `MemoryTrackerPeakUsage` informados pelo cliente | São os mesmos eventos do log. Correção de 2026-09-15: a justificativa original estava errada — o `query_log` **não** está desligado — o [D12](../TESTES.md#d12--os-system-logs-do-clickhouse-derrubam-o-servidor) o manteve, e a T3.3 o usa |
| `read_rows`/`read_bytes` no CSV de tempo | Colunas vazias no CSV de tempo; valores em `instr_<ts>.csv` | Cronometragem e instrumentação são passos separados (Metodologia, item 1) |
| Leitura com o usuário do tenant | `u_<tenant>_bench`: só `SELECT`, sem cota e sem limites de resultado, tempo e memória | O `p_<tenant>_ro` tem `max_result_rows = 200000` e cota por minuto; a q05 (494 mil linhas) falhava e o `-c 8` mediria a cota. O Postgres não tem limite equivalente. Decisão do usuário |
| Recursos como no orçamento do E1 | Postgres igualado ao ClickHouse: 3 CPU, 3 GiB, `shared_buffers` 640MB | Com 1,75 GiB a tabela não cabia no cache e o `pg` relia o disco. Decisão do usuário: "mesmo hardware" |
| Clientes sem local definido | Pods `bench-cliente-pg` e `bench-cliente-ch` (`benchmark/clientes.yaml`) | A CPU do cliente não entra no limite do motor. Decisão do usuário |
| `DISCARD ALL` + restart no modo frio | Restart do pod `postgres-0` antes de **cada** execução fria de `pg` e `pgt` | Uma consulta fria esquentaria o cache da seguinte |
| — | `psql` com `FETCH_COUNT` e `cursor_tuple_fraction = 1` | Sem cursor, o psql guarda o resultado inteiro da q05 na memória; o `cursor_tuple_fraction` mantém o plano de um `SELECT` comum |
| — | `--paridade` antes de medir | Os três braços precisam devolver o mesmo resultado, senão a suíte mede trabalhos diferentes. Pegou dois defeitos antes da primeira medição |
| `-r 10` | `-r 30` com `-c 1`; `-r 10` com `-c 8` | Com 10 amostras, o p95 pelo rank mais próximo é o próprio máximo, e uma execução lenta o move sozinha |
| Braços alternados a cada consulta | Um braço inteiro por vez: todas as consultas de `pg`, depois `pgt`, depois `ch` | `pg` (3,3 GB) e `pgt` (2,8 GB) dividem um pod de 3 GiB. Alternar os braços faz um expulsar o cache do outro, e a latência passa a depender da ordem |
| **Aceite: desvio de p95 < 30 %** | **Desvio de p50 < 30 % com `-c 1`**; p95 apresentado como faixa entre rodadas; `-c 8` indicativo | Duas execuções mostraram que, neste ambiente (4 vCPU no WSL2), o p95 oscila de 30 % a 120 % entre rodadas enquanto o p50 fica estável, e que com `-c 8` o cliente disputa CPU com o motor. A diferença entre os motores (9 a 57×) é muito maior que essa oscilação. Decisão do usuário, 2026-09-14 |

---

## T3.3 — Vizinho barulhento

> **Resultado (2026-09-15):** o vizinho leva o p95 do painel a 20× o isolado, **e a cota da T1.5 não o
> traz de volta** (16×). Quem protege é o limite de CPU e concorrência por usuário (1,02×), testado numa
> quarta fase que a especificação não tinha. Execução em [TESTES §6.6](../TESTES.md#66-t33--vizinho-barulhento).
> A T3.3 saiu do escopo em 2026-09-14 para o `RESULTADO.md` sair no mesmo dia, e voltou por decisão do
> usuário.

### Roteiro

1. `dm_acme` e `dm_globex` carregados;
2. medir p95 de `dm_acme` isolado;
3. rodar em loop uma query pesada e sem filtro em `dm_globex`;
4. re-medir p95 de `dm_acme` — **degrada**;
5. aplicar `QUOTA` e `max_execution_time` em `dm_globex`;
6. medir de novo — **volta**.

### Por que fazer de propósito

A pesquisa base (§10.1) classifica database-por-tenant no mesmo cluster como **sem isolamento de
recursos**, e recomenda instância dedicada. A escolha de cluster único é certa para a POC, mas o relatório
não pode afirmar isolamento forte.

Demonstrar a degradação e depois o controle transforma a fraqueza do modelo em prova de que **o mecanismo
de controle existe e funciona** — muito mais convincente do que afirmar "isolamos" e ser desmentido na
primeira pergunta.

### Aceite

```bash
bash benchmark/noisy-neighbour.sh
```
Três números: p95 isolado, p95 sob carga do vizinho, p95 sob carga **com** quota. O terceiro próximo do
primeiro.

> **Revisto em 2026-09-14** (decisão do usuário): a fase 2 degrada o p95 da q04 em 1,3× ou mais, e o
> **melhor mecanismo**, cota ou limite de CPU e concorrência, o traz a até 1,3× o isolado.

### Desvios da implementação em relação a esta especificação

| Especificado | Implementado | Por quê |
|---|---|---|
| Três pontos: isolado, vizinho, vizinho com quota | Quatro fases: a quarta com `u_globex_limitado`, `max_threads = 1` e `max_concurrent_queries_for_user = 1` | A hipótese registrada antes da execução era que a cota, que limita volume por minuto, não protegeria a latência. Sem uma alternativa medida, o resultado seria só "não funciona". Usuário e perfil próprios autorizados pelo usuário |
| Aplicar `QUOTA` e `max_execution_time` em `dm_globex` | Fase 3 com o leitor real, `u_globex_ro`: perfil e cota da T1.5, sem nada novo | Mede o que produção teria, e não uma cota desenhada para o teste |
| — | Fase 4 **sem** cota | As rejeições por concorrência contam como erro, e a cota limita erros a 20 por minuto. Com as duas juntas, quem bloquearia o vizinho seria a cota de erros, e não o limite que a fase testa |
| — | Fase 2 com `u_globex_bench`, sem cota nem limites | Para a degradação ser a do motor, e não a de um vizinho já contido |
| Medir por número de execuções | Por tempo: 180 s por fase, q04 e q01 alternadas, 10 s de aquecimento, 60 s de pausa, 2 rodadas | A carga do vizinho é contínua; medir por tempo dá a mesma janela a toda fase. Os 60 s zeram a janela da cota |
| Vizinho sem local definido | Pod `bench-vizinho-ch`, separado do pod que mede | O cliente do vizinho não disputa CPU com o cliente que mede. O estrangulamento do pod que mede ficou em 0 |

---

## T3.4 — Relatório

`benchmark/results/RESULTADO.md`, nesta ordem — **a ordem é a argumentação**:

| # | Seção | Por que nesta posição |
|---|---|---|
| 1 | Ambiente: perfil, `--cpus/--memory`, versões de imagem, contagem de linhas, tamanho em disco por motor | Antes de qualquer número. Sem isso os números não são interpretáveis |
| 2 | **Corretude**: `count(*)` e `sum(valor)` por `(filial, mês)`, com coluna `divergência` = 0 | É a segunda coisa que o DBA vai pedir. Vindo antes, desarma a objeção |
| 3 | Latência: `query \| p50 PG \| p95 PG \| p50 CH \| p95 CH \| fator p95` | O número principal |
| 4 | I/O: `read_bytes` e `read_rows` por query | É o que **explica** a tabela 3. Sem ela o ganho parece mágica |
| 5 | Compressão em disco: `pg_total_relation_size` × `sum(bytes_on_disk)` de `system.parts` | Frequentemente o número isolado mais convincente |
| 6 | q05 (`SELECT *`) destacada | Põe a decisão sobre o framework na mesa com um número |
| 7 | Vizinho barulhento, antes e depois da quota — e do limite de CPU e concorrência | Responde à pergunta de multi-tenant |
| 8 | Ressalvas | Ver abaixo |

Mais um gráfico de barras ASCII por query, para colar num deck sem ferramenta.

### As ressalvas são obrigatórias

- Page cache do SO não derrubado — "cold" é cache do motor;
- minikube não é produção: uma réplica, sem Keeper, sem replicação;
- volume da POC não é o volume produtivo — as conclusões são de **razão**, não de latência absoluta;
- database-por-tenant não dá isolamento de recursos; o que a POC mostra é que a cota não protege a
  latência, e que o limite de CPU e concorrência protege, com custo para o vizinho;
- método de carga: a POC lê uma `MergeTree` limpa. Com o B2 decidido para produção
  ([ADR-004](../decisoes/ADR-004-janela-de-carga.md)), as consultas do mês corrente pagam o `FINAL`,
  medido em 3,5 a 7× no painel enquanto a partição tem várias parts.

> Um benchmark que exagera é exatamente o que a gerência vai atacar. Declarar a limitação vale mais que o
> número — e é o que faz o resto da apresentação ser levado a sério.

### Aceite

`benchmark/results/RESULTADO.md` com as 8 seções preenchidas e nenhum campo vazio.

---

## Checklist Go/No-Go do épico

Verificado em 2026-09-14; a T3.3, em 2026-09-15.

- [x] [ADR-004](../decisoes/ADR-004-janela-de-carga.md) preenchido com o número medido
- [x] `bash benchmark/compare-counts.sh` — `delta = 0` em toda linha
- [x] `bash scripts/verify-rbac.sh` — 4 asserções negativas passam (e o leitor do globex negado no acme)
- [x] Suíte rodada com `-c 1` e `-c 8` — aceite revisto pelo usuário: desvio de **p50** < 30 % com `-c 1`,
      15 de 15; `-c 8` indicativo
- [x] `bash benchmark/noisy-neighbour.sh` — p95 degrada (20×) e volta pelo limite de CPU e
      concorrência (1,02×). **Pela cota, não volta** (16×) — aceite revisto pelo usuário
- [x] `RESULTADO.md` com as 8 seções, incluindo a de ressalvas
- [x] Nenhuma afirmação no relatório que a seção "o que a POC não prova" contradiga — o
      [ARQUITETURA §7](../ARQUITETURA.md#7-o-que-esta-poc-não-prova) foi ajustado ao resultado da T3.3
