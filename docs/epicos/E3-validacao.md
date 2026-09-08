# Épico 3 — Validação

| Campo | Valor |
|---|---|
| Versão | 1.0 |
| Data | 2026-09-04 |
| Status | Não iniciado |
| Objetivo | Produzir `benchmark/results/RESULTADO.md` — o material da apresentação |
| Depende de | [E2](E2-execucao.md) completo |
| Bloqueia | — |
| Progresso | [../TODO.md](../TODO.md) |

---

## Objetivo

Medir, não afirmar. Este épico produz o único artefato que a gerência vai ler, e contém dois **portões**
que interrompem o trabalho se o dado não estiver íntegro.

## Premissas

1. Os dois braços estão carregados a partir do mesmo Delta silver.
2. O dado é uma amostra real — as conclusões são de **razão entre motores**, não de latência absoluta.
3. Existem dois tenants no ClickHouse.

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

Roda logo após [E2](E2-execucao.md) T2.5, **antes** de fechar a DDL definitiva.

### O que medir

Quantos meses distintos uma execução típica do pipeline toca. Uma query só, na silver:

```sql
SELECT count(DISTINCT date_format(ts, 'yyyyMM')) AS meses_tocados
FROM delta.`s3a://datamart/business_datavault_data-bee/dw_andon_peso`
WHERE ts >= <janela típica de execução>
```

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

---

## T3.3 — Vizinho barulhento

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
| 7 | Vizinho barulhento, antes e depois da quota | Responde à pergunta de multi-tenant |
| 8 | Ressalvas | Ver abaixo |

Mais um gráfico de barras ASCII por query, para colar num deck sem ferramenta.

### As ressalvas são obrigatórias

- Page cache do SO não derrubado — "cold" é cache do motor;
- minikube não é produção: uma réplica, sem Keeper, sem replicação;
- volume da POC não é o volume produtivo — as conclusões são de **razão**, não de latência absoluta;
- database-por-tenant não dá isolamento de recursos; o que a POC mostra é o controle por quota.

> Um benchmark que exagera é exatamente o que a gerência vai atacar. Declarar a limitação vale mais que o
> número — e é o que faz o resto da apresentação ser levado a sério.

### Aceite

`benchmark/results/RESULTADO.md` com as 8 seções preenchidas e nenhum campo vazio.

---

## Checklist Go/No-Go do épico

- [ ] [ADR-004](../decisoes/ADR-004-janela-de-carga.md) preenchido com o número medido
- [ ] `bash benchmark/compare-counts.sh` — `delta = 0` em toda linha
- [ ] `bash scripts/verify-rbac.sh` — 4 asserções negativas passam
- [ ] Suíte rodada com `-c 1` e `-c 8`, desvio de p95 < 30% entre rodadas idênticas
- [ ] `bash benchmark/noisy-neighbour.sh` — p95 degrada e volta após a quota
- [ ] `RESULTADO.md` com as 8 seções, incluindo a de ressalvas
- [ ] Nenhuma afirmação no relatório que a seção "o que a POC não prova" contradiga
