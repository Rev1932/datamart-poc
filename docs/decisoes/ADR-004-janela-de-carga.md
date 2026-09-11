# ADR-004 — Estratégia de carga: `REPLACE PARTITION` × `ReplacingMergeTree`

| Campo | Valor |
|---|---|
| Status | **Aceita** em 2026-09-11 — POC com `REPLACE PARTITION`; produção futura com `ReplacingMergeTree` por watermark (B2) |
| Data | 2026-09-04, decidida em 2026-09-11 no portão [E3](../epicos/E3-validacao.md) T3.0 |
| Task | [E2](../epicos/E2-execucao.md) T2.2, T2.3 |

## Contexto

A V1 carrega com `writeTo().append()` numa tabela `ReplacingMergeTree(load_dts)` de partição única, e
deduplica no merge. Funciona, mas a dedup é assíncrona: entre o INSERT e o merge, a tabela tem linhas
duplicadas, e `SELECT ... FINAL` custa caro.

A V2 propõe `REPLACE PARTITION`: escrever numa staging e trocar a partição inteira, de forma atômica e
idempotente.

## O defeito que essa proposta esconde

`spark-source-code/resources/queries/fact_200_cep.sql:44`:

```sql
WHERE to_timestamp(substring(dap.data_hora, 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS')
      >= current_timestamp() - INTERVAL 10 DAYS
```

Com `PARTITION BY toYYYYMM(timestamp)`, trocar a partição `202609` com um DataFrame que contém só os dias
10–20 **apaga os dias 1–9 de setembro**. `REPLACE PARTITION` descarta o conteúdo anterior da partição
inteira — é exatamente isso que o torna idempotente, e exatamente isso que o torna perigoso com incremento
parcial.

O sintoma é a contagem da partição **caindo** entre execuções, sem nenhum erro.

## Correção obrigatória, independente da decisão

A query precisa ser parametrizada por janela **alinhada a fronteira de partição**, não por "últimos N
dias". `QueryUtils.build_query` já faz substituição de placeholder (`MINIO_BASE_PATH`, `S3_PATH_SILVER`);
estender com `JANELA_INICIO`/`JANELA_FIM` são ~6 linhas, e é digno de upstream.

Mais uma guarda executável antes da troca:
- `min(df.timestamp) >= JANELA_INICIO`
- `JANELA_INICIO == toStartOfMonth(JANELA_INICIO)`

## A decisão, e por que ela não é derivável do desenho

| Meses tocados por execução típica | Estratégia | Motivo |
|---|---|---|
| Poucos | `REPLACE PARTITION` | Atômica, idempotente, sem depender de merge assíncrono. Custo proporcional aos meses tocados |
| Muitos | `ReplacingMergeTree` com partição única | Reescrever N partições inteiras a cada execução custa mais do que o merge assíncrono. Pesquisa base §9.4 |

Isso depende da **distribuição temporal do dado real do usuário**, que ninguém mediu. Fechar a DDL antes de
medir é apostar.

## Medição

Query única na silver, em [E3](../epicos/E3-validacao.md) T3.0:

```sql
SELECT count(DISTINCT date_format(ts, 'yyyyMM')) AS meses_tocados
FROM delta.`s3a://datamart/business_datavault_data-bee/dw_andon_peso`
WHERE ts >= <janela típica de execução>
```

## Resultado

Medido em 2026-09-11. Análise completa, com as medições de escrita e leitura dos dois métodos, em
[analise-estrategia-carga-clickhouse.md](../analise-estrategia-carga-clickhouse.md).

| Pergunta | Resposta medida |
|---|---|
| Meses tocados por execução da POC | **1**, por construção: a DAG deriva uma janela de um mês do `data_interval` |
| Meses tocados por execução em produção | **2**: um mês continua recebendo linhas por até 14 dias depois de fechado |
| Atraso de chegada, 5 filiais, 9,7 M de linhas | p50 7,9 h · p99 213 h · 12,3 % com mais de 3 dias · 0,016 % com mais de 14 dias |
| Custo do `REPLACE PARTITION` no pico | staging de 3,48 M linhas em ~3,1 s; troca em 11 a 37 ms |
| Custo de leitura do `ReplacingMergeTree` | `FINAL` de 3,5 a 7× o `MergeTree` no painel enquanto a partição tem várias parts; igual com uma part |

### Decisão do usuário

1. **Na POC fica `REPLACE PARTITION`**, como está. O foco da POC é comparar a leitura entre ClickHouse
   e Postgres, e a troca de partição entrega uma tabela sem duplicata e lida sem `FINAL`.
2. **Em produção será implementado o B2:** `ReplacingMergeTree` carregado a cada 6 h por **watermark
   de `load_dts`**, com sobreposição para os replicadores, chave de ordenação só com colunas imutáveis,
   consolidação por idade e `final = 1` no perfil de leitura. Custo estimado em §12 da análise.
3. **Janela por data do evento está descartada** para qualquer estratégia: com 3 dias de margem perde
   12,3 % das linhas em silêncio.

### Consequência para o benchmark

A T3.2 mede o ClickHouse lendo uma tabela `MergeTree` limpa. Com o B2 em produção, as consultas do mês
corrente vão pagar o `FINAL`. Isso entra nas ressalvas do `RESULTADO.md` (T3.4), com o número medido.

## Consequências da escolha por `REPLACE PARTITION`

- Atomicidade é **por partição, não pelo conjunto**: com 3 meses tocados há uma janela em que 1 de 3 está
  trocado. Aceitável na POC, mas precisa ser nomeado. Testar comandos separados por vírgula num único
  `ALTER` é barato e pode resolver.
- A staging precisa de política de armazenamento idêntica ao alvo. `CREATE TABLE AS` garante hoje; quebra
  no dia em que houver tiering só no destino, com `Tables have different storage policies`.
- A staging fica no **mesmo database** do alvo — cross-database funciona em versões recentes, mas não
  sempre.
- Exige `repartition` pela expressão de partição antes do `append`, senão cada task escreve em todas as
  partições e gera `tasks × partições` parts, levando a `Code 252 TOO_MANY_PARTS`.
