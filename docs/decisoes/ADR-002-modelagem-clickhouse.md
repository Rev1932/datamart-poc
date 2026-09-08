# ADR-002 — Modelagem da tabela no ClickHouse

| Campo | Valor |
|---|---|
| Status | Aceita |
| Data | 2026-09-04 |
| Task | [E2](../epicos/E2-execucao.md) T2.3 |

## Contexto

A V1 declara em `ddl/01_create_datamart_table.sql`:

```sql
ENGINE = ReplacingMergeTree(load_dts)
ORDER BY (hk_business_id)
PARTITION BY tuple()
```
com todas as colunas de negócio `Nullable`.

O comentário no arquivo justifica a partição única: "garante que TODAS as linhas de um mesmo
`hk_business_id` fiquem na mesma partição, para o `ReplacingMergeTree` deduplicar corretamente". A
justificativa é correta **para o objetivo da V1**, que era medir stress de merge. Ela não serve para o
objetivo da V2, que é medir leitura.

## Decisão

```sql
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (filial, banco, unidade_producao_id, timestamp)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
```

`LowCardinality(String)` em `filial, banco, unidade_producao_nome, atributo_nome_pai, atributo_tipo,
unidade_medida, nome_limite_*`. `Decimal(18,4)` em `valor` e `valor_limite_*`. `Nullable` removido de
`timestamp, filial, banco, unidade_producao_id`.

## Justificativa

### A chave de ordenação é o modo de falha dominante

Um SHA-256 tem distribuição uniforme por construção. Ordenar por ele espalha linhas da mesma filial, do
mesmo mês, da mesma unidade de produção por todas as granularidades do disco. O índice esparso — o
mecanismo pelo qual o ClickHouse evita ler dado irrelevante — deixa de podar qualquer coisa, e toda query
vira varredura completa.

O sintoma não é um erro. É um ganho de 2–3× onde deveria haver 10–50×, e alguém concluindo que o
ClickHouse "não fez tanta diferença assim".

**Esta é a única decisão da POC que não se corrige sem recriar a tabela e recarregar tudo.**

### A chave correta já está declarada no ambiente

Os índices da gold produtiva (`journey-dashboards-pipeline/sql/gold/02_gold_indexes.sql`) descrevem o
padrão de acesso real do dashboard: `(filial, banco, data_hora)` e
`(filial, banco, unidade_producao_codigo)`. A chave de ordenação segue esse padrão, do mais usado em
igualdade para o mais granular.

### Particionamento mensal

Habilita poda por período e é o pré-requisito de `REPLACE PARTITION` ([ADR-004](ADR-004-janela-de-carga.md)).
Granularidade diária geraria partes demais para o volume da POC.

### Tipos

| Escolha | Efeito |
|---|---|
| `LowCardinality(String)` | Dicionariza colunas com poucos valores distintos: corta disco e acelera `GROUP BY` |
| `Decimal(18,4)` em vez de `DOUBLE` | A query hoje faz `CAST(... AS DOUBLE)`. O `get_type_sql` do Postgres mapeia isso para `DOUBLE PRECISION`; sem alinhar os dois braços, `sum(valor)` diverge no portão de corretude |
| `Nullable` removido | Cada coluna `Nullable` carrega uma coluna extra de máscara. Além disso, `PARTITION BY` e chave de ordenação exigem não-nulo |

## Consequências

- `timestamp` não pode ser `Nullable`, mas `to_timestamp(substring(...))` devolve `NULL` em dado
  malformado. Exige limpeza explícita no `transform` **compartilhado** entre os braços
  ([E2](../epicos/E2-execucao.md) T2.4). Sem isso: `Code 349 CANNOT_CONVERT_TO_NULLABLE` no meio da carga.
- Sair de `ReplacingMergeTree` transfere a idempotência do merge assíncrono para a troca atômica de
  partição — decisão sujeita ao portão de [ADR-004](ADR-004-janela-de-carga.md).
- `timestamp` é nome legal de coluna nos dois motores, mas colide com o nome do tipo em alguns contextos:
  usar crase no ClickHouse e aspas duplas no Postgres nas queries do benchmark.
