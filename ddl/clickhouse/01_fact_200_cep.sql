-- Tabela de fato do datamart, por tenant. Aplicada pelo bootstrap com {{TENANT}} substituído.
--
-- MergeTree, não ReplacingMergeTree: a idempotência vem da troca atômica de partição
-- (E2/T2.3), não do merge assíncrono. Ver docs/decisoes/ADR-002 e ADR-004.

CREATE TABLE IF NOT EXISTS dm_{{TENANT}}.fact_200_cep
(
    hk_business_id          String,
    load_dts                DateTime64(3),

    filial                  LowCardinality(String),
    banco                   LowCardinality(String),
    andon_peso_id           Nullable(Int64),
    ordem_producao_id       Nullable(Int64),
    nr_ordem_producao       Nullable(String),
    unidade_producao_id     Int64,
    unidade_producao_nome   LowCardinality(String),
    entidade_id             Nullable(Int64),
    nome_entidade           Nullable(String),
    produto_id              Nullable(Int64),
    produto_codigo          Nullable(String),
    nome_produto            Nullable(String),
    atributo_id_pai         Nullable(Int64),
    timestamp               DateTime64(3),
    atributo_nome_pai       LowCardinality(String),
    valor                   Decimal(9,3),
    nome_limite_superior    LowCardinality(String),
    nome_limite_inferior    LowCardinality(String),
    valor_limite_superior   Nullable(Decimal(9,3)),
    valor_limite_inferior   Nullable(Decimal(9,3)),
    unidade_medida          LowCardinality(String),
    atributo_tipo           LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (filial, banco, unidade_producao_id, timestamp)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
