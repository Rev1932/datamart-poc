-- Tabela destino do Job C (pipeline `datamart`).
-- TEMPLATE para a tabela fact_200_cep: as colunas de negócio abaixo espelham o
-- resultado de resources/queries/fact_200_cep.sql. Se a query mudar (ou para outra
-- tabela), ajuste as colunas/tipos para casar com o schema do DataFrame do Spark
-- (o connector ClickHouse-Spark exige que a tabela exista com tipos compatíveis).
--
-- Chave e versão vêm prontas da leitura da gold:
--   hk_business_id = sha2(concat_ws("_", <chave_pk>), 256)  -> String hex de 64 chars
--   load_dts       = current_timestamp()

CREATE DATABASE IF NOT EXISTS datamart;

CREATE TABLE IF NOT EXISTS datamart.fact_200_cep
(
    hk_business_id          String,
    load_dts                DateTime64(3),

    filial                  Nullable(String),
    banco                   Nullable(String),
    andon_peso_id           Nullable(Int64),
    ordem_producao_id       Nullable(Int64),
    nr_ordem_producao       Nullable(String),
    unidade_producao_id     Nullable(Int64),
    unidade_producao_nome   Nullable(String),
    entidade_id             Nullable(Int32),
    nome_entidade           Nullable(String),
    produto_id              Nullable(Int64),
    produto_codigo          Nullable(String),
    nome_produto            Nullable(String),
    atributo_id_pai         Nullable(Int32),
    timestamp               Nullable(DateTime64(3)),
    atributo_nome_pai       Nullable(String),
    valor                   Nullable(Float64),
    nome_limite_superior    Nullable(String),
    nome_limite_inferior    Nullable(String),
    valor_limite_superior   Nullable(Float64),
    valor_limite_inferior   Nullable(Float64),
    unidade_medida          Nullable(String),
    atributo_tipo           Nullable(String)
)
ENGINE = ReplacingMergeTree(load_dts)
ORDER BY (hk_business_id)
-- PARTITION BY tuple() (partição única): garante que TODAS as linhas de um mesmo
-- hk_business_id fiquem na mesma partição, para o ReplacingMergeTree deduplicar
-- corretamente mesmo em recargas ao longo do tempo. O merge fica global (stress máximo,
-- ideal para a POC). Se preferir particionar, use uma coluna de negócio ESTÁVEL por
-- chave (nunca load_dts) — ver eixo de tuning B4 no PLAN.md.
PARTITION BY tuple()
SETTINGS index_granularity = 8192;
