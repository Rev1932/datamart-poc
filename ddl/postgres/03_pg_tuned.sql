-- Terceiro braço da comparação: o teto do Postgres bem ajustado, no schema gold_tuned.
-- Responde "então é só arrumar o Postgres?" com número, em vez de opinião.
--
-- Aplicar DEPOIS da carga e de 02_indices.sql:
--   psql -f ddl/postgres/03_pg_tuned.sql
--   psql -v origem=probe.fact_200_cep -f ddl/postgres/03_pg_tuned.sql   (para sondar)

\set ON_ERROR_STOP on
\if :{?origem}
\else
  \set origem public.fact_200_cep
\endif

\echo 'origem:' :origem

SELECT count(*) = 0 AS origem_vazia FROM :origem \gset
\if :origem_vazia
\warn 'ERRO: a origem nao tem linha nenhuma. Carregue o dado antes.'
\quit
\endif

CREATE SCHEMA IF NOT EXISTS gold_tuned;

DROP TABLE IF EXISTS gold_tuned.fact_200_cep;

-- Sem PRIMARY KEY: uma tabela particionada exige que a PK contenha a chave de partição,
-- e esta é alvo de leitura, nunca de escrita concorrente.
CREATE TABLE gold_tuned.fact_200_cep (
    LIKE :origem INCLUDING DEFAULTS
) PARTITION BY RANGE ("timestamp");

-- Índice do painel, o MESMO de 02_indices.sql. Sem ele o braço tunado perderia para o
-- braço simples, e a comparação viraria um espantalho ao contrário.
CREATE INDEX ix_tuned_dash
    ON gold_tuned.fact_200_cep (filial, banco, unidade_producao_id, "timestamp");

-- BRIN sobre a coluna de particionamento. Só rende porque o INSERT abaixo grava em ordem
-- de timestamp: BRIN guarda min/max por faixa de blocos e é inútil em tabela embaralhada.
CREATE INDEX ix_tuned_brin
    ON gold_tuned.fact_200_cep USING brin ("timestamp") WITH (pages_per_range = 32);

-- Uma partição por mês presente na origem. \gexec: o psql não substitui :origem dentro
-- de um bloco DO, então a geração fica em SQL puro.
WITH lim AS (
    SELECT date_trunc('month', min("timestamp")) AS ini,
           date_trunc('month', max("timestamp")) AS fim
    FROM :origem
)
SELECT format(
    'CREATE TABLE gold_tuned.%I PARTITION OF gold_tuned.fact_200_cep FOR VALUES FROM (%L) TO (%L)',
    'fact_200_cep_' || to_char(m, 'YYYYMM'), m, m + interval '1 month')
FROM lim, generate_series(lim.ini, lim.fim, interval '1 month') AS m
\gexec

-- Recolhe o que cair fora da janela coberta, em vez de recusar o INSERT.
CREATE TABLE gold_tuned.fact_200_cep_default
    PARTITION OF gold_tuned.fact_200_cep DEFAULT;

-- ORDER BY não é cosmético: é o que dá correlação física ao BRIN. Sem ele o índice
-- existe, é consultado, e não descarta bloco nenhum.
INSERT INTO gold_tuned.fact_200_cep
SELECT * FROM :origem ORDER BY "timestamp";

ANALYZE gold_tuned.fact_200_cep;

SELECT count(*) AS particoes FROM pg_inherits
WHERE inhparent = 'gold_tuned.fact_200_cep'::regclass;
