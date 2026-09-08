-- Prova o efeito do índice de 02_indices.sql sem depender da carga real.
-- Cria uma tabela-sonda com o mesmo shape e a mesma distribuição de filtro,
-- popula, e deixa o EXPLAIN comparável antes/depois do índice.

CREATE SCHEMA IF NOT EXISTS probe;

DROP TABLE IF EXISTS probe.fact_200_cep;
CREATE TABLE probe.fact_200_cep (
    andon_peso_id        BIGINT PRIMARY KEY,
    filial               TEXT,
    banco                TEXT,
    unidade_producao_id  BIGINT,
    "timestamp"          TIMESTAMP,
    valor                NUMERIC(18,4)
);

INSERT INTO probe.fact_200_cep
SELECT
    g,
    'filial_' || lpad(((g % 5) + 1)::text, 2, '0'),
    'banco_'  || lpad(((g % 12) + 1)::text, 2, '0'),
    (g % 40) + 1,
    TIMESTAMP '2026-01-01 00:00:00' + ((g % 250) || ' days')::interval,
    (g % 1000)::numeric / 10
FROM generate_series(1, 300000) AS g;

ANALYZE probe.fact_200_cep;
