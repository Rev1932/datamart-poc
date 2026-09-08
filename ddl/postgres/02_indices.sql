-- Índice do dashboard. Corrige o defeito D3: o honeycomb cria a tabela com um
-- índice só, o da PK (andon_peso_id), que NÃO é o filtro do painel.
-- Aplicar DEPOIS da carga — construir o índice antes torna cada INSERT mais caro
-- e o resultado é o mesmo.

CREATE INDEX IF NOT EXISTS ix_fact_200_cep_dash
    ON public.fact_200_cep (filial, banco, unidade_producao_id, "timestamp");

ANALYZE public.fact_200_cep;
