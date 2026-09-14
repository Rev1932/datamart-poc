-- View fina do painel CEP, no padrão das views gold de produção: colunas nomeadas, sem filtro.
CREATE OR REPLACE VIEW public.vw_cep AS
SELECT filial, banco, unidade_producao_id, unidade_producao_nome, produto_id, produto_codigo,
       nome_produto, nr_ordem_producao, atributo_nome_pai, atributo_tipo, "timestamp", valor,
       valor_limite_inferior, valor_limite_superior, unidade_medida
FROM public.fact_200_cep;

CREATE OR REPLACE VIEW gold_tuned.vw_cep AS
SELECT filial, banco, unidade_producao_id, unidade_producao_nome, produto_id, produto_codigo,
       nome_produto, nr_ordem_producao, atributo_nome_pai, atributo_tipo, "timestamp", valor,
       valor_limite_inferior, valor_limite_superior, unidade_medida
FROM gold_tuned.fact_200_cep;
