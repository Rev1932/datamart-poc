-- View fina do painel CEP, a mesma de 00_views.pg.sql. {{DB}} é trocado pelo database do tenant.
CREATE OR REPLACE VIEW {{DB}}.vw_cep AS
SELECT filial, banco, unidade_producao_id, unidade_producao_nome, produto_id, produto_codigo,
       nome_produto, nr_ordem_producao, atributo_nome_pai, atributo_tipo, `timestamp`, valor,
       valor_limite_inferior, valor_limite_superior, unidade_medida
FROM {{DB}}.fact_200_cep;
