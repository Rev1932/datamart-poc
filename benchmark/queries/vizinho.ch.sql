SELECT produto_id, atributo_nome_pai, unidade_producao_id,
       count(), avg(valor), quantile(0.99)(valor), uniq(nr_ordem_producao)
FROM fact_200_cep
GROUP BY produto_id, atributo_nome_pai, unidade_producao_id
ORDER BY count() DESC
LIMIT 100
