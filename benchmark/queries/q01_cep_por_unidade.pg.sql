SELECT unidade_producao_id, atributo_nome_pai,
       count(*)                          AS n,
       round(avg(valor), 3)              AS media,
       round(stddev_samp(valor), 3)      AS desvio,
       min(valor)                        AS minimo,
       max(valor)                        AS maximo,
       round(100.0 * count(*) FILTER (WHERE valor > valor_limite_superior OR valor < valor_limite_inferior)
             / count(*), 3)              AS pct_fora
FROM fact_200_cep
WHERE filial = :'filial' AND banco = :'banco'
  AND "timestamp" >= :'mes_ini' AND "timestamp" < :'mes_fim'
GROUP BY unidade_producao_id, atributo_nome_pai
ORDER BY unidade_producao_id, atributo_nome_pai COLLATE "C";
