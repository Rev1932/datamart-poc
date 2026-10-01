SELECT "timestamp"::date AS dia, atributo_nome_pai,
       count(*)             AS n,
       round(avg(valor), 3) AS media,
       min(valor)           AS minimo,
       max(valor)           AS maximo
FROM fact_200_cep
WHERE filial = :'filial' AND banco = :'banco'
  AND "timestamp" >= :'tri_ini' AND "timestamp" < :'tri_fim'
GROUP BY 1, 2
ORDER BY dia, atributo_nome_pai COLLATE "C";
