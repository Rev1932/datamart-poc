SELECT produto_id,
       count(*) FILTER (WHERE valor > valor_limite_superior OR valor < valor_limite_inferior) AS n_fora,
       count(*) AS n
FROM fact_200_cep
WHERE "timestamp" >= :'tri_ini' AND "timestamp" < :'tri_fim'
GROUP BY produto_id
ORDER BY n_fora DESC, produto_id
LIMIT 20;
