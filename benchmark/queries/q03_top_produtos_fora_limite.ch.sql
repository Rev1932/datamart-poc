SELECT produto_id,
       countIf(valor > valor_limite_superior OR valor < valor_limite_inferior) AS n_fora,
       count() AS n
FROM fact_200_cep
WHERE `timestamp` >= {tri_ini:DateTime64(3)} AND `timestamp` < {tri_fim:DateTime64(3)}
GROUP BY produto_id
ORDER BY n_fora DESC, produto_id
LIMIT 20
