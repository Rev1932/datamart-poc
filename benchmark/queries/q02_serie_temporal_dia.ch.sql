SELECT toDate(`timestamp`) AS dia, atributo_nome_pai,
       count()              AS n,
       round(avg(valor), 3) AS media,
       min(valor)           AS minimo,
       max(valor)           AS maximo
FROM fact_200_cep
WHERE filial = {filial:String} AND banco = {banco:String}
  AND `timestamp` >= {tri_ini:DateTime64(3)} AND `timestamp` < {tri_fim:DateTime64(3)}
GROUP BY dia, atributo_nome_pai
ORDER BY dia, atributo_nome_pai
