SELECT unidade_producao_nome, atributo_nome_pai, toDate(`timestamp`) AS dia,
       count()              AS n,
       round(avg(valor), 3) AS media,
       min(valor)           AS minimo,
       max(valor)           AS maximo,
       round(100 * countIf(valor > valor_limite_superior OR valor < valor_limite_inferior)
             / count(), 3)  AS pct_fora
FROM vw_cep
WHERE filial = {filial:String} AND banco = {banco:String}
  AND `timestamp` >= {sem_ini:DateTime64(3)} AND `timestamp` < {sem_fim:DateTime64(3)}
GROUP BY unidade_producao_nome, atributo_nome_pai, dia
ORDER BY dia, unidade_producao_nome, atributo_nome_pai
LIMIT 500
