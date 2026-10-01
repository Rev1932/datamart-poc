SELECT unidade_producao_id, atributo_nome_pai,
       count()                           AS n,
       round(avg(valor), 3)              AS media,
       round(stddevSamp(valor), 3)       AS desvio,
       min(valor)                        AS minimo,
       max(valor)                        AS maximo,
       round(100 * countIf(valor > valor_limite_superior OR valor < valor_limite_inferior)
             / count(), 3)               AS pct_fora
FROM fact_200_cep
WHERE filial = {filial:String} AND banco = {banco:String}
  AND `timestamp` >= {mes_ini:DateTime64(3)} AND `timestamp` < {mes_fim:DateTime64(3)}
GROUP BY unidade_producao_id, atributo_nome_pai
ORDER BY unidade_producao_id, atributo_nome_pai
