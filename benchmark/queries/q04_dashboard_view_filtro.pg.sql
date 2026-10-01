SELECT unidade_producao_nome, atributo_nome_pai, "timestamp"::date AS dia,
       count(*)             AS n,
       round(avg(valor), 3) AS media,
       min(valor)           AS minimo,
       max(valor)           AS maximo,
       round(100.0 * count(*) FILTER (WHERE valor > valor_limite_superior OR valor < valor_limite_inferior)
             / count(*), 3) AS pct_fora
FROM vw_cep
WHERE filial = :'filial' AND banco = :'banco'
  AND "timestamp" >= :'sem_ini' AND "timestamp" < :'sem_fim'
GROUP BY unidade_producao_nome, atributo_nome_pai, dia
ORDER BY dia, unidade_producao_nome COLLATE "C", atributo_nome_pai COLLATE "C"
LIMIT 500;
