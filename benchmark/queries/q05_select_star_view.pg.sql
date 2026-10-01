SELECT *
FROM vw_cep
WHERE filial = :'filial' AND banco = :'banco'
  AND "timestamp" >= :'sem_ini' AND "timestamp" < :'sem_fim';
