SELECT *
FROM vw_cep
WHERE filial = {filial:String} AND banco = {banco:String}
  AND `timestamp` >= {sem_ini:DateTime64(3)} AND `timestamp` < {sem_fim:DateTime64(3)}
