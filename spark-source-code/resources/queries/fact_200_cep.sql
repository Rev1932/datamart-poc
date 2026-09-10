SELECT
    dap.unidade_origem AS filial,
    dap.dataset_origem AS banco,
    dap.id AS andon_peso_id,
    dop.id AS ordem_producao_id,
    dap.nr_ordem_producao AS nr_ordem_producao,
    dup.id AS unidade_producao_id,
    dap.unidade_producao_nome AS unidade_producao_nome,
    0 AS entidade_id,
    '' AS nome_entidade,
    dop.produto_id AS produto_id,
    dma.codigo AS produto_codigo,
    dap.material AS nome_produto,
    0 AS atributo_id_pai,
    to_timestamp(
        substring(dap.data_hora, 1, 23),
        'yyyy-MM-dd HH:mm:ss.SSS'
    ) AS timestamp,
    'Peso SET' AS atributo_nome_pai,
    CAST(dap.real AS DECIMAL(9,3)) AS valor,
    'Peso MAX' AS nome_limite_superior,
    'Peso MIN' AS nome_limite_inferior,
    CAST(dap.limite_superior AS DECIMAL(9,3)) AS valor_limite_superior,
    CAST(dap.limite_inferior AS DECIMAL(9,3)) AS valor_limite_inferior,
    '' AS unidade_medida,
    '' AS atributo_tipo
FROM delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_andon_peso` dap
INNER JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_ordem_producao` dop
    ON dop.nr_ordem_producao = dap.nr_ordem_producao
    AND dop.unidade_origem = dap.unidade_origem
    AND dop.dataset_origem = dap.dataset_origem
INNER JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_unidade_producao` dup
    ON dap.unidade_producao_nome = dup.codigo
    AND dap.unidade_origem = dup.unidade_origem
    AND dap.dataset_origem = dup.dataset_origem
INNER JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_material` dma
    ON dma.id = dop.produto_id
    AND dma.unidade_origem = dop.unidade_origem
    AND dma.dataset_origem = dop.dataset_origem
WHERE
    to_timestamp(
        substring(dap.data_hora, 1, 23),
        'yyyy-MM-dd HH:mm:ss.SSS'
    ) >= to_timestamp('JANELA_INICIO')
    AND to_timestamp(
        substring(dap.data_hora, 1, 23),
        'yyyy-MM-dd HH:mm:ss.SSS'
    ) < to_timestamp('JANELA_FIM')
    AND CAST(dap.real AS DECIMAL(9,3)) <> 0
