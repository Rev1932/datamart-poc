SELECT
    tab.material,
    tab.plant,
    tab.stgeLoc,
    tab.lote,
    dm.descricao,
    tab.movimento_sap,
    tab.unidade_medida,
    tab.nr_ordem_producao,
    tab.filial,
    tab.banco,
    sum(tab.quantidade) AS soma_quantidade,
    max(tab.data_sap) AS max_data_sap,
    count(*) AS qtde_movimentos
FROM
(
    SELECT
        get_json_object(gm_json.texto, '$.material') AS material,
        get_json_object(gm_json.texto, '$.plant') AS plant,
        get_json_object(gm_json.texto, '$.stgeLoc') AS stgeLoc,
        get_json_object(gm_json.texto, '$.batch') AS lote,
        get_json_object(gm_json.texto, '$.moveType') AS movimento_sap,
        CAST(get_json_object(gm_json.texto, '$.entryQnt') AS DOUBLE) AS quantidade,
        get_json_object(gm_json.texto, '$.entryUom') AS unidade_medida,
        q.data_criacao AS data_sap,
        op.nr_ordem_producao,
        q.unidade_origem AS filial,
        q.dataset_origem AS banco
    FROM delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_unipac_sap_queue` q
    INNER JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_ordem_producao` op
        ON op.id = q.ordem_producao_id
        AND op.unidade_origem = q.unidade_origem
        AND op.dataset_origem = q.dataset_origem
    LATERAL VIEW explode(
        from_json(
            get_json_object(q.body, '$.goodsmovements'),
            'ARRAY<STRING>'
        )
    ) gm_json AS texto
) AS tab
INNER JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_material` dm
    ON dm.codigo = tab.material
    AND dm.unidade_origem = tab.filial
    AND dm.dataset_origem = tab.banco
GROUP BY
    tab.material,
    tab.plant,
    tab.stgeLoc,
    tab.lote,
    dm.descricao,
    tab.movimento_sap,
    tab.unidade_medida,
    tab.nr_ordem_producao,
    tab.filial,
    tab.banco
