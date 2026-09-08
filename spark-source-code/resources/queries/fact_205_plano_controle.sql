SELECT
    o.id AS onboarding_id,
    o.status AS onboarding_status,
    o.evento_tipo,
    o.evento_referencia,
    up_orig.codigo AS unidade_producao_origem,
    up_dest.codigo AS unidade_producao_destino,
    plano.codigo AS plano_codigo,
    plano.nome AS plano_nome,
    item_ent.codigo AS item_id,
    item_ent.nome AS item_nome,
    a.id AS apontamento_id,
    a.resposta,
    a.observacao,
    a.frequencia,
    a.justificativa,
    a.meta_dado,
    a.ordem_producao_id,
    c.codigo AS colaborador_codigo,
    c.nome AS colaborador_nome,
    ct.codigo AS tipo_apontamento,
    CAST(a.data_criacao AS TIMESTAMP) AS apontamento_data_criacao,
    CAST(o.data_evento AS TIMESTAMP) AS data_evento,
    CAST(o.data_conclusao AS TIMESTAMP) AS onboarding_conclusao,
    unix_timestamp(CAST(o.data_conclusao AS TIMESTAMP))
    - unix_timestamp(CAST(o.data_evento AS TIMESTAMP)) AS duracao_segundos
FROM delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_checklist_onboarding` o
INNER JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_unidade_producao` up_orig
    ON up_orig.id = o.unidade_producao_origem_id
INNER JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_unidade_producao` up_dest
    ON up_dest.id = o.unidade_producao_destino_id
INNER JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_entidade` plano
    ON plano.id = o.entidade_id
LEFT JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_checklist_apontamento` a
    ON a.onboarding_id = o.id
LEFT JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_entidade` item_ent
    ON item_ent.id = a.entidade_id
LEFT JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_colaborador` c
    ON c.id = a.colaborador_id
LEFT JOIN delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_checklist_tipo` ct
    ON ct.id = a.tipo_id
WHERE ct.codigo IN (
    'PLANO_CONTROLE'
)
