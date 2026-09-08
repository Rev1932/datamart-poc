-- Métricas de merge do ReplacingMergeTree para datamart.fact_200_cep.

-- Parts ativas: quanto mais parts pequenas, mais pressão de merge.
SELECT
    count()                         AS parts_ativas,
    sum(rows)                       AS linhas,
    formatReadableSize(sum(bytes_on_disk)) AS tamanho,
    max(level)                      AS max_merge_level
FROM system.parts
WHERE database = 'datamart' AND table = 'fact_200_cep' AND active;

-- Merges em andamento (throughput/duração indicam gargalo).
SELECT
    elapsed,
    progress,
    num_parts,
    formatReadableSize(total_size_bytes_compressed) AS tamanho,
    result_part_name
FROM system.merges
WHERE database = 'datamart' AND table = 'fact_200_cep';

-- Histórico recente de merges (duração média, bytes processados).
SELECT
    event_type,
    count()                                  AS n,
    round(avg(duration_ms))                  AS dur_media_ms,
    formatReadableSize(sum(size_in_bytes))   AS bytes
FROM system.part_log
WHERE database = 'datamart' AND table = 'fact_200_cep'
  AND event_time > now() - INTERVAL 1 HOUR
GROUP BY event_type
ORDER BY event_type;

-- Dedup: total físico vs chaves únicas (a diferença é o que o merge ainda vai colapsar).
SELECT
    count()                  AS total_fisico,
    uniqExact(hk_business_id) AS chaves_unicas
FROM datamart.fact_200_cep;
