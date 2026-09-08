-- Template de RBAC por tenant. Renderizado por infra/clickhouse/job-rbac.yaml
-- substituindo {{TENANT}}, {{PWD_RO}} e {{PWD_LD}}. Idempotente.

CREATE DATABASE IF NOT EXISTS dm_{{TENANT}};

CREATE ROLE IF NOT EXISTS r_{{TENANT}}_ro;
CREATE ROLE IF NOT EXISTS r_{{TENANT}}_loader;

-- readonly = 2 e não 1: o valor 1 proíbe SET e quebra clientes que emitem settings
-- de sessão. join_use_nulls = 1 impede LEFT JOIN preencher com o default do tipo.
CREATE SETTINGS PROFILE IF NOT EXISTS p_{{TENANT}}_ro SETTINGS
    readonly = 2,
    join_use_nulls = 1,
    max_memory_usage = 700000000,
    max_execution_time = 30,
    max_result_rows = 200000;

CREATE QUOTA IF NOT EXISTS q_{{TENANT}}
    KEYED BY user_name
    FOR INTERVAL 1 MINUTE
        MAX queries = 120,
            errors = 20,
            result_rows = 5000000,
            read_rows = 500000000,
            execution_time = 60
    TO r_{{TENANT}}_ro;

GRANT SELECT ON dm_{{TENANT}}.* TO r_{{TENANT}}_ro;

-- ALTER MOVE PARTITION cobre o REPLACE PARTITION; ALTER DELETE é exigido do lado
-- de origem da troca. Faltando um, o erro nomeia o privilégio ausente.
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER MOVE PARTITION, ALTER DELETE
    ON dm_{{TENANT}}.* TO r_{{TENANT}}_loader;

-- Tabelas de sistema que o connector Spark le antes de qualquer query, no
-- ClickHouseCatalog.initialize e na inferencia de schema. So o connector as toca: pelo
-- clickhouse-client o loader nunca precisa delas, e a falta aparece como Code 497.
-- NAO conceder system.* inteiro: system.query_log nao tem filtro por permissao e
-- exporia as queries dos outros tenants. As tres ultimas ja sao filtradas por acesso.
GRANT SELECT ON system.clusters  TO r_{{TENANT}}_loader;
GRANT SELECT ON system.macros    TO r_{{TENANT}}_loader;
GRANT SELECT ON system.databases TO r_{{TENANT}}_loader;
GRANT SELECT ON system.tables    TO r_{{TENANT}}_loader;
GRANT SELECT ON system.columns   TO r_{{TENANT}}_loader;
GRANT SELECT ON system.parts     TO r_{{TENANT}}_loader;

CREATE USER IF NOT EXISTS u_{{TENANT}}_ro
    IDENTIFIED WITH sha256_password BY '{{PWD_RO}}'
    SETTINGS PROFILE 'p_{{TENANT}}_ro';
GRANT r_{{TENANT}}_ro TO u_{{TENANT}}_ro;
ALTER USER u_{{TENANT}}_ro DEFAULT ROLE r_{{TENANT}}_ro;

CREATE USER IF NOT EXISTS u_{{TENANT}}_loader
    IDENTIFIED WITH sha256_password BY '{{PWD_LD}}';
GRANT r_{{TENANT}}_loader TO u_{{TENANT}}_loader;
ALTER USER u_{{TENANT}}_loader DEFAULT ROLE r_{{TENANT}}_loader;
