-- Vizinho da fase 4 da T3.3: limite de CPU e de concorrência, sem cota. Com cota, os erros de
-- concorrência gastariam a cota de erros e o bloqueio viria dela, não do limite testado.
CREATE SETTINGS PROFILE IF NOT EXISTS p_{{TENANT}}_limitado SETTINGS
    readonly = 2, max_threads = 1, max_concurrent_queries_for_user = 1,
    max_memory_usage = 700000000, max_execution_time = 30, max_result_rows = 200000;
CREATE USER IF NOT EXISTS u_{{TENANT}}_limitado IDENTIFIED WITH sha256_password BY '{{SENHA}}'
    SETTINGS PROFILE 'p_{{TENANT}}_limitado';
GRANT SELECT ON {{DB}}.* TO u_{{TENANT}}_limitado;
