-- Leitor do benchmark: só SELECT no database do tenant, sem cota e sem os limites de resultado,
-- tempo e memória do p_<tenant>_ro, que o Postgres não tem. {{SENHA}} vem do Secret ch-bench.
CREATE SETTINGS PROFILE IF NOT EXISTS p_bench SETTINGS readonly = 2;
CREATE USER IF NOT EXISTS u_{{TENANT}}_bench IDENTIFIED WITH sha256_password BY '{{SENHA}}' SETTINGS PROFILE 'p_bench';
GRANT SELECT ON {{DB}}.* TO u_{{TENANT}}_bench;
