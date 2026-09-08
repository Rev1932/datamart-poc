-- Um database por tenant, espelhando dm_<tenant> do ClickHouse: as duas pontas da
-- comparação precisam ter a mesma granularidade de isolamento.
-- CREATE DATABASE não aceita IF NOT EXISTS nem roda em transação — daí o \gexec.

SELECT format('CREATE DATABASE %I OWNER %I', d, current_user)
FROM (VALUES ('dm_acme'), ('dm_globex')) AS t(d)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = d)
\gexec
