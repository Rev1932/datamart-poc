# Pendências para execução em Kubernetes — `spark_into_postgres`

> **Escopo:** impeditivos e requisitos para rodar o projeto como job efêmero no K8s
> (config via ConfigMap/Secret em variáveis de ambiente, sem `.conf` versionado).
> **Última atualização:** 2026-07-20 · branch `feat/spark_postgres` (base `0cc09a4`).
> Documento de histórico — itens são atualizados conforme resolvidos.

Legenda: ✅ resolvido · 🔴 impede execução · 🟠 falta artefato de deploy · 🟡 contrato do
manifesto · ⚠️ risco menor.

Ordem real de falha num pod: `import main` → `import pipeline_factory` → `initialize_logger()`
(import-time) → … → `main()`. Bloqueadores de import vêm antes de tudo.

---

## ✅ Resolvidos

### B1 — Logger ClickHouse ausente do schema (crash na importação)
`initialize_logger()` é chamado em import-time (`main.py:13`, `core/pipeline_factory.py:9`,
`tranformer/transformer.py:7`) e constrói o `ClickHouseAsyncHandler`, que lê `clickhouse.*`. O
`ConfigManager` novo (env-only) não tinha essas chaves → retornava `None` → crash.
**Ação:** incluídas no `_ENV_SCHEMA` (`utils/config_manager.py`) as chaves
`CLICKHOUSE_URL/USERNAME/PASSWORD/SERVICE_NAME/VERIFY_SSL`, com coerção dedicada
`_coerce_verify_ssl` (aceita bool OU nome de certificado, resolvido contra `resources/`).
Atualizado `resources/.env.example`. Testes: contrato ClickHouse e coerção dual do `verify_ssl`.

### B2 — Campos inexistentes em `PipelineConfig` (`.topic`/`.config_name`)
`main.py` e `handler_logger.apply_and_trace_context` acessavam atributos que não existem em
`PipelineConfig` → `AttributeError` em toda execução.
**Ação:** `main.py:46` passou a compor o app-name com `filial_name`+`table_name`;
`handler_logger.py:171-172` usa `filial_name`/`table_name`/`tenant_name`. Sem nenhum acesso
`.topic`/`.config_name`/`.client_name` restante (verificado por grep + reprodução).

### I1 — `clickhouse.url` como hard-requirement (resíduo do B1)
Mesmo com as chaves no schema, `ClickHouseAsyncHandler.__init__` fazia
`config.get("clickhouse.url").rstrip('/')` sem guarda: se o manifesto **não** injetasse
`CLICKHOUSE_URL`, voltava a estourar `AttributeError` já na importação, derrubando o job antes
do `main()`.
**Ação (`utils/handler_logger.py`):**
- `initialize_logger()` só monta o `ClickHouseAsyncHandler` quando `clickhouse.url` está
  configurada; caso contrário emite `warning` e segue **só com o console** (`StreamHandler`).
  Um sink de log ausente não derruba mais o job efêmero. Cobre também `CLICKHOUSE_URL=""`.
- Guard defensivo no `__init__`: sem URL, lança `ValueError` claro (em vez de `AttributeError`)
  para uso indevido fora do factory.
- **Nota:** falha de *runtime* do ClickHouse já era tolerada (`_send_log` captura exceção); o
  fix trata só o caso de *construção* sem config.
- Teste novo `tests/unit/test_handler_logger.py` (4 casos): URL ausente → degrada; URL vazia →
  degrada; URL presente → handler ativo; construção direta sem URL → `ValueError`.
- **Verificado:** repro do pod sem `CLICKHOUSE_URL` completa sem exceção (`['StreamHandler']`);
  suíte relevante 56 passed.

### I2 — `config_name`/`pipeline_type` via subscrição (afetava só `bronze_silver`)
`core/pipeline_orchestrator.py:63` e `repo/repository.py:206` faziam
`runtime_parameters['config_name']` (e `['pipeline_type']`). Como `PipelineConfig.__getitem__`
retorna `None` para chave inexistente (não estoura), gerava caminho errado `data-bee_None` no
input do bronze→silver. Não afetava `gold_datamart`/`gold`.
**Ação:** trocado para campos reais de `PipelineConfig` —
`pipeline_orchestrator.py:63` usa `["filial_name"]`/`["pipeline"]`; `repository.py:206` usa
`f"data-bee_{runtime_parameters['filial_name']}"`.
**Verificado:** com `PipelineConfig` real, `unit = data-bee_limeira` e
`input_path = …/data-bee_replication/data-bee_limeira/fact_200_cep` (sem `None`).
**Pendência residual (cosmética):** a docstring em `tranformer/transformer.py:109` ainda cita
`config_name` — só comentário desatualizado, não afeta execução.

---

## 🟠 Empacotamento/deploy — inexistentes no projeto

Confirmado: **não há** Dockerfile, requirements/pyproject, manifesto nem script de submit no
projeto (o único `/pyproject.toml` é a cópia do contexto do container vazada na raiz do host).

- **I3 — Imagem/Dockerfile:** base Spark 3.5 + deps Python (`pyhocon`, `requests`, `minio`,
  `trino`, `psycopg2-binary`, `pyspark`, `delta-spark`) + JARs no classpath: `hadoop-aws`/S3A,
  `delta`, e o **driver JDBC `org.postgresql`** — obrigatório p/ `repository.py:167`
  (`.write.format("jdbc")`).
- **I4 — Declaração de dependências versionada** no projeto (requirements/pyproject).
- **I5 — Manifesto K8s** (`SparkApplication`/`Job`): `image`, entrypoint (`spark-submit main.py`
  + args), `envFrom: [configMapRef, secretRef]`, `restartPolicy: Never`, e (cluster-mode)
  `serviceAccount` + RBAC.

---

## 🟡 Contrato que o manifesto precisa cumprir

- **I6 — Env vars do `_ENV_SCHEMA` por pipeline.** `gold_datamart`: `MINIO_BASE_PATH` +
  `POSTGRES_*` + `CLICKHOUSE_*`. **Não** usa `TRINO_*` (`pipeline_factory.py:41-43`).
  `bronze_silver`/`gold`: adicionam `TRINO_*` e `SPARK_S3_*`.
- **I7 — Credenciais S3A do Spark:** `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  (`session.py:31-33`, `EnvironmentVariableCredentialsProvider`). Distintas de
  `SPARK_S3_ACCESS_KEY/SECRET_KEY` (essas alimentam só o cliente `minio-py` do bronze_silver).
  Mesmo `gold_datamart` lê Delta do S3 → precisa das creds AWS.
- **I8 — Master do Spark:** `session.py` não define `.master()`; vem do `spark-submit`
  (`--master k8s://…` ou `local[*]`). Definir o modo do pod.
- **I9 — Args de CLI obrigatórios:** `--tenant_name --filial_name --table_name --primary_key`
  (+ `--pipeline`), passados pelo entrypoint/manifesto (`main.py:20-31`).

---

## 🗂️ Assets no filesystem da imagem

- **I10 — `resources/` como diretório-irmão de `src/`** (auto-discovery sobe de `__file__`).
  Deve conter `queries/<table>.sql` (**só `fact_200_cep.sql` existe hoje**) e
  `wildcard_datadriven_cloud.crt` (usado no `verify_ssl` do ClickHouse).
- **I11 — Disco writable** para spill/shuffle do Spark (`emptyDir` em `spark.local.dir`/`/tmp`).

---

## ✅ Adequado para efêmero (não é impeditivo)

- **Stateless** entre restarts: lê S3/Delta, escreve Postgres/Delta/S3; staging Postgres é
  dropada (`repository.py:161-183`).
- **Exit code:** `main()` re-lança exceção (`main.py:70-71`) → exit ≠ 0 → pod `Failed`.
- **Shutdown:** `spark.stop()` no `finally`.
- **ConfigManager:** contrato preservado, sem I/O de arquivo, coerções ok.

## ⚠️ Riscos menores
- Logs ClickHouse assíncronos (`ThreadPoolExecutor`) **não** são flushados no shutdown
  (`handler.close()` nunca é chamado) → últimos logs podem se perder quando o pod morre.
- `obter_ip_local()` conecta a `8.8.8.8` — falha tolerada em pod sem egress.

---

## Prioridade dos itens abertos

| Para… | Mínimo necessário |
|---|---|
| **`gold_datamart` correto** | I6 (env), I7 (creds S3A), I10 (query `.sql`) |
| **Empacotar/subir** | I3, I4, I5, I8, I9, I11 |
