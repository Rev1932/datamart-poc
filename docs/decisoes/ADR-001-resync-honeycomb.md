# ADR-001 — Re-sincronizar o fork inteiro com `honeycomb@main`

| Campo | Valor |
|---|---|
| Status | Aceita |
| Data | 2026-09-04 |
| Task | [E2](../epicos/E2-execucao.md) T2.1 |

## Contexto

`datamart-poc/spark-source-code/` é um fork defasado do honeycomb. Seu `PipelineFactory` expõe
`{gold, datamart, oee_cleaner, bronze_silver}`, enquanto o honeycomb produtivo expõe
`{gold, gold_datamart, bronze_silver, silver_super_tenant, delta_maintenance, restore_backups}`. O CLI
também divergiu: o fork usa `--config-name/--table-name/--chave-pk`; o honeycomb usa
`--tenant_name/--filial_name/--tables_json/--max_workers/--primary_key`.

A POC precisa do braço Postgres, que já existe no honeycomb como `RepositoryGoldDatamart`.

## Alternativas

**(a) Portar só `RepositoryGoldDatamart` para o fork.** Parece menor.

**(b) Substituir `spark-source-code/` por uma cópia de `honeycomb@main` e aplicar o delta V2 por cima.**

## Decisão

**(b).**

## Justificativa

**A alternativa (a) é uma ilusão de escopo.** `RepositoryGoldDatamart` depende de:

| Dependência | Por quê |
|---|---|
| `utils/config_manager.py` | Usa `environment_parameters.get("postgres")`, que só existe no `ConfigManager` novo (env vars). O fork lê HOCON de arquivo |
| `utils/pipeline_config.py` | Usa `runtime_parameters.get("primary_key")`; o fork tem `topic`, não `filial_name` |
| `main.py` | O CLI precisa aceitar os argumentos novos |
| `core/pipeline_factory.py` | Chave `gold_datamart` |
| `core/pipeline_orchestrator.py` | `PipelineGoldDatamart` |
| `utils/postgres_utils.py` | Não existe no fork |

Isso é o re-sync inteiro — sem a honestidade de chamá-lo assim, e sem trazer junto `table_runner.py`,
`delta_maintenance`, `silver_super_tenant`, `restore_backups` e a suíte de testes, que é justamente o que
torna a POC "idêntica à produção".

**Três razões independentes fecham a decisão:**

1. **A DAG produtiva não roda contra o fork.** `k8s_unipac_bronze_silver.py` monta `spec.arguments` com
   `--tenant_name/--filial_name/--tables_json/--max_workers`. O fork não aceita nenhum. Reescrever o
   manifesto para o CLI antigo faria o braço Airflow deixar de provar qualquer coisa — e replicar a stack
   produtiva era o objetivo declarado.
2. **A mudança precisa virar PR upstream.** Um patch desenvolvido contra um `repository.py` com layout de
   classes diferente não aplica. Desenvolvendo contra um checkout de `honeycomb@main`, **o diff É o PR**.
3. **O fork não tem testes.** O honeycomb tem `tests/integration/test_repository_gold_datamart.py` com
   testcontainers, que é o molde exato do teste de integração ClickHouse.

## Consequências

- O único ativo do fork preservado é `utils/clickhouse_connection.py` — 30 linhas, sem dependência do
  código velho.
- O delta V2 usa o prefixo `DATAMART_CH_*`, não `CLICKHOUSE_*` (defeito D2), para não colidir com o
  handler de auditoria. `DATAMART_CH_DATABASE` por tenant é o análogo exato de `POSTGRES_DATABASE`, então
  a substituição `spark-TENANT-config` da DAG funciona sem alteração — e é isso que torna o patch
  aceitável upstream.
- Risco assumido: o re-sync pode quebrar algo que só o fork tinha. Mitigação: rodar `pytest -q`
  imediatamente após o rsync, antes de qualquer edição.

---

## Revisão de 2026-09-09 — a base é a tag `3.3.0`, não `main`

O re-sync foi executado contra **`3.3.0` (`a5f2fa4`, 2026-08-05)**, extraída com
`git archive`, sem alterar o repositório do honeycomb.

`main` foi descartado por medição, não por preferência. Em `b0d7472` a
`resources/queries/fact_200_cep.sql` foi reescrita como `UNION ALL` de dois ramos:

| | `3.3.0` | `main` (`b0d7472`) |
|---|---|---|
| Tabelas silver | 4 | 9 |
| Dependência de gold | nenhuma | `S3_PATH_GOLD/dim_limites` |
| Filtro temporal | `INTERVAL 10 DAYS` (o defeito **D1**) | **nenhum** — lê a silver inteira |
| Formato de `data_hora` | `.SSS`, 3 casas | `.SSSSSS`, 6 casas |
| Tipo de `valor` | `CAST(... AS DOUBLE)` | `decimal(18,4)` |

O dado real ingerido tem 4 tabelas e `data_hora` com 3 casas decimais. Contra `main`, a POC não roda:
faltam 5 tabelas silver e uma gold que ninguém materializou.

**Consequência para o PR upstream:** o patch nasce sobre `3.3.0` e vai precisar de rebase. As duas peças
novas — a janela em `queryutils` e o braço ClickHouse — não tocam nada que `main` reescreveu, exceto o
predicado de `fact_200_cep.sql`, que em `main` não existe mais. O rebase é de conflito conhecido, não de
surpresa.

Chaves da factory em `3.3.0`: `gold`, `gold_datamart`, `bronze_silver`, `silver_super_tenant`,
`delta_maintenance`. `datamart_ch` foi acrescentada por T2.3.
