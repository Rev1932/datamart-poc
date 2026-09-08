# Seed do control plane

Um arquivo por coleção de `Data_Catalog`, aplicado por `infra/mongodb/job-seed.yaml`.
As DAGs leem sempre o documento mais recente (`find_one(sort=[("_id", -1)])`), então
reaplicar o Job cria uma nova versão em vez de sobrescrever — o histórico fica.

| Coleção | Lida por | Campos obrigatórios |
|---|---|---|
| `k8s_<tenant>` | DAG `bronze_silver` | `filiais[]`, `tables[{name, chave_pk[]}]`, `honeycomb_version` |
| `k8s_<tenant>_gold` | DAG `gold_datamart` | `tables[{name, chave_pk[], gold_type}]`, `honeycomb_version` |

**`filiais` precisa bater com os diretórios** sob `s3a://datamart/data-bee_replication/data-bee_<filial>/`.
Não há validação cruzada: um nome errado faz o honeycomb não achar arquivo, logar "nenhum
arquivo" e **concluir com SUCESSO**. É o que `scripts/load-bronze.sh --validate` (E2/T2.5) cobre.

`honeycomb_version` é validada por `^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$` e vira a tag da imagem.

## `gold_type` e a divergência da POC

Em produção os valores válidos são `dimension`, `fact_delta` e `fact_postgres`, e a DAG de gold
**exige ao menos uma tabela de cada** — um grupo vazio significaria uma etapa que roda verde sem
materializar nada. Há ainda a amarra por prefixo: `dim_` só aceita `dimension`, `fact_` aceita
`fact_delta` ou `fact_postgres`.

A POC usa só `fact_postgres`: as DAGs de gold daqui (E2/T2.6) são de destino único — uma para o
Postgres, outra para o ClickHouse — e não têm o roteamento de três vias. **T2.6 precisa remover a
validação de grupo vazio ao copiar a DAG de produção**, ou ela falha antes de subir qualquer pod.
