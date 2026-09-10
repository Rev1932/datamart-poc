# Seed do control plane

Um arquivo por coleção de `Data_Catalog`, aplicado por `infra/mongodb/job-seed.yaml`.
As DAGs leem sempre o documento mais recente (`find_one(sort=[("_id", -1)])`), então
reaplicar o Job cria uma nova versão em vez de sobrescrever — o histórico fica.

| Coleção | Lida por | Campos obrigatórios |
|---|---|---|
| `k8s_<tenant>` | ninguém (contrato documental) | `tables[{name, chave_pk[]}]` — as tabelas **silver** que precisam existir |
| `k8s_<tenant>_gold` | DAG `k8s_<tenant>_datamart` (tasks PG e CH) | `tables[{name, chave_pk[], gold_type}]`, `honeycomb_version` |

**Mudou em [E2 v3.0](../../docs/epicos/E2-execucao.md):** com bronze → silver e a gold em Delta fora do
escopo, nenhuma DAG lê `k8s_<tenant>` — ele fica como declaração do contrato de entrada da silver.
`filiais[]` deixa de ser consumido, e `colunas_zorder` de `k8s_<tenant>_gold` também: ele só servia ao
`OPTIMIZE` do Delta gold. `chave_pk` **continua obrigatória** — é dela que sai o `hk_business_id`.

**Os nomes em `tables[]` precisam bater com os diretórios Delta** sob
`s3a://datamart/business_datavault_data-bee/<tabela>/`, ingeridos pelo usuário. Não há validação cruzada
em lugar nenhum: um nome errado só aparece como `Path does not exist` na primeira execução do braço.

**Resolvido em 2026-09-09** (decisão: a seed se adequa ao prefixo). Três correções, todas apuradas nos
predicados de join de `resources/queries/fact_200_cep.sql`:

| Campo | Antes | Agora | Por quê |
|---|---|---|---|
| `tables[].name` | `andon_peso`, … (3) | `dw_andon_peso`, … (4) | O prefixo é o contrato; `dw_material` faltava |
| `tables[].chave_pk` (silver) | `andon_peso_id` | `id`, `unidade_origem`, `dataset_origem` | `<tabela>_id` são **aliases da gold**, não colunas da silver. `dap.id`, `dop.id`, `dup.id`, `dma.id` são as colunas reais |
| `chave_pk` do gold | `["andon_peso_id"]` | `["filial", "banco", "andon_peso_id"]` | `dap.id` repete entre filiais e bancos. Ver abaixo |

> **A chave da gold precisa das três colunas.** `andon_peso_id` é `dap.id`, único dentro de uma origem —
> mas a fato agrega várias (`filiais[]` tem duas por tenant). Com a chave curta, duas filiais com o mesmo
> `id` produzem o mesmo `hk_business_id`, o `ReplacingMergeTree` colapsa o par e **uma das linhas some sem
> erro**. `filial` e `banco` existem na gold exatamente por isso.

**Aplicado ao cluster em 2026-09-10.** O Job re-executado insere uma nova versão de cada documento; as
antigas ficam, e as DAGs leem a mais recente. Conferido: os quatro documentos no topo de cada coleção já
trazem o prefixo `dw_` e a chave de três colunas.

`honeycomb_version` é validada por `^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$` e vira a tag da imagem.

## `gold_type` e a divergência da POC

Em produção os valores válidos são `dimension`, `fact_delta` e `fact_postgres`, e a DAG de gold
**exige ao menos uma tabela de cada** — um grupo vazio significaria uma etapa que roda verde sem
materializar nada. Há ainda a amarra por prefixo: `dim_` só aceita `dimension`, `fact_` aceita
`fact_delta` ou `fact_postgres`.

A POC usa só `fact_postgres`: as duas tasks de datamart daqui (E2/T2.6) são de destino único — uma para o
Postgres, outra para o ClickHouse — e não têm o roteamento de três vias. **T2.6 precisa remover a
validação de grupo vazio ao copiar a DAG de produção**, ou ela falha antes de subir qualquer pod.
