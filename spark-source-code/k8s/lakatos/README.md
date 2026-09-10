# Super Tenant Lakatos — `silver_super_tenant`

Artefatos do modelo de **super tenant**: um fabricante que revende o sistema embarcado e precisa
de relatórios sobre as máquinas instaladas nos clientes dele.

## O modelo

O Honeycomb conhece hoje um único modelo: **um Tenant, N filiais**, um bucket por tenant, uma
Silver compartilhada entre as filiais e particionada por `source = data-bee_<filial>`. Quando não
há filiais, `--filial_name` é igual ao `--tenant_name`.

O super tenant é uma camada a mais, **paralela e não intrusiva**:

```
s3a://datawake-belafatia/business_datavault_data-bee/<tabela>  ─┐   (tenant intacto, segue no padrão)
s3a://datawake-clienteN/business_datavault_data-bee/<tabela>   ─┤
                                                                ├──► s3a://datawake-lakatos/
    reidentificação: cada CLIENTE vira uma FILIAL do Lakatos    ┘      business_datavault_data-bee/<tabela>
```

O dado **não é reprocessado** — já passou por transformação, quarentena e Data Vault no tenant de
origem. O que muda é a identidade:

| Coluna | O que acontece |
| :-- | :-- |
| `source` | vira `data-bee_<cliente>` — é a **partição** do destino |
| `unidade_origem` | vira `<cliente>` — é o que os `.sql` do gold leem como `filial` |
| `hk_business_id` | re-hash `sha2(hk_original + '_' + cliente)` |
| `filial_origem` / `tenant_origem` | **acrescentadas**: preservam a identidade que o overwrite apaga |
| `dataset_origem`, `load_dts`, `source_file` | **preservados** |

O re-hash usa o hash **original**, que já distingue as filiais da origem: dois clientes com uma
filial homônima (`matriz` nos dois) deixam de colidir, e as filiais de um mesmo cliente seguem
como linhas distintas. Ancorado em `tests/integration/test_silver_super_tenant.py`.

Como a Silver resultante tem a mesma forma de qualquer outra (mesma pasta, mesma partição, CDF
ligado), **a camada gold funciona sem alteração de código** e o `delta_maintenance` também.

## Arquivos

| Arquivo | Papel |
| :-- | :-- |
| `configmap.yaml` | `spark-lakatos-config` — env não-secreto. **`TRINO_SCHEMA` precisa ser exclusivo.** |
| `secret.example.yaml` | Contrato do `spark-lakatos-secret` (não versionar valores reais). |
| `sparkapplication.yaml` | Template do job, modo multi-tabela. |

## Ordem de aplicação

```bash
kubectl apply -f configmap.yaml
kubectl apply -f secret.example.yaml     # substitua por Secret real (OpenBao/External Secrets)
# SparkApplication é aplicado pela DAG (SparkKubernetesOperator), não manualmente.
```

## ⚠️ Pré-requisito de infra — BLOQUEIA a execução real

Este é o **único** pipeline que lê buckets de outros tenants. O usuário S3A do `spark-secrets`
(`dw-app-spark-k8s`) precisa de:

- `s3:GetObject` + `s3:ListBucket` em **todo** bucket de cliente do Lakatos (`datawake-belafatia`, …);
- RW em `datawake-lakatos`.

`../README.md` já registra que a policy desse usuário é restrita (só RW em `hive-warehouse`) e que
isso bloqueia a execução real — este pipeline **amplia** o escopo desse bloqueio, não o cria.

Duas opções para quem administra o MinIO:

1. **Ampliar a policy do `dw-app-spark-k8s`** com read-only nos buckets dos clientes. Simples,
   mas acopla cada cliente novo a uma mudança de policy, e amplia o acesso de um usuário
   compartilhado por todos os pipelines.
2. **Usuário dedicado** (ex.: `dw-app-lakatos-consolidacao`) com policy própria, entregue no
   `spark-lakatos-secret`, que sobrescreve `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` no
   `envFrom` (o último vence — por isso a ordem no manifesto). **Recomendada:** menor privilégio,
   sem tocar no usuário compartilhado.

**Sintoma se faltar permissão:** o job falha na inspeção das origens, nomeando o tenant e o path.
É deliberado — sem essa guarda, um 403 seria indistinguível de "esse cliente não tem essa tabela"
(estado legítimo, que é pulado) e o job terminaria **verde sem ter consolidado nada**.

## Operação

**Ordem no Airflow.** A DAG do Lakatos depende da conclusão do `bronze_silver` de **todos** os
clientes de origem. Rodando antes, ela consolida dado velho — não corrompe nada (a consolidação é
idempotente), só atrasa. Use `ExternalTaskSensor` por cliente ou uma DAG mestre.

**Manutenção Delta.** Agende `delta_maintenance --delta_layer silver` do Lakatos com frequência
**maior** que a dos demais tenants: o merge aqui é insert **+ update**, e reescrever as linhas
correspondidas gera mais arquivos órfãos que o insert-only do `bronze_silver`.

**Remover um cliente do Lakatos.** Tirá-lo de `--source_tenants` **não** remove o dado dele: o
merge nunca deleta, e a partição fica congelada na última carga. A remoção é manual e consciente:

```sql
DELETE FROM delta.`s3a://datawake-lakatos/business_datavault_data-bee/<tabela>`
WHERE source = 'data-bee_<cliente>';
```

seguido do `delta_maintenance` para o VACUUM. A poda automática por ausência na lista **não** foi
implementada de propósito: um typo em `--source_tenants` apagaria um cliente inteiro.

**Gold do Lakatos — auditar antes do go-live.** O gold **não** usa o `hk_business_id` da Silver:
`RepositoryGoldDataVaults` recalcula o seu a partir do `--primary_key` da execução e depois faz
`dropDuplicates`. Se a `chave_pk` de alguma tabela gold do Lakatos **não** incluir a coluna de
filial (`unidade_origem`, apelidada de `filial` nos `.sql`), dois clientes com o mesmo `id`
colapsam numa linha só — perda de dado silenciosa, invisível no log.
