# Validação em Kubernetes — job `honeycomb` orquestrado por DAG

> **Escopo:** relatório da validação do job em cluster Kubernetes, da submissão manual à geração
> dinâmica de DAG por tenant. Cobre `gold` (§2–§5) e `bronze_silver` (§6–§7), os defeitos
> encontrados e o que falta para produção.
> **Última atualização:** 2026-07-23 · branch `main` (base `5353334`).
> Complementa [`deploy-kubernetes.md`](./deploy-kubernetes.md) (artefatos de deploy) e
> [`pendencias-kubernetes.md`](./pendencias-kubernetes.md) (impeditivos de código).
> O template de DAG e a API de provisionamento são documentados no Honeymaker,
> `src/honeymaker/domains/airflow/README.md`.

**Resultado:** o job está validado fim a fim em Kubernetes, em quatro camadas — submissão manual
(§2–§4), orquestração por `SparkKubernetesOperator` (§5), **geração dinâmica de manifesto por
tenant a partir de config no MongoDB** (§6) e o pipeline `bronze_silver` com merge entre filiais
(§7). A entrega é um template que o Honeymaker instancia por tenant. Três defeitos do código do
honeycomb foram descobertos e documentados (§12), nenhum corrigido no repositório — o mais grave é
perda de dado silenciosa sob concorrência.

**Percurso da validação:** submissão manual → DAG única com lista estática → DAG factory por
filial → **template por tenant instanciado pelo Honeymaker** (desenho final). Os passos
intermediários estão preservados aqui pelas lições que renderam; o artefato de produção é o
template do §6.

---

## 1. Contexto

Até esta validação, os manifestos do projeto nunca haviam sido submetidos a um cluster. A
execução foi feita em **minikube** (Spark Operator 2.5.0, imagem `honeycomb:latest` do Harbor,
MinIO local), justamente para separar falhas de manifesto de falhas de orquestração antes de
envolver o Airflow.

Decisões que definiram o alvo: pipeline **`gold`** · identidade **`spark-unipac`** · bucket de
ensaio **`datawake-unipac-uat`** · sem registrar tabela no Trino nesta passada.

## 2. Defeito principal — `mountPath` duplicado

A primeira submissão falhou **sem criar pod nenhum**:

```
Pod "spark-unipac-gold-fact-200-cep-driver" is invalid:
spec.containers[0].volumeMounts[4].mountPath: Invalid value: "/tmp/spark-local": must be unique
```

**Causa.** `spark.local.dir=/tmp/spark-local` no `sparkConf` faz o próprio Spark criar um
`emptyDir` nesse caminho. Somado ao volume explícito declarado no mesmo `mountPath`, saem dois
mounts idênticos e a API rejeita o pod. O `LocalDirsFeatureStep` do Spark só reconhece — e
portanto não duplica — volumes cujo nome tem o prefixo **`spark-local-dir-`**; o nome
`spark-local-dir`, sem o hífen final, não casa com a regra.

**Correção.** Volume renomeado para `spark-local-dir-1` e `spark.local.dir` removido do
`sparkConf` (passa a vir do volume reconhecido, preservando o controle de `sizeLimit`).

**Alcance.** O defeito era idêntico em `k8s/sparkapplication.yaml` e no manifesto base da DAG
(`dtlk-airflow-pipeline/dags/manifests/spark-unipac-gold.yaml`). Ambos corrigidos. Teria
quebrado da mesma forma em `dw-dados` — com a diferença de que, submetido pela DAG, o sintoma
seria uma task vermelha sem pod algum para inspecionar.

> Este é o retorno concreto da regra "submeter uma vez à mão antes de plugar no Airflow".

## 3. Demais correções aplicadas

### 3.1 `k8s/sparkapplication.yaml`

| Campo | Antes | Depois | Motivo |
| :-- | :-- | :-- | :-- |
| `envFrom` (driver **e** executor) | `spark-into-postgres-config` / `-secret` | `spark-unipac-config` / `spark-unipac-secret` | os objetos renomeados não existiam sob o nome antigo → `CreateContainerConfigError` |
| `arguments` | `--pipeline gold_datamart` | `--pipeline gold` | pipeline escolhido |
| `labels` | `spark-into-postgres` | `spark-unipac` | coerência de identidade |

### 3.2 `k8s/configmap.yaml` — endpoints fora do contrato da plataforma

| Chave | Antes | Depois |
| :-- | :-- | :-- |
| `SPARK_S3_ENDPOINT` | `https://uat-minio.datawake.cloud:443` | `https://minio.datawake.cloud:9000` |
| `TRINO_HOST` | `trino.datawake.cloud` | `trinodb2.datawake.cloud` |
| `TRINO_PORT` | `48085` | `443` |
| `TRINO_CATALOG` | `tenant` | `delta_lake` |
| `CLICKHOUSE_VERIFY_SSL` | `wildcard_datadriven_cloud.crt` | `true` |

`uat-minio` não existe em nenhum lugar da plataforma, e as credenciais do `spark-secrets` valem
para `minio.datawake.cloud:9000`. O endpoint antigo do Trino (`:48085`, HTTPS direto do Compose)
foi descontinuado em favor do TLS terminado no Traefik, e os catálogos por tenant foram
consolidados no catálogo unificado `delta_lake`.

### 3.3 `k8s/secret.example.yaml`

Renomeado para `spark-unipac-secret`, casando com o `envFrom`. As chaves foram anotadas por
pipeline: **`gold` não consome `POSTGRES_*` nem `SPARK_S3_*`** — o primeiro é do `gold_datamart`
e o segundo alimenta o cliente `minio-py`, instanciado apenas em `RepositoryBronzeToSilver`.

> ⚠️ **Armadilha.** Apesar do prefixo, `TRINO_SCHEMA_FOLDER`, `_SILVER` e `_GOLD` são
> **componentes de caminho S3**, não configuração de Trino: `queryutils.build_query` levanta
> `ValueError` se `_SILVER`/`_GOLD` forem `None`, e `RepositoryGoldDataVaults.output_path`
> depende de `_GOLD`. São obrigatórias mesmo numa execução sem Trino.

## 4. Evidência da validação

Ambiente: minikube (4 CPU / 8 GiB), Spark Operator 2.5.0 em `dw-dados`, MinIO no host,
1 driver + 1 executor, imagem `hub.datawake.cloud/dw-dados/honeycomb:latest`.

Seed: 4 tabelas Delta silver, sendo 7 linhas em `dw_andon_peso` — 5 válidas, 1 fora da janela de
10 dias e 1 com `real = 0`.

| | 1ª execução | 2ª execução |
| :-- | :-- | :-- |
| Extraídos | 5 registros | 5 registros |
| Após `transform` | 5 | 5 |
| Operação Delta | `WRITE` (v0) | `MERGE` (v1) |
| Linhas na tabela | 5 | **5** |
| `load_dts` | 18:25:44 | **18:30:19** |

**O que isso comprova:**

- os dois filtros do `WHERE` funcionam (7 semeadas → 5 extraídas);
- os três `INNER JOIN` resolvem — `produto_codigo` (`MAT-001..005`) chega via
  `dw_ordem_producao.produto_id` → `dw_material`;
- `hk_business_id` bate com `sha2(concat_ws("_", andon_peso_id, filial, banco))` — **0
  divergências, 0 duplicatas**;
- **o job é idempotente**: a segunda execução manteve 5 linhas e atualizou o `load_dts`, ou seja
  o `whenMatchedUpdateAll` disparou em vez de inserir. É o que sustenta `retries: 1` na DAG;
- S3A autentica e opera contra o MinIO; executores são agendados e registrados pelo K8s;
- sem `CLICKHOUSE_URL`, o logger degrada para console sem derrubar o job.

## 5. Validação via Airflow (`SparkKubernetesOperator`)

Segunda rodada: a mesma execução, agora disparada por uma DAG. Ambiente local — Airflow 2.11.2,
CeleryExecutor, provider `apache-airflow-providers-cncf-kubernetes` **8.4.2** (produção usa
**10.1.0**), rodando **fora** do cluster.

**Resultado:** a DAG criou o `SparkApplication`, acompanhou o driver até o fim, trouxe o log dele
para o log da task e propagou o estado. A tabela Delta ganhou duas novas versões `MERGE` (a
execução e o retry), mantendo 5 linhas e 0 duplicatas — a idempotência que sustenta o
`retries: 1` foi exercitada de ponta a ponta pela orquestração, não só inferida do código.

### 5.1 Airflow fora do cluster: conectividade não é automática

Com o Airflow em containers e o Kubernetes em outro runtime, **as redes não se falam**:

```
worker -> 192.168.49.2:8443           INALCANÇÁVEL  (redes docker distintas)
worker -> host.docker.internal:32776  No route to host
```

A API do minikube é publicada apenas como `127.0.0.1:32776` — loopback, logo inacessível de
outro container. A solução é anexar os containers que executam tasks (no CeleryExecutor, os
**workers**) à rede docker do cluster, e usar o IP interno do nó. O certificado do apiserver já
traz esse IP nos SANs, então o TLS valida sem `insecure-skip-tls-verify`.

A connection `kubernetes_default` **não pode** usar `in_cluster` nesse arranjo, e o kubeconfig do
host não serve como está: aponta para `127.0.0.1` e referencia os certificados **por caminho**,
que não existem dentro do container. O caminho limpo é um kubeconfig achatado — `kubectl config
view --raw --minify --flatten` embute os certificados em base64 —, com o `server:` reescrito para
a rota interna, entregue no `extra` da conexão (`kube_config`). Sem montar volume nenhum.

### 5.2 O operator descarta o `name` — o CR é nomeado pelo `task_id`

```python
super().__init__(name=name, **kwargs)
self.name = self.create_job_name()      # sobrescreve na linha seguinte

def create_job_name(self):
    initial_name = add_unique_suffix(name=self.task_id, max_len=MAX_LABEL_LEN)
```

O parâmetro `name` é aceito e **imediatamente jogado fora**, e o `metadata.name` do manifesto
também é ignorado (o `CustomObjectLauncher` recebe `name=self.name`). O CR sempre se chama
`<task_id>-<sufixo aleatório>` — por exemplo `spark-gold-9shi0ak3`.

**Consequência para dynamic task mapping:** com N tabelas mapeadas na mesma task, todos os CRs
compartilham o mesmo prefixo e ficam indistinguíveis pelo nome. A rastreabilidade por tabela tem
de vir do `map_index` do Airflow ou do `--table_name` em `spec.arguments`. Se o nome do CR
precisar identificar a tabela, o caminho é **uma task por tabela** (task_ids distintos), não
mapeamento dinâmico.

> Verificado no provider 8.4.2. Confirmar antes de promover para o 10.1.0 de produção.

### 5.3 O CR e o pod somem ao fim da task

`delete_on_termination` é `True` por padrão (herdado do `KubernetesPodOperator`): assim que a
task termina, pod e CR são removidos. `kubectl logs` e `kubectl describe` deixam de ser opções
para post-mortem — **o log da task do Airflow passa a ser a única fonte**. Felizmente ele contém
o stdout completo do driver, prefixado por `[spark-kubernetes-driver]`.

### 5.4 Comportamentos do provider 8.4.2 confirmados

Três premissas do desenho da DAG, verificadas no código instalado e não assumidas:

- **`application_file` aceita YAML inline.** `manage_template_specs` tenta resolver o valor como
  caminho e **cai para tratá-lo como conteúdo** se não for um arquivo existente — então passar o
  manifesto renderizado por `.expand()`/`.expand_kwargs()` funciona.
- **`random_name_suffix=True`** por padrão → re-execuções não colidem com o CR, que é imutável
  por nome.
- **O corpo é embrulhado automaticamente** (`if "spark" not in template_body`), então um
  manifesto `SparkApplication` cru é aceito sem adaptação.

### 5.5 Evidência

Extraído do log da task (o operator faz tail do driver):

```
{spark_kubernetes.py:285} Creating sparkApplication.
[spark-kubernetes-driver] AppLogger - INFO  - Extraídos 5 registros
[spark-kubernetes-driver] AppLogger - INFO  - Dados transformados possuem 5 registros
[spark-kubernetes-driver] AppLogger - INFO  - Pipeline concluído com sucesso!
[spark-kubernetes-driver] AppLogger - ERROR - Falha crítica: Tipo de autenticação inválido.
```

Histórico Delta após a rodada: versões **2 e 3, ambas `MERGE`** — 5 linhas, `load_dts`
atualizado, 0 hashes duplicados.

## 6. Geração dinâmica de manifesto por tenant (config no MongoDB)

A entrega final não é uma DAG escrita à mão, e sim um **template que o Honeymaker instancia por
tenant**. O ciclo de vida começa no Honeymaker: um `POST` grava, na mesma saga, o documento de
config no MongoDB **e** o arquivo `pipeline_<tenant>.py`. Documentação do template e da API no
próprio Honeymaker: `src/honeymaker/domains/airflow/README.md`.

**Arquitetura da DAG gerada:** 1 DAG por tenant (`<tenant>_bronze_silver`); as **filiais e tabelas
são lidas do Mongo em runtime**, dentro de um `@task`, e viram um `SparkApplication` por
combinação `(filial × tabela)` via `expand_kwargs`. Só o `dag_id` e o `schedule` são resolvidos
na geração/parse; trocar filiais ou tabelas não exige regerar a DAG.

**Validado com dois tenants reais**, provando que a parametrização é por JSON e não um caso
especial:

| Tenant | `filiais` × `tables` | Instâncias mapeadas | `schedule` (do JSON) |
| :-- | :-- | :-- | :-- |
| `unipac` | 5 × 33 | **165** | `30 3,9,15,21 * * *` |
| `jurandir` | 2 × 1 | **2** | `0 3 * * *` |

Ambos saem do mesmo template, com cardinalidades e agendamentos diferentes. O `jurandir` foi
executado ponta a ponta no minikube: os dois `SparkApplication` subiram, gravaram Delta e as duas
partições de filial coexistiram (`data-bee_juran`, `data-bee_dir` — 8 linhas, 0 duplicatas).

**Resiliência de parse confirmada:** com o Mongo inalcançável, as duas DAGs continuam listadas
com `schedule=None`, em vez de sumirem do DagBag — ao contrário das DAGs legadas do repositório,
que fazem `raise` em parse time e desaparecem quando o Mongo oscila.

**Dois defeitos que o segundo tenant expôs:**

- **Conexão Mongo errada.** O template inicial usava `mongo_default`, que aponta para um Mongo
  legado; o `unipac` mascarou (coleção homônima antiga existe lá), mas o `jurandir` veio com
  `schedule=None` sem erro. Corrigido para uma connection dedicada `honeymaker_mongo`.
- **Schema legado do `unipac`.** O documento usava `filial_name` / `entity_list_incremental`; o
  schema canônico do Honeymaker é `filiais` / `tables`. Migrado com `$rename`.

## 7. Perda de dado silenciosa sob concorrência (`bronze_silver`)

Observado ao rodar uma DAG com 2 filiais na **mesma tabela** (`jurandir`: `juran`/`dir` × `vendas`)
com `max_active_tis_per_dag=2`. Resultado: a tabela silver ficou com **uma só das duas filiais**,
sem erro em lugar nenhum. É a interação de três defeitos já catalogados isoladamente, que juntos
causam **perda permanente e invisível**:

1. **Primeira carga não é concorrente-segura.** `RepositoryBronzeToSilver.write()` (`repository.py`)
   usa `mode("overwrite")` quando a tabela Delta ainda não existe. Duas filiais em primeira carga
   simultânea → uma vence, a outra é sobrescrita.
2. **`PipelineProcess.run()` não relança** (`core/pipeline.py`): o job da filial perdedora loga
   `Pipeline concluído com sucesso!` e sai com exit 0.
3. **`cleaning_processed` roda mesmo assim** (`pipeline_orchestrator.py`): move os parquet de
   origem da filial perdedora para `processed/*.bkp` — apesar de o dado nunca ter sido escrito.

O golpe fatal é a **reexecução não recuperar**: como a origem já foi arquivada no passo 3, a
segunda tentativa lê uma pasta vazia (`read()` → `None` → "Nenhum arquivo para processar") e
declara sucesso de novo. A filial fica marcada como processada sem nunca ter sido escrita.

**Verificação da causa raiz — serialização remove o problema.** Com `max_active_tis_per_dag=1`
(constante `MAX_SIMULTANEOS` do template), `juran` faz a primeira carga e `dir` cai no caminho de
**merge** (`whenNotMatchedInsertAll` por `hk_business_id`, com retry para
`ConcurrentAppendException`), que é seguro. As duas partições passam a coexistir. O template do
Honeymaker fixa `MAX_SIMULTANEOS = 1` por isso.

> Serializar é mitigação de orquestração, não correção. O defeito real está no honeycomb: a
> primeira carga deveria ser idempotente/atômica, e `cleaning_processed` jamais deveria rodar
> quando a escrita não foi confirmada. Enquanto isso não for corrigido, **duas execuções da mesma
> tabela nunca podem se sobrepor** — nem entre filiais, nem entre um retry e o run original.

## 8. Impedimento remanescente — o passo do Trino é incondicional

O log da execução com dado válido termina assim:

```
AppLogger - INFO  - Pipeline concluído com sucesso!
AppLogger - ERROR - Falha crítica: Tipo de autenticação inválido. Use 'basic' ou 'oauth2'.
  File "utils/trino_connection.py", line 44, in __init__
```

`PipelineGoldWrapper.run()` (`src/main/core/pipeline_factory.py:24-28`) constrói o
`TrinoConnection` **sempre**, depois da escrita Delta. Sem `TRINO_*` no ambiente, a exceção sobe
até o `main()`, que a relança → `SparkApplication` em `FAILED`, **mesmo com o dado gravado com
sucesso**. Sucesso e falha viram o mesmo estado, e a DAG não consegue distinguir.

**Correção proposta**, no padrão que já existe no projeto: `initialize_logger()`
(`src/main/utils/handler_logger.py:155-163`) só monta o handler do ClickHouse quando
`clickhouse.url` está configurada, senão emite `warning` e segue. Aplicar o equivalente ao passo
do Trino — registrar apenas quando `trino.host` estiver presente — torna a execução sem Trino um
estado de primeira classe e endurece o job para qualquer ambiente que não o tenha.

## 9. Como reproduzir a validação local

```bash
# cluster + operator
minikube start --cpus=4 --memory=8192 --driver=docker
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm upgrade --install spark-operator spark-operator/spark-operator --version 2.5.0 \
  --namespace dw-dados --create-namespace \
  --set 'spark.jobNamespaces={dw-dados}' \
  --set spark.serviceAccount.create=true --set spark.serviceAccount.name=spark --wait

# imagem
docker pull hub.datawake.cloud/dw-dados/honeycomb:latest
minikube image load hub.datawake.cloud/dw-dados/honeycomb:latest
```

O manifesto local difere do de produção por remover `nodeSelector`, `tolerations` e
`imagePullSecrets` (não existem no minikube), usar `imagePullPolicy: Never` e reduzir recursos.
Como o feature gate `LoadSparkDefaults` não está ativo, o `spark-defaults.conf` da imagem é
sombreado pelo conf gerado no submit — então Delta, S3A e `catalogImplementation=in-memory`
precisam estar explícitos no `sparkConf`.

Para inspecionar a saída fora do cluster, use `spark.sql.warehouse.dir=/tmp/warehouse`: o valor
da imagem aponta para `s3a://hive-warehouse/default`, que não existe em MinIO local, e qualquer
operação de catálogo (ex.: `DESCRIBE HISTORY`) falha ao tentar criá-lo.

## 10. O que falta para produção

**Código**
- Guarda do Trino (§6) → nova release e imagem; atualizar a tag nos dois manifestos.

**Infra** (requer acesso ao cluster)
- Criar `spark-unipac-secret` em `dw-dados` e aplicar o ConfigMap.
- Confirmar que a tag da imagem existe no Harbor.
- Verificar que o bucket alvo tem as 4 tabelas silver de origem.

**Sem cobertura de teste**
- `nodeSelector`, `tolerations` e `imagePullSecrets` — removidos no ambiente local.
- `register_table` no Trino.
- Dimensionamento real: validado com 2 GiB; produção declara 12 GiB.

> ⚠️ **Conferir a chave primária antes da primeira execução contra dado real.** A PK usada
> (`andon_peso_id`, `filial`, `banco`) veio dos testes unitários
> (`tests/unit/test_main.py:38`), não da configuração de produção. `hk_business_id` é o hash
> dessas colunas e é a chave do merge: se divergir do conjunto que gerou uma tabela existente, o
> merge **insere duplicatas em vez de atualizar**, sem erro.

## 11. Arquivos alterados

**`honeycomb`** — `k8s/sparkapplication.yaml`, `k8s/configmap.yaml`, `k8s/secret.example.yaml`
(correções dos §2–§3) e este documento. **O código-fonte do honeycomb não foi alterado**: o fix
do `MinioAdmin` (TLS derivado do esquema do endpoint, necessário contra MinIO HTTP) vive apenas na
imagem local de teste `honeycomb:local-minio-fix`, não no repositório — ver §10.

**`honeymaker`** (a entrega principal) — o template de DAG e sua integração:
- `src/honeymaker/domains/airflow/dag_template.py` — `DAG_TEMPLATE_HONEYCOMB` + registro `TEMPLATES`;
- `schemas.py` — campo `pipeline: Literal["journey","honeycomb"] = "journey"`;
- `dag_writer.py`, `service.py`, `router.py` — propagam o `pipeline` pela saga;
- `src/honeymaker/domains/airflow/README.md` — documentação do domínio e do template.

Retrocompatível (default `journey`); os 71 testes do domínio airflow passam.

**Ambiente de validação local (`dtlk`/`dags`)** — não versionado nesta sessão: template renderizado
em `pipeline_{unipac,jurandir}.py`, `manifests/spark-bronze-silver.yaml`, e as connections
`honeymaker_mongo` / `kubernetes_default` no Airflow.

> **Princípio que guiou o desenho:** o spec do pod é fonte única (`manifests/spark-bronze-silver.yaml`),
> lido pela DAG que sobrescreve apenas `spec.arguments`. Reconstruir o spec em Python foi o que fez
> uma versão anterior divergir da plataforma em 11 pontos.

## 12. Defeitos abertos no honeycomb (candidatos a PR)

Nenhum corrigido no repositório nesta validação; todos observados em execução real.

| Defeito | Seção | Gravidade |
| :-- | :-- | :-- |
| Perda de dado silenciosa sob concorrência (`bronze_silver`) | §7 | **Alta** — silenciosa e permanente |
| Passo do Trino incondicional (job reporta `FAILED` com dado gravado) | §8 | Média — mascara sucesso |
| `MinioAdmin` fixa `secure=True`, ignora o esquema do endpoint | §10 | Baixa — só morde fora de HTTPS |
