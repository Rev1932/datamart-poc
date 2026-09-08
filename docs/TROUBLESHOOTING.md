# Troubleshooting — POC Datamart (MinIO + Spark + ClickHouse no minikube)

Registro dos problemas encontrados ao subir a stack e rodar a pipeline `datamart`,
com sintoma, erro exato (para busca), diagnóstico, causa raiz e correção aplicada.
Ambiente: **WSL2 + minikube (driver docker)**, Spark **3.5.1**, Python **3.12**,
ClickHouse (Altinity operator), Spark Operator (kubeflow).

Índice:
1. [`application-local.conf` ausente (ConfigManager)](#1-application-localconf-ausente)
2. [pyhocon mantém aspas em chaves com ponto (`spark.conf`)](#2-pyhocon-mantem-aspas-nas-chaves-com-ponto)
3. [Python 3.12 na imagem (deadsnakes falha no focal)](#3-python-312-na-imagem--deadsnakes-nao-funciona-no-focal)
4. [Log handler ClickHouse quebra no import](#4-log-handler-clickhouse-quebra-no-import)
5. [`--packages` (ivy) falha no Spark Operator: `HOME=/nonexistent`](#5---packages-ivy-falha-no-spark-operator)
6. [CHI nunca fica `condition=Ready`](#6-chi-nunca-fica-conditionready)
7. [Setting de merge "não aplicado" (falso positivo)](#7-setting-de-merge-nao-aplicado-falso-positivo)
8. [Avisos benignos do minikube](#8-avisos-benignos-do-minikube)
9. [`UNRESOLVED_COLUMN dap.andon_peso_id` + chave-pk inexistente](#9-unresolved_column-dapandon_peso_id--chave-pk-inexistente)
10. [Pipeline `datamart` trava no `extract` / apiserver TLS timeout](#10-pipeline-datamart-trava-no-extract--apiserver-tls-timeout--em-aberto)
11. [`save` no ClickHouse falha com `Magic is not correct` (LZ4) — incompat de versão](#11-save-no-clickhouse-falha-com-magic-is-not-correct-lz4--incompat-de-versao)

---

## 1. `application-local.conf` ausente

**Sintoma:** ao rodar qualquer pipeline, `ConfigManager` lança
`Exception: Não foi foi encontrado arquivo de configuração, cria arquivo em resource/application-local.conf`.

**Causa raiz:** `utils/config_manager.py` carrega `application-<ENV>.conf` (ENV default `local`)
e o arquivo está no `.gitignore` (`resources/.gitignore` → `application-local.conf`),
portanto não vem no repositório.

**Correção:** criado `spark-source-code/resources/application-local.conf` com as chaves
usadas pelo código: `minio.base_path`, `spark.s3.*`, `spark.conf.*`, `trino.schema_folder_*`,
`clickhouse.*`. Segredos vêm de env (`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`,
`CLICKHOUSE_PASSWORD`) via `${?VAR}`.

**Prevenção:** manter um `application-local.conf.example` versionado; lembrar que
`schema_folder_silver` **precisa** bater com onde o `bronze_silver` grava a silver
(`business_datavault_data-bee`), senão a query da gold não acha as tabelas.

---

## 2. pyhocon mantém aspas nas chaves com ponto

**Sintoma:** configs do Spark (Delta, catálogo ClickHouse, S3A) não eram aplicadas;
o Delta não resolvia `delta.\`path\`` e o catálogo `clickhouse` não existia.

**Diagnóstico:** iterando `config.get("spark.conf").items()`, a chave vinha como
`'"spark.sql.extensions"'` — **com as aspas literais** (testado com pyhocon 0.3.61 e a
versão mais nova; ambas se comportam igual). Assim `builder.config('"spark.sql.extensions"', v)`
registrava uma chave Spark inválida.

**Causa raiz:** no HOCON, chaves com ponto precisam ser aspeadas (`"spark.sql.extensions"`),
e o pyhocon não remove as aspas ao expor a chave via `.items()`.

**Correção:** em `spark-source-code/src/main/utils/session.py`, no loop de `spark.conf`:
```python
for key, value in config.get("spark.conf", {}).items():
    key = key.strip('"')   # pyhocon mantém as aspas em chaves com ponto
    builder = builder.config(key, value)
```

**Prevenção:** validar a config localmente antes de buildar:
```python
from pyhocon import ConfigFactory
c = ConfigFactory.parse_file("spark-source-code/resources/application-local.conf")
print([k.strip('"') for k in c.get("spark.conf").keys()])
```

---

## 3. Python 3.12 na imagem — deadsnakes não funciona no focal

**Contexto:** o código usa f-strings com aspas aninhadas (PEP 701), ex.:
`f"...{config_params["pipeline_type"]}..."` → **exige Python 3.12** (SyntaxError em <3.12).
A base `apache/spark:3.5.1` é Ubuntu **focal 20.04** com Python 3.8.

**Sintoma 1 (build):** `E: Unable to locate package python3.12-venv` /`python3.12-dev`.
**Sintoma 2 (build):** depois, `/bin/sh: 1: python3.12: not found` (exit 127) no `get-pip`.

**Diagnóstico:** dentro da base, `apt-cache policy python3.12` resolvia para
`postgresql-plpython3-12` (o apt trata o `.` como regex e casa outro pacote), e
`apt-cache madison python3.12` vinha **vazio** — ou seja, o índice do PPA deadsnakes
não estava disponível (rede até o launchpad falhando neste ambiente).

**Causa raiz:** deadsnakes/apt indisponível/inconsistente no focal deste ambiente.

**Correção:** parar de usar apt/deadsnakes e **copiar o Python 3.12 de
`python:3.12-slim-bullseye`** via multi-stage (bullseye tem glibc 2.31, igual ao focal,
então o binário roda). O `pip` já vem no `site-packages` copiado. Ver
`images/spark/Dockerfile`:
```dockerfile
FROM python:3.12-slim-bullseye AS py
FROM apache/spark:3.5.1
COPY --from=py /usr/local/bin/python3.12 /usr/local/bin/python3.12
COPY --from=py /usr/local/lib/python3.12 /usr/local/lib/python3.12
COPY --from=py /usr/local/lib/libpython3.12.so.1.0 /usr/local/lib/
RUN ldconfig && python3.12 --version
ENV PYSPARK_PYTHON=/usr/local/bin/python3.12 PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.12
```

**Prevenção:** usar **bullseye** (não bookworm) como fonte do Python — bookworm tem
glibc 2.36 e o binário quebraria no focal com `GLIBC_2.34 not found`. Validar no build:
`python3.12 -c "import ssl, ctypes, lzma, sqlite3"`.

---

## 4. Log handler ClickHouse quebra no import

**Sintoma:** import de qualquer módulo falha:
```
File ".../utils/handler_logger.py", line 38, in __init__
    self.base_url = config.get("clickhouse.url").rstrip('/')
AttributeError: 'NoneType' object has no attribute 'rstrip'
```

**Causa raiz:** `initialize_logger()` (chamado no nível de módulo em vários arquivos)
sempre instancia `ClickHouseAsyncHandler`, que exige `clickhouse.url` (ele envia os logs
da app para `datawake_logs.logs` via HTTP).

**Correção:** o envio de logs para o ClickHouse foi **desabilitado** nesta POC (decisão
do time — não poluir a instância-datamart). Gate por config em
`spark-source-code/src/main/utils/handler_logger.py`:
```python
if str(config_manager.get("clickhouse.logs_enabled", True)).lower() == "true":
    logger_base.addHandler(ClickHouseAsyncHandler(config_manager))
```
e em `application-local.conf`: `clickhouse.logs_enabled = false`. O default `True`
preserva o comportamento de produção.

**Nota:** existe também telemetria opcional via SQL Server (`utils/log.py`), que tenta
`POST {sqlserver.url}/gerenciamento_ingestoes`. Sem `sqlserver.*` na config ela apenas
imprime o payload e falha de forma silenciosa (capturada) — não é fatal.

---

## 5. `--packages` (ivy) falha no Spark Operator

**Sintoma:** a `SparkApplication` vai direto para `SUBMISSION_FAILED`/`FAILED`, **sem
criar o pod do driver**. No status/eventos:
```
failed to run spark-submit: ...
Ivy Default Cache set to: /nonexistent/.ivy2.5.2/cache
Exception in thread "main" java.io.FileNotFoundException:
  /nonexistent/.ivy2.5.2/cache/resolved-...xml (No such file or directory)
  at org.apache.spark.util.MavenUtils$.resolveMavenCoordinates(...)
```

**Causa raiz:** com `spec.deps.packages`, o **operator** roda `spark-submit --packages`
e o ivy tenta escrever o cache em `HOME=/nonexistent` (usuário do operator sem home).
Além disso, em k8s cluster mode os jars resolvidos no operator não chegariam ao driver.

**Correção:** **assar os jars na imagem** (`/opt/spark/jars`) e remover `deps.packages`
dos manifests e do `submit.sh`. No `images/spark/Dockerfile`, resolvemos via ivy no build
(rodando um Pi em `local[1]` só para baixar as deps transitivas) e copiamos:
```dockerfile
RUN /opt/spark/bin/spark-submit --master "local[1]" \
      --packages "$SPARK_JARS_PACKAGES" --conf spark.jars.ivy=/tmp/.ivy2 \
      /opt/spark/examples/src/main/python/pi.py 1 \
    && find /tmp/.ivy2 -name '*.jar' -exec cp -n {} /opt/spark/jars/ \; \
    && rm -rf /tmp/.ivy2
```
Jars assados: `delta-spark`, `delta-storage`, `hadoop-aws`, `aws-java-sdk-bundle`,
`clickhouse-spark-runtime`, `clickhouse-client`, `clickhouse-data`,
`clickhouse-http-client`, `httpclient5`.

**Prevenção / alternativas:** se precisar de `--packages` em runtime, definir
`spark.jars.ivy=/tmp/.ivy2` (writable) **e** um `spark.kubernetes.file.upload.path`
(ex.: `s3a://...`) para os jars chegarem ao driver — mais frágil que assar na imagem.

---

## 6. CHI nunca fica `condition=Ready`

**Sintoma:** `kubectl wait --for=condition=Ready chi/datamart` expira, mesmo com o pod
`chi-datamart-datamart-0-0-0` `1/1 Running`.

**Causa raiz:** o Altinity operator **não** expõe uma condition `Ready`; ele usa
`.status.status` (que fica `Completed` quando pronto).

**Correção:** trocar a espera por poll no status agregado. Em `scripts/bootstrap.sh` e
`infra/clickhouse/operator-install.md`:
```bash
until [ "$(kubectl -n datamart get chi datamart -o jsonpath='{.status.status}')" = "Completed" ]; do sleep 10; done
```

---

## 7. Setting de merge "não aplicado" (falso positivo)

**Sintoma:** `SELECT value FROM system.settings WHERE name='background_merges_mutations_concurrency_ratio'`
retornava `2`, mesmo tendo configurado `4` no CHI.

**Diagnóstico:** `system.settings` mostra o default de sessão para alguns settings de
servidor. A fonte autoritativa é `system.server_settings`:
```sql
SELECT name, value, changed FROM system.server_settings
WHERE name IN ('background_pool_size','background_merges_mutations_concurrency_ratio');
-- background_pool_size=16 changed=1 ; background_merges_mutations_concurrency_ratio=4 changed=1
```

**Conclusão:** **não era bug** — o setting estava aplicado. Ao validar settings de
servidor/merge, consultar `system.server_settings` (servidor) e `system.merge_tree_settings`
(merge_tree), não `system.settings`.

---

## 8. Avisos benignos do minikube

**Sintoma:** `minikube start` avisa
`You cannot change the memory size/CPUs/disk for an existing minikube cluster`.

**Causa:** já existia um cluster minikube com outra config de recursos; `start` reusa o
existente e ignora `--cpus/--memory/--disk-size`.

**Ação:** benigno. Para aplicar novos recursos: `minikube delete` e recriar (apaga os
dados — aceitável entre rodadas de benchmark).

---

## 9. `UNRESOLVED_COLUMN dap.andon_peso_id` + chave-pk inexistente

**Sintoma:** o pod do driver `datamart-fact-200-cep-driver` vai a `Completed`, **mas grava
0 linhas** no ClickHouse (`SELECT count() FROM datamart.fact_200_cep` = 0). No log do
driver, a pipeline falha por dentro (o `Completed` é enganoso — o `finally` do `main.py`
sempre imprime `Processo finalizado`):
```
[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name
`dap`.`andon_peso_id` cannot be resolved. Did you mean one of the following?
[`dap`.`new_id`, `dap`.`peca_id`, `dop`.`new_id`, ...]; line 4 pos 4;
```

**Diagnóstico:** dois defeitos encadeados na leitura da gold
(`RepositoryGoldDataVaults.read()`), que é compartilhada por `gold` e `datamart`:
1. A query `resources/queries/fact_200_cep.sql` selecionava `dap.andon_peso_id`, coluna
   que **não existe** na silver `dw_andon_peso` (as colunas são `id`, `peca_id`, `new_id`…).
   O `id` da tabela **é** o id do andon_peso.
2. Mesmo com a query corrigida, o hash quebraria: os manifests passavam `--chave-pk id`,
   mas a **saída da query não tem coluna `id`** (foi renomeada), então
   `F.sha2(F.concat_ws("_", F.col("id")...))` (`repo/repository.py:55-60`) não resolveria.

**Causa raiz:** query referenciando coluna inexistente + chave de hash apontando para
coluna ausente no resultado. (Havia ainda um `fact_200_cep_.sql` órfão — cópia com schema
divergente, não carregado por `QueryUtils.build_query`, que só gerava confusão.)

**Correção aplicada:**
- `resources/queries/fact_200_cep.sql:11`: `dap.andon_peso_id AS andon_peso_id` →
  **`dap.id AS andon_peso_id`** (mantidos os `CAST`, que casam com os tipos do `ddl/01`).
- `infra/spark/sparkapplication-ingest.yaml` e `sparkapplication-gold.yaml`: `--chave-pk id`
  → **`andon_peso_id`** (grão = 1 linha por medição; `hk_business_id = sha2(andon_peso_id)`).
- Removido `resources/queries/fact_200_cep_.sql` (órfão).
- Rebuild da imagem (`minikube image build -t datamart-spark:poc -f images/spark/Dockerfile .`),
  pois a query vai assada na imagem.

**Verificação:** após o rebuild, o `extract` **passou do analyzer** (sem `UNRESOLVED_COLUMN`)
— o defeito de código está resolvido. A execução, porém, esbarrou em performance (§10).

**Ponto aberto:** confirmar a composição do `hk_business_id` = `andon_peso_id`. Os JOINs por
`nr_ordem_producao` podem *fan-out* (uma medição casando com >1 ordem); se o grão desejado
for por (medição, ordem), a chave-pk deve ser composta (`andon_peso_id nr_ordem_producao`).

---

## 10. Pipeline `datamart` trava no `extract` / apiserver TLS timeout

> Status: **contornado** (2026-07-03). Gargalo de **infra/dimensionamento**, não de código.
> O walkthrough rodou reduzindo o volume (poda por `source`); ver "Resolução" ao fim.

**Sintoma:** com a query corrigida, a `SparkApplication datamart-fact-200-cep` fica em
`RUNNING` indefinidamente (>15 min) sem gravar no ClickHouse. Sinais:
- Log do driver **silencia** logo após montar o plano dos JOINs:
  ```
  ... WARN SparkStringUtils: Truncated the string representation of a plan since it was
  too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
  ```
  e depois disso **nenhuma** linha `Extraídos N registros` nem `Starting job`.
- Executors com **0 tasks finalizadas** (`grep -c "Finished task"` = 0).
- Em seguida o próprio `kubectl` para de responder:
  ```
  Unable to connect to the server: net/http: TLS handshake timeout
  ```

**Diagnóstico:** gargalo clássico de *small files + Delta pesado* em cluster
subdimensionado, durante a reconstrução do snapshot + listagem da silver (fase de
`extract`/`count`, antes de qualquer job Spark):
- `dw_andon_peso` tem **2192+ commits** no `_delta_log` e **centenas de parquet pequenos**
  por partição `source=` (ex.: ~740 arquivos só em `source=data-bee_limeira`). Reconstruir
  o snapshot Delta e listar/abrir tudo via s3a→MinIO é lento no driver.
- Apenas **2 executors (1 core / 2g cada)** para um JOIN de 4 tabelas silver.
- Tudo co-locado num **minikube de nó único (4 CPU / 8g)** — driver + 2 executors +
  ClickHouse + MinIO. A carga do Spark **starva o `kube-apiserver`** (sem CPU para o
  handshake TLS), daí o `kubectl` cair. **Sintoma de saturação de nó, não de rede.**

**Causa raiz (hipótese a confirmar):** volume da silver (muitos arquivos pequenos + log
Delta longo) desproporcional aos recursos do minikube de nó único.

**Resolução (2026-07-03):**
- O job travado (42h em `RUNNING`, driver sem chegar a **nenhum** job Spark, depois perdendo
  o apiserver) foi morto com `kubectl -n datamart delete sparkapplication datamart-fact-200-cep`.
  Pós-kill o nó normalizou (CPU ~3%). Confirma que o hang era **saturação do nó**.
- Para o walkthrough, reduzimos o volume podando a query para **uma** `source=`:
  `AND dap.source='data-bee_uberaba'` (+ as 3 dims), marcado `[WALKTHROUGH-TEMP]` em
  `fact_200_cep.sql`. Mesmo assim o `extract` levou **~10–14 min** (uberaba ~26k linhas).

**Achado importante — compactar os arquivos de dados NÃO acelera o extract.** Rodamos
`OPTIMIZE ... where source='data-bee_uberaba'` nas 4 silver (dw_andon_peso **66→1** arquivo,
etc.) e o `extract` **não** melhorou. O gargalo não são os parquets da partição lida: é a
**reconstrução do snapshot Delta**, que lê os `AddFile` de *todas* as ~2000 partições da
tabela inteira (o checkpoint mais recente) independentemente do filtro de `source`. Para
atacar de fato seria preciso compactar a **tabela inteira** (todas as partições) e/ou
enxugar o `_delta_log`, ou subir CPU/memória do nó (`minikube delete` + recriar maior — §8).
Para o objetivo da POC (medir o merge no ClickHouse) isso não bloqueia: basta tolerar o
extract lento uma vez.

---

## 11. `save` no ClickHouse falha com `Magic is not correct` (LZ4) — incompat de versão

> Status: **resolvido** (2026-07-03). Bug latente que só apareceu quando o `extract` (§10)
> finalmente completou — antes disso o job nunca chegava ao `save`.

**Sintoma:** o `extract` conclui (`Extraídos N registros`), o `transform` também, e o `save`
estoura já em `ClickHouseCatalog.initialize` (antes de gravar qualquer linha):
```
java.io.IOException: Magic is not correct - expect [-126] but got [-60]
  at com.clickhouse.data.stream.Lz4InputStream.updateBuffer
  at com.clickhouse.spark.format.JSONEachRowSimpleOutput$.deserialize
  at com.clickhouse.spark.ClickHouseCatalog.initialize(ClickHouseCatalog.scala:79)
  at org.apache.spark.sql.DataFrameWriterV2.append
```
A `SparkApplication` ainda assim termina `COMPLETED` (o erro é capturado pelo wrapper), mas
`SELECT count() FROM datamart.fact_200_cep` = **0**.

**Causa raiz:** incompatibilidade de versão entre o cliente **clickhouse-java 0.6.3**
(assado na imagem) e o servidor **ClickHouse 26.6** no framing LZ4 das respostas HTTP.
Verificado direto no HTTP (porta 8123) via `wget ... ?compress=1`: o servidor devolve
framing **padrão e correto** (16 bytes de checksum, depois o magic `0x82` = -126); o cliente
0.6.3 lê o offset errado e vê `0xC4` (-60, um byte do checksum). O servidor está OK — o
cliente é que está velho (~2 anos). Os knobs de compressão do conector **não** desligam esse
caminho: testados `spark.sql.catalog.clickhouse.option.compress=false` e
`spark.clickhouse.read.compression.codec=none`, ambos **sem efeito** no control-query do
`initialize` (o conector 0.8.0 força LZ4 ali).

**Correção:** alinhar a versão do cliente/conector nos jars da imagem
(`images/spark/Dockerfile`, `SPARK_JARS_PACKAGES`):
```
com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0  ->  0.10.0
com.clickhouse:clickhouse-client:0.6.3                        ->  0.9.8
com.clickhouse:clickhouse-http-client:0.6.3                   ->  0.9.8
```
O conector 0.10.0 manteve a API usada (sem `NoSuchMethod`). Após o rebuild, o `save` grava
normalmente. **Não** faça downgrade do servidor (ClickHouse recusa downgrade de major com
dados já gravados; e o servidor não tem defeito).

**Como validar rápido (sem rodar o job pesado):** um *smoke test* que só toca o catálogo
reproduz/valida o `initialize` em ~90 s, sem ler Delta:
```python
spark.sql("SHOW TABLES IN clickhouse.datamart").show()  # dispara ClickHouseCatalog.initialize
```

**Inspecionar o framing HTTP:** o pod do ClickHouse **não** tem `curl`, mas tem
`wget`/`od`/`xxd`. Autentique por header: `wget -qO- --header='X-ClickHouse-User: datamart'
--header='X-ClickHouse-Key: <pwd>' 'http://localhost:8123/?compress=1&query=SELECT%201%20FORMAT%20JSONEachRow' | od -An -tx1`.

---

## Comandos úteis de diagnóstico

```bash
# Estado geral
kubectl get pods -A
kubectl -n datamart get chi datamart -o jsonpath='{.status.status}'

# SparkApplication: estado e motivo de falha
kubectl -n datamart get sparkapplication <nome> -o jsonpath='{.status.applicationState}'
kubectl -n datamart describe sparkapplication <nome> | sed -n '/Events:/,$p'
kubectl -n datamart logs <nome>-driver           # onde argparse/pipeline rodam
kubectl -n spark-operator logs -l app.kubernetes.io/name=spark-operator --tail=50

# ClickHouse
kubectl -n datamart exec -it chi-datamart-datamart-0-0-0 -- \
  clickhouse-client --user datamart --password datamart123 --query "SELECT 1"

# Imagem no minikube / jars assados
minikube image ls | grep datamart-spark
eval "$(minikube docker-env)"; docker run --rm --user root datamart-spark:poc ls /opt/spark/jars
```
