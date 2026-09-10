import time
import re
from pyspark import StorageLevel
from core.pipeline import  Pipeline, PipelineProcess
from utils.handler_logger import initialize_logger

'''
    O nome orchestrator deve ser alterado para o nome do cliente no qual está sendo aplicado esse pipeline
'''

# Mesmo literal que RepositoryBronzeToSilver usa hardcoded para montar o output_path
# (repo/repository.py). Fica como FALLBACK de TRINO_SCHEMA_FOLDER_SILVER: se a env nao vier,
# a manutencao aponta para a pasta certa em vez de montar um path invalido silenciosamente.
SILVER_FOLDER_FALLBACK = "business_datavault_data-bee"


def resolve_delta_path(environment_parameters, table_name: str, layer: str) -> str:
    """Monta o path da tabela Delta pela convencao de cada camada.

    Nao ha metastore do lado do job: silver e gold sao derivadas de `minio.base_path` mais a
    pasta da camada, exatamente como os repositorios fazem na escrita. Qualquer divergencia
    aqui aponta a manutencao para um path que nao existe.
    """
    base_path = environment_parameters.get("minio.base_path")
    if not base_path:
        raise ValueError("MINIO_BASE_PATH ausente: impossivel montar o path da tabela Delta.")

    if layer == "silver":
        folder = environment_parameters.get("trino.schema_folder_silver") or SILVER_FOLDER_FALLBACK
    elif layer == "gold":
        folder = environment_parameters.get("trino.schema_folder_gold")
        if not folder:
            raise ValueError("TRINO_SCHEMA_FOLDER_GOLD ausente: exigido por --delta_layer gold.")
    else:
        raise ValueError(f"Camada Delta desconhecida: {layer!r}. Use 'silver' ou 'gold'.")

    return f"{base_path}/{folder}/{table_name}"


def normalize_zorder_columns(colunas):
    """Normaliza `--colunas_zorder` para lista de colunas ou None.

    Aceita o que o argparse (`nargs='*'`) entrega — lista —, uma string separada por virgula
    (formato conveniente para o ConfigMap/DAG) ou ausencia. Devolve None quando nao ha coluna,
    que e o que `DeltaMaintenance.run_optimize` interpreta como OPTIMIZE generico.

    Deliberadamente NAO usa `ast.literal_eval(f"{...}")` como RepositoryGoldDataVaults: aquele
    padrao estoura ValueError para qualquer string que nao seja um literal Python.
    """
    if not colunas:
        return None
    if isinstance(colunas, str):
        colunas = colunas.split(",")
    colunas = [str(c).strip() for c in colunas if str(c).strip()]
    return colunas or None


class PipelineGold(Pipeline):

    def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir: str):
        super().__init__(spark)
        self.environment_parameters = environment_parameters
        from repo.repository import RepositoryGoldDataVaults
        self.repository = RepositoryGoldDataVaults(
            self.spark, self.environment_parameters, runtime_parameters, queries_dir)

    def extract(self):
        return self.repository.read()

    def transform(self, data):
        return self.repository.transform(data)

    def save(self, data):
        return self.repository.write(data)    


class PipelineGoldDatamart(Pipeline):

    def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir: str):
        super().__init__(spark)
        self.environment_parameters = environment_parameters
        from repo.repository import RepositoryGoldDatamart
        self.repository = RepositoryGoldDatamart(
            self.spark, self.environment_parameters, runtime_parameters, queries_dir)

    def extract(self):
        return self.repository.read()

    def transform(self, data):
        return self.repository.transform(data)

    def save(self, data):
        return self.repository.write(data)


class PipelineDatamartClickhouse(Pipeline):

    def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir: str):
        super().__init__(spark)
        self.environment_parameters = environment_parameters
        from repo.repository import RepositoryDatamartClickhouse
        self.repository = RepositoryDatamartClickhouse(
            self.spark, self.environment_parameters, runtime_parameters, queries_dir)

    def extract(self):
        return self.repository.read()

    def transform(self, data):
        return self.repository.transform(data)

    def save(self, data):
        return self.repository.write(data)


class PipelineBronzeToSilver(PipelineProcess):
    def __init__(self, spark, environment_parameters, runtime_parameters):

        self.logger = initialize_logger()
        self.runtime_parameters = runtime_parameters
        from repo.repository import RepositoryBronzeToSilver
        self.repository = RepositoryBronzeToSilver(spark, environment_parameters, runtime_parameters)
        from tranformer.transformer import RawDataVaultInitTransformerAuto
        self.raw_data_vault_init_transformer = RawDataVaultInitTransformerAuto(environment_parameters, runtime_parameters)
        from tranformer.transformer import HardBusinessRulesTransformerAuto
        self.hard_business_rules_transformer = HardBusinessRulesTransformerAuto(environment_parameters, runtime_parameters)
        from tranformer.transformer import ValidateBusinessRulesTransformer
        self.validate_business_rules_transformer = ValidateBusinessRulesTransformer()
                        
        self.logger.info(f"Inicialização de {runtime_parameters['filial_name']} para o processo {runtime_parameters['pipeline']} com sucesso.")

    def _extract_problematic_file(self, error_message: str) -> str | None:
        """Extrai o path do arquivo problemático da mensagem de erro do Spark."""
        match = re.search(r'file\s+s3a?://[^/]+/([^\s]+\.parquet)', str(error_message))
        return match.group(1) if match else None

    def _gravar_e_limpar(self, data):
        """
        Materializa o DataFrame transformado e o consome nas duas etapas finais.

        `write()` e `cleaning_processed()` sao DUAS acoes Spark sobre a mesma linhagem —
        sem cache, toda a cadeia (leitura dos parquets, alinhamento de schema, os tres
        transformers) e recomputada do zero na segunda. O persist paga uma vez e reusa.

        O unpersist no `finally` nao e opcional: com varias tabelas em paralelo no mesmo
        pod, cache nao liberado ocupa a memoria/disco dos executors ate o fim do job.
        """
        data.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            self.repository.write(data)
            self.repository.cleaning_processed(data)
        finally:
            data.unpersist()

    def _process_single_file(self, file_path: str):
        """Processa um único arquivo problemático pelo path completo."""
        self.logger.info(f"[FALLBACK] Processando arquivo problemático: {file_path}")
        start_time = time.time()

        data = self.repository.read_file(file_path)

        input_count = data.count()
        self.logger.info(f"[FALLBACK] {input_count} registros extraídos de {file_path}")

        if input_count == 0:
            self.logger.warning(f"[FALLBACK] Arquivo vazio: {file_path}")
            return

        data = self.raw_data_vault_init_transformer.transform(data)
        data = self.hard_business_rules_transformer.transform(data)
        data = self.validate_business_rules_transformer.transform(data)

        self._gravar_e_limpar(data)

        duration = round(time.time() - start_time, 2)
        self.logger.info(f"[FALLBACK] Arquivo {file_path} processado em {duration}s")


    def preparer(self):
        self.logger.debug("Executando preparer (sem tarefas pendentes).")

    def process(self):

        table_process_exception = ['dw_entidade_atributo_registro']
        if self.runtime_parameters["table_name"] in table_process_exception:
            self.process_one_file_per_execution()
        else:
            try:
                self.logger.info("[BATCH] Tentando processamento em batch unificado.")
                self.process_data()
                self.logger.info("[BATCH] Processamento em batch concluído com sucesso.")

            except Exception as e:
                error_message = str(e)
                self.logger.warning(
                    f"[BATCH] Falha no processamento em batch. "
                    f"Acionando processamento por grupos de schema.\nErro: {error_message}"
                )
                self.process_by_schema_groups()

    def process_data(self):
        self.logger.debug("Lendo dados...")
        data = self.repository.read()

        if data is None:
            self.logger.info("Nenhum arquivo para processar.")
            return 0
        
        if "dataset_origem" in data.columns:
            try:
                first_row = data.select("dataset_origem").first()
                if first_row:
                    self.db_name = first_row["dataset_origem"]
                self.logger.info(f"Valor de dataset_origem extraido")
            except Exception as e:
                self.logger.info(f"Não foi possível extrair db_name para telemetria: {e}")
        else:
            self.logger.info(f"dataset_origem não encontrado")

        input_count = data.count()
        if input_count == 0:
            self.logger.debug("Arquivo vazio detectado.")
            return 0
        print(input_count)
        self.logger.debug("Aplicando RawDataVaultInitTransformer...")
        data = self.raw_data_vault_init_transformer.transform(data)

        self.logger.debug("Aplicando HardBusinessRulesTransformer...")
        data = self.hard_business_rules_transformer.transform(data)

        self.logger.debug("Aplicando ValidateBusinessRulesTransformer...")
        data = self.validate_business_rules_transformer.transform(data)

        self._gravar_e_limpar(data)
        return 1

    def process_by_schema_groups(self):
        """
        Agrupa os arquivos Parquet pendentes por schema físico e processa cada grupo
        em uma única leitura/transformação. Reduz o número de ciclos de I/O e de chamadas
        aos transformers de N arquivos para N schemas distintos.
        """
        self.logger.info("Iniciando agrupamento de arquivos por schema...")
        schema_groups = self.repository.get_files_grouped_by_schema()

        if not schema_groups:
            self.logger.info("Nenhum arquivo encontrado para processar.")
            return

        total_groups = len(schema_groups)
        self.logger.info(f"Schemas distintos encontrados: {total_groups}")

        for group_index, (schema_key, file_names) in enumerate(schema_groups.items(), start=1):
            start_time = time.time()
            self.logger.info(
                f"[SCHEMA {group_index}/{total_groups}] Processando {len(file_names)} arquivo(s)."
            )

            try:
                data = self.repository.read_files_group(file_names)

                input_count = data.count()
                self.logger.info(f"[SCHEMA {group_index}] {input_count} registros extraídos.")

                if input_count == 0:
                    self.logger.debug(f"[SCHEMA {group_index}] Grupo sem registros, pulando.")
                    continue

                self.logger.debug("Aplicando RawDataVaultInitTransformer...")
                data = self.raw_data_vault_init_transformer.transform(data)

                self.logger.debug("Aplicando HardBusinessRulesTransformer...")
                data = self.hard_business_rules_transformer.transform(data)

                self.logger.debug("Aplicando ValidateBusinessRulesTransformer...")
                data = self.validate_business_rules_transformer.transform(data)

                self._gravar_e_limpar(data)

                duration = round(time.time() - start_time, 2)
                self.logger.info(f"[SCHEMA {group_index}] Concluído em {duration}s")

            except Exception as e:
                self.logger.error(
                    f"[SCHEMA {group_index}] Erro ao processar grupo: {str(e)}", exc_info=True
                )
                raise

        self.logger.info("Processamento por grupos de schema concluído com sucesso.")

    def process_one_file_per_execution(self):
        self.logger.info("Iniciando busca de arquivos para processamento...")
        files_to_process = self.repository.get_origin_list_file_names()

        if not files_to_process:
            self.logger.info("Nenhum arquivo novo encontrado para processar.")
            return

        self.logger.info(f"Total de arquivos detectados: {len(files_to_process)}")

        for index, file_name in enumerate(files_to_process, start=1):
            start_time = time.time()
            self.logger.info(f"Processando arquivo {index}/{len(files_to_process)}: {file_name}")

            try:
                self.logger.debug(f"[{file_name}] Iniciando leitura...")
                data = self.repository.read_file(file_name)

                input_count = data.count()
                self.logger.info(f"[{file_name}] Extraídos {input_count} registros.")

                if input_count == 0:
                    self.logger.debug(f"[{file_name}] Arquivo vazio.")
                    continue

                self.logger.debug(f"[{file_name}] Aplicando RawDataVaultInitTransformer...")
                data = self.raw_data_vault_init_transformer.transform(data)

                self.logger.debug(f"[{file_name}] Aplicando HardBusinessRulesTransformer...")
                data = self.hard_business_rules_transformer.transform(data)

                self.logger.debug(f"[{file_name}] Escrevendo e limpando...")
                self._gravar_e_limpar(data)

                duration = round(time.time() - start_time, 2)
                self.logger.info(f"[{file_name}] Finalizado em {duration}s")

            except Exception as e:
                self.logger.error(
                    f"Erro no arquivo {file_name}: {str(e)}",
                    exc_info=True
                )
                raise

        self.logger.info("Processamento finalizado com sucesso.")


class PipelineSilverSuperTenant(PipelineProcess):
    """
    Camada Silver de um SUPER TENANT: consolida as Silvers ja processadas de N clientes.

    O modelo padrao do Honeycomb e um Tenant com N filiais. Um super tenant (um fabricante que
    revende o sistema embarcado) precisa de um modelo a mais: cada CLIENTE dele vira uma FILIAL
    do super tenant, sem que o tenant do cliente mude em nada.

    Herda de PipelineProcess, e nao de Pipeline, por tres motivos concretos:

    1. `Pipeline.run()` chama `data.count()` DUAS vezes so para logar volumetria. Aqui cada count
       e uma varredura completa das Silvers de N buckets.
    2. `Pipeline.run()` assume UM DataFrame no fluxo extract->transform->save. A unidade de
       trabalho aqui e o par (tabela, tenant): N leituras e N merges, cada um com o SEU predicado
       de particao.
    3. `preparer()` tem trabalho real e separavel — descobrir quais origens existem e resolver o
       schema alvo ANTES de tocar em qualquer dado. Se o schema divergir entre clientes, o job
       precisa falhar antes de escrever a primeira linha.

    E tambem o que faz o irmao mais proximo, PipelineBronzeToSilver — o outro escritor de Silver.

    NENHUM transformer e aplicado: o dado ja passou por RawDataVaultInitTransformerAuto,
    HardBusinessRulesTransformerAuto e ValidateBusinessRulesTransformer no tenant de origem. O
    que muda e a REIDENTIFICACAO da filial, nao o conteudo.
    """

    def __init__(self, spark, environment_parameters, runtime_parameters):
        super().__init__(spark)
        self.runtime_parameters = runtime_parameters
        self.table_name = runtime_parameters["table_name"]
        from repo.repository import RepositorySilverSuperTenant
        self.repository = RepositorySilverSuperTenant(
            spark, environment_parameters, runtime_parameters)
        self.origens = []
        self.target_schema = None

    def preparer(self):
        """Descobre as origens e resolve o schema alvo antes de qualquer escrita."""
        self.origens = self.repository.origens_disponiveis()
        self.target_schema = self.repository.resolver_schema_alvo(self.origens)
        self.repository.evoluir_schema_destino(self.target_schema)

        self.logger.info(
            f"[{self.table_name}] {len(self.origens)} origem(ns): "
            f"{[o['tenant'] for o in self.origens]}; "
            f"{len(self.target_schema)} coluna(s) no schema alvo."
        )

    def process(self):
        """
        Consolida uma origem por vez, SEQUENCIALMENTE.

        Sequencial de proposito: as N origens escrevem na MESMA tabela Delta, e paralelizar aqui
        recriaria dentro de um unico pod a corrida entre filiais que o predicado de particao
        resolve entre pods. Alem disso o TableRunner ja paraleliza por TABELA — paralelizar
        tambem por tenant multiplicaria `max_workers x len(source_tenants)` leituras full
        concorrentes sobre a memoria dos executors.
        """
        falhas = []
        total = len(self.origens)

        for indice, origem in enumerate(self.origens, start=1):
            tenant = origem["tenant"]
            inicio = time.time()

            try:
                self.logger.info(
                    f"[{self.table_name}] [{indice}/{total}] consolidando {tenant}."
                )
                data = self.repository.read(tenant, origem["path"], self.target_schema)
                self.repository.write(data, source=f"data-bee_{tenant}")

                duracao = round(time.time() - inicio, 2)
                self.logger.info(f"[{self.table_name}] {tenant} OK em {duracao}s.")

            except Exception as e:
                # Isolamento por ORIGEM, espelhando o isolamento por tabela do TableRunner: um
                # cliente com problema nao pode impedir a consolidacao dos outros. Mas a falha e
                # RELANCADA no fim — nunca engolida — para que a tabela conste como FALHA no
                # relatorio e o exit code do driver marque a SparkApplication como FAILED.
                self.logger.exception(f"[{self.table_name}] FALHA em {tenant}: {str(e)}")
                falhas.append(f"{tenant}: {type(e).__name__}: {e}")

        if falhas:
            raise RuntimeError(
                f"[{self.table_name}] {len(falhas)}/{total} origem(ns) falharam: {falhas}"
            )


class PipelineDeltaMaintenance(PipelineProcess):
    """
    Manutencao de UMA tabela Delta: OPTIMIZE seguido de VACUUM.

    Existe porque nenhum dos pipelines de ingestao limpa o que escreve. O bronze_silver faz
    MERGE de N filiais na mesma tabela e nao compacta nem remove os arquivos antigos; o gold
    compacta a cada escrita mas nunca roda VACUUM. O resultado sao diretorios `.delta` no MinIO
    dominados por arquivos que nenhuma versao ativa referencia.

    A ORDEM IMPORTA: OPTIMIZE primeiro (reescreve os arquivos pequenos em poucos grandes,
    marcando os antigos como removidos), VACUUM depois (apaga fisicamente o que foi removido ha
    mais tempo que a retencao). Os arquivos que o OPTIMIZE acabou de marcar so serao apagados
    numa execucao futura, quando ultrapassarem a janela de retencao — o ganho de espaco da
    primeira execucao vem do acumulo historico, nao do OPTIMIZE desta.

    Nao escreve dados nem mexe no catalogo: OPTIMIZE/VACUUM nao mudam a localizacao da tabela,
    entao nao ha nada a re-registrar no Trino.
    """

    def __init__(self, spark, environment_parameters, runtime_parameters):
        super().__init__(spark)
        self.table_name = runtime_parameters.get("table_name")
        self.layer = runtime_parameters.get("delta_layer") or "silver"
        self.output_path = resolve_delta_path(
            environment_parameters, self.table_name, self.layer)
        self.z_order_columns = normalize_zorder_columns(
            runtime_parameters.get("colunas_zorder"))
        self.retention_hours = int(runtime_parameters.get("retention_hours") or 168)

        self.logger.info(
            f"Manutencao Delta preparada para {self.table_name} "
            f"(camada={self.layer}, path={self.output_path}, "
            f"retencao={self.retention_hours}h, z-order={self.z_order_columns})"
        )

    def preparer(self):
        """
        Confirma que o destino e mesmo uma tabela Delta antes de tocar nela.

        `DeltaMaintenance.__init__` ja faz `DeltaTable.forPath`, mas o erro dele nao diz QUAL
        tabela do lote estava errada. Um nome de tabela digitado errado no `--tables_json`
        precisa falhar aqui, nomeado, e ser contabilizado pelo TableRunner como a falha daquela
        tabela — nao como sucesso silencioso.
        """
        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(self.spark, self.output_path):
            raise ValueError(
                f"'{self.output_path}' nao e uma tabela Delta. Verifique o nome da tabela "
                f"({self.table_name!r}) e a camada (--delta_layer {self.layer})."
            )

    def process(self):
        from utils.delta_maintenance import DeltaMaintenance
        maintenance = DeltaMaintenance(self.spark, self.output_path)

        inicio = time.time()
        maintenance.run_optimize(self.z_order_columns)
        self.logger.info(
            f"[{self.table_name}] OPTIMIZE em {round(time.time() - inicio, 2)}s."
        )

        inicio = time.time()
        maintenance.run_vacuum(self.retention_hours)
        self.logger.info(
            f"[{self.table_name}] VACUUM em {round(time.time() - inicio, 2)}s."
        )
