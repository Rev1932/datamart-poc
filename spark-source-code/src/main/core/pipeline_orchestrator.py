import time
import pyspark.sql.functions as F
import logging
import re
from core.pipeline import  Pipeline, PipelineProcess
from repo.repository import RepositoryGoldDataVaults, RepositoryBronzeToSilver
from tranformer.transformer import ValidateBusinessRulesTransformer, RawDataVaultInitTransformerAuto, HardBusinessRulesTransformerAuto
from utils.clickhouse_connection import ClickHouseConnection
from utils.handler_logger import initialize_logger


class PipelineGold(Pipeline):

    def __init__(self, spark, config_application, params_config, queries_dir: str):
        super().__init__(spark, config_application, params_config)
        self.config_application = config_application

        self.repository = RepositoryGoldDataVaults(self.spark, self.config_application, params_config, queries_dir)

    def extract(self):
        return self.repository.read()

    def transform(self, data):
        return data

    def save(self, data):
        return self.repository.write(data)


class PipelineGoldClickHouse(PipelineGold):
    """
    Datamart: reusa exatamente a leitura da gold (query sobre a silver + hk_business_id
    + load_dts) e, no lugar de gravar Delta no MinIO, grava o DataFrame no ClickHouse.
    Só o `save` muda em relação à PipelineGold.
    """

    def __init__(self, spark, config_application, params_config, queries_dir: str):
        super().__init__(spark, config_application, params_config, queries_dir)
        self.params_config = params_config
        self.clickhouse = ClickHouseConnection(self.config_manager)

    def save(self, data):
        return self.clickhouse.write(data, self.params_config["table_name"])


class PipelineBronzeToSilver(PipelineProcess):
    def __init__(self, spark, config_application, config_params):

        super().__init__(spark, config_application, config_params)
        self.logger = initialize_logger()
        self.config_params = config_params
        self.repository = RepositoryBronzeToSilver(spark, config_application, config_params)
        self.raw_data_vault_init_transformer = RawDataVaultInitTransformerAuto(config_application, config_params)
        self.hard_business_rules_transformer = HardBusinessRulesTransformerAuto(config_application, config_params)
        self.validate_business_rules_transformer = ValidateBusinessRulesTransformer()
                        
        self.logger.info(f"Inicialização de {config_params["config_name"]} para o processo {config_params["pipeline_type"]} com sucesso.")

    def _extract_problematic_file(self, error_message: str) -> str | None:
        """Extrai o path do arquivo problemático da mensagem de erro do Spark."""
        match = re.search(r'file\s+s3a?://[^/]+/([^\s]+\.parquet)', str(error_message))
        return match.group(1) if match else None

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

        self.repository.write(data)
        self.repository.cleaning_processed(data)

        duration = round(time.time() - start_time, 2)
        self.logger.info(f"[FALLBACK] Arquivo {file_path} processado em {duration}s")


    def preparer(self):
        self.logger.debug("Executando preparer (sem tarefas pendentes).")

    def process(self):

        table_process_exception = ['']
        if self.config_params["table_name"] in table_process_exception:
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

        self.repository.write(data)

        self.repository.cleaning_processed(data)
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

                self.repository.write(data)
                self.repository.cleaning_processed(data)

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
            return 0

        self.logger.info(f"Total de arquivos detectados: {len(files_to_process)}")
        processed_count = 0

        for index, file_name in enumerate(files_to_process, start=1):
            start_time = time.time()
            self.logger.info(f"Processando arquivo {index}/{len(files_to_process)}: {file_name}")

            try:
                self.logger.debug(f"[{file_name}] Iniciando leitura...")
                data = self.repository.read_file(file_name)

                if not self.db_name and "dataset_origem" in data.columns:
                    try:
                        first_row = data.select("dataset_origem").first()
                        if first_row:
                            self.db_name = first_row["dataset_origem"]
                    except:
                        pass

                input_count = data.count()
                self.logger.info(f"[{file_name}] Extraídos {input_count} registros.")

                if input_count == 0:
                    self.logger.debug(f"[{file_name}] Arquivo vazio.")
                    continue

                self.logger.debug(f"[{file_name}] Aplicando RawDataVaultInitTransformer...")
                data = self.raw_data_vault_init_transformer.transform(data)

                self.logger.debug(f"[{file_name}] Aplicando HardBusinessRulesTransformer...")
                data = self.hard_business_rules_transformer.transform(data)

                output_count = data.count()
                self.logger.info(f"[{file_name}] Registros finais: {output_count}")

                self.logger.debug(f"[{file_name}] Escrevendo dados...")
                self.repository.write(data)

                self.logger.debug(f"[{file_name}] Limpando arquivos...")
                self.repository.cleaning_processed(data)

                duration = round(time.time() - start_time, 2)
                self.logger.info(f"[{file_name}] Finalizado em {duration}s")
                processed_count += 1

            except Exception as e:
                self.logger.error(
                    f"Erro no arquivo {file_name}: {str(e)}",
                    exc_info=True
                )
                raise

        self.logger.info("Processamento finalizado com sucesso.")
        return processed_count