
from utils.trino_connection import TrinoConnection
from .pipeline_orchestrator import (
  PipelineBronzeToSilver,
  PipelineGold,
  PipelineGoldClickHouse
  )
from utils.trino_connection import TrinoConnection
from utils.minio_admin import MinioAdmin
from utils.handler_logger import initialize_logger, apply_and_trace_context

# init global logger
logger_base = initialize_logger()

class PipelineOeeCleaner():
  def __init__(self, spark, config_application, config_params, query_config=None):
    self.spark = spark
    self.config_application = config_application
    self.config_params = config_params

  def run(self):
    minio_admin  = MinioAdmin(self.config_application.get("spark.s3"))
    minio_admin.cleaning_temp_oee()


class PipelineGoldWrapper():
  def __init__(self, spark, config_application, config_params, query_config):
    self.spark = spark
    self.config_application = config_application
    self.config_params = config_params
    self.query_config = query_config
    self.PIPELINE_NAME = "gold"

  def run(self):
    gold = PipelineGold(self.spark, self.config_application, self.config_params, self.query_config)
    gold.run() 
    trino = TrinoConnection(self.config_application) 
    trino.create_table(self.config_params['table_name'], self.query_config, self.PIPELINE_NAME)


class PipelineDatamartWrapper():
  """
  Ingestão do datamart no ClickHouse. Reusa a leitura da gold via
  PipelineGoldClickHouse e grava no ClickHouse (sem passo Trino).
  """
  def __init__(self, spark, config_application, config_params, query_config):
    self.spark = spark
    self.config_application = config_application
    self.config_params = config_params
    self.query_config = query_config

  def run(self):
    datamart = PipelineGoldClickHouse(self.spark, self.config_application, self.config_params, self.query_config)
    datamart.run()


class PipelineBronzeToSilverWrapper():
  def __init__(self, spark, config_application, config_params, query_config=None):
    self.spark = spark
    self.config_application = config_application
    self.config_params = config_params

  def run(self):
    landing = PipelineBronzeToSilver(self.spark, self.config_application, self.config_params)
    landing.run()
    trino = TrinoConnection(self.config_application)
    trino.schema_folder = self.config_application.get("trino.schema_folder_silver")
    trino.create_table(self.config_params["table_name"])


class PipelineFactory():
  """
  Hub de orquestração para a execução de diferentes pipelines.
  Atua como uma camada de abstração para gerenciar o fluxo de ingestão.
  """
  def __init__(self):
    # Mapeamento dos pipelines disponíveis
    self.pipelines = {
      "gold": PipelineGoldWrapper,
      "datamart": PipelineDatamartWrapper,
      "oee_cleaner": PipelineOeeCleaner,
      "bronze_silver": PipelineBronzeToSilverWrapper,
    }

  def new_instance(self, key: str, spark, config_application, config_params, query_config):
      logger = initialize_logger()

      logger.info(f"Criando pipeline: {key}")

      if key in self.pipelines:
          classe = self.pipelines[key]
          return classe(spark, config_application, config_params, query_config)
      else:
          logger.error(f"Pipeline não encontrado: {key}")
          raise Exception(f"Erro: Tipo de classe '{key}' não encontrado.")

