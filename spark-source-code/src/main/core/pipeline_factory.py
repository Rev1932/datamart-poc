from .pipeline_orchestrator import (
  PipelineBronzeToSilver,
  PipelineDeltaMaintenance,
  PipelineGold,
  PipelineDatamartClickhouse,
  PipelineGoldDatamart,
  PipelineSilverSuperTenant
  )
from utils.handler_logger import initialize_logger, apply_and_trace_context

# init global logger
logger_base = initialize_logger()


class PipelineGoldWrapper():
  '''
    Processo para gerar tabelas com valor de negocio, salvo em delta_lake
  '''
  def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir):
    self.spark = spark
    self.environment_parameters = environment_parameters
    self.runtime_parameters = runtime_parameters
    self.queries_dir = queries_dir
    self.PIPELINE_NAME = runtime_parameters['pipeline']

  def run(self):
    gold = PipelineGold(self.spark, self.environment_parameters, self.runtime_parameters, self.queries_dir)
    gold.run()
    from utils.trino_connection import TrinoConnection
    trino = TrinoConnection(self.environment_parameters)
    trino.create_table(self.runtime_parameters['table_name'], self.PIPELINE_NAME)


class PipelineGoldDatamartWrapper():
  """
  Processo para gerar tabelas com valor de negocio, salvo no datamart do Tenant. 
  """
  def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir):
    self.spark = spark
    self.environment_parameters = environment_parameters
    self.runtime_parameters = runtime_parameters
    self.queries_dir = queries_dir

  def run(self):
    gold_datamart = PipelineGoldDatamart(self.spark, self.environment_parameters, self.runtime_parameters, self.queries_dir)
    gold_datamart.run()


class PipelineDatamartClickhouseWrapper():
  """
  Processo para gerar tabelas com valor de negocio, salvo no datamart ClickHouse do Tenant.
  """
  def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir):
    self.spark = spark
    self.environment_parameters = environment_parameters
    self.runtime_parameters = runtime_parameters
    self.queries_dir = queries_dir

  def run(self):
    datamart = PipelineDatamartClickhouse(
      self.spark, self.environment_parameters, self.runtime_parameters, self.queries_dir)
    datamart.run()


class PipelineBronzeToSilverWrapper():
  def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir=None):
    self.spark = spark
    self.environment_parameters = environment_parameters
    self.runtime_parameters = runtime_parameters

  def run(self):
    landing = PipelineBronzeToSilver(self.spark, self.environment_parameters, self.runtime_parameters)
    landing.run()
    from utils.trino_connection import TrinoConnection
    trino = TrinoConnection(self.environment_parameters)
    trino.schema_folder = self.environment_parameters.get("trino.schema_folder_silver")
    trino.create_table(self.runtime_parameters["table_name"])


class PipelineSilverSuperTenantWrapper():
  """
  Silver de super tenant: N Silvers de clientes -> 1 Silver consolidada.

  Registra no Trino com o MESMO `trino.schema_folder_silver` do bronze_silver: para o Trino e
  para os `.sql` do gold, a Silver do super tenant e indistinguivel de qualquer outra.

  `primary_key`/`chave_pk` sao IGNORADOS aqui — o hk_business_id vem re-hasheado do hk ja
  calculado na origem, nao das colunas de negocio. Continuam obrigatorios so porque as guardas do
  main.py e do normalize_tables valem para todos os pipelines (mesma situacao do
  delta_maintenance). `--is_merge_schema` tambem e ignorado: a uniao de schemas e explicita.
  """
  def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir=None):
    self.spark = spark
    self.environment_parameters = environment_parameters
    self.runtime_parameters = runtime_parameters
    self.queries_dir = queries_dir

  def run(self):
    consolidacao = PipelineSilverSuperTenant(
      self.spark, self.environment_parameters, self.runtime_parameters)
    consolidacao.run()
    from utils.trino_connection import TrinoConnection
    trino = TrinoConnection(self.environment_parameters)
    trino.schema_folder = self.environment_parameters.get("trino.schema_folder_silver")
    trino.create_table(self.runtime_parameters["table_name"])


class PipelineDeltaMaintenanceWrapper():
  """
  Manutencao (OPTIMIZE + VACUUM) das tabelas Delta no MinIO.

  Nao e um pipeline de ingestao: nao le origem, nao escreve dado novo e nao registra nada no
  Trino — OPTIMIZE/VACUUM nao mudam a localizacao da tabela. Recebe `queries_dir` apenas para
  manter a assinatura posicional que o factory chama.

  `primary_key`/`chave_pk` sao IGNORADOS aqui; continuam obrigatorios so porque as guardas do
  main.py e do normalize_tables valem para todos os pipelines.
  """
  def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir=None):
    self.spark = spark
    self.environment_parameters = environment_parameters
    self.runtime_parameters = runtime_parameters
    self.queries_dir = queries_dir

  def run(self):
    manutencao = PipelineDeltaMaintenance(
      self.spark, self.environment_parameters, self.runtime_parameters)
    manutencao.run()


class PipelineFactory():
  """
  Hub de orquestração para a execução de diferentes pipelines.
  Atua como uma camada de abstração para gerenciar o fluxo de ingestão.
  """
  def __init__(self):
    # Mapeamento dos pipelines disponíveis
    self.pipelines = {
      "gold": PipelineGoldWrapper,
      "gold_datamart": PipelineGoldDatamartWrapper,
      "datamart_ch": PipelineDatamartClickhouseWrapper,
      "bronze_silver": PipelineBronzeToSilverWrapper,
      "silver_super_tenant": PipelineSilverSuperTenantWrapper,
      "delta_maintenance": PipelineDeltaMaintenanceWrapper,
    }

  def new_instance(self, key: str, spark, environment_parameters, runtime_parameters, queries_dir):
      logger = initialize_logger()

      logger.info(f"Criando pipeline: {key}")

      if key in self.pipelines:
          classe = self.pipelines[key]
          return classe(spark, environment_parameters, runtime_parameters, queries_dir)
      else:
          logger.error(f"Pipeline não encontrado: {key}")
          raise Exception(f"Erro: Tipo de classe '{key}' não encontrado.")

