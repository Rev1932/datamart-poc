import logging
from abc import ABC, abstractmethod
from utils.delta_maintenance import DeltaMaintenance
from utils.log import GerenciamentoIngestoesSender
from datetime import datetime
from utils.handler_logger import initialize_logger

class Pipeline(ABC):
    """
    Classe base abstrata para pipelines de dados

    Implementa o padão Template Method para pipelines de processamento de dados. 
    Define o esqueleto do processo ETL enquanto permite que subclasses substituam etapas específicas.
    """

    def __init__(self, spark, config_application=None, config_params=None, config_manager=None):
        """
        Inicializa o pipeline

        Args:
            spark: SparkSession para usar no pipeline
        """
        self.spark = spark
        self.config_application = config_application
        self.config_params = config_params
        self.logger = initialize_logger()
        if config_manager is None:
            from utils.config_manager import ConfigManager
            config_manager = ConfigManager()
        
        self.config_manager = config_manager
        
        # Inicializamos o sender (Se as configs existirem)
        self.sender = None
        if self.config_application:
            try:
                # Busque as credenciais onde você as guarda (ex: yaml, env)
                url = self.config_manager.get("sqlserver.url")
                user = self.config_manager.get("sqlserver.user")
                password = self.config_manager.get("sqlserver.password")
                self.sender = GerenciamentoIngestoesSender(url, user, password)
            except Exception as e:
                self.logger.warning(f"Aviso: Não foi possível instanciar a API de Ingestões: {e}")

    def _enviar_telemetria(self, status: str, count: int = None, desc_error: str = None, last_run: str = None, last_process_run: str = None):
        """Método interno para padronizar o envio para a API."""
        if not self.sender or not self.config_params:
            return

        try:
            nome_config = self.config_params.config_name

            # O Payload que indica se rodou ou quebrou
            itens = [{"status_execucao": status}]

            self.sender.enviar_async(
                itens=itens,
                identificador="pipeline_orquestrador",
                tenant=nome_config,
                filial="gold",
                db_name="gold",
                table_name=self.config_params.table_name,
                camada=self.config_params.pipeline_type,
                count=count,
                desc_error=desc_error,
                last_error_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S") if desc_error else None,
                last_run=last_run,
                last_process_run=last_process_run
            )
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Erro durante o envio de telemetria: {error_msg}")

    @abstractmethod
    def extract(self):
        """
        Extrai dados da origem

        Returns:
            DataFrame: Dados extraídos da origem
        """
        pass

    @abstractmethod 
    def transform(self, data):
        """
        Transforma os dados

        Args:
            data: DataFrame de dados extraídos

        Returns:
            DataFrame: Dados transformados
        """
        pass

    @abstractmethod 
    def save(self, data):
        """
        Salva dados para o destino

        Args:
            data: DataFrame para carregar
        """
        pass

    def run(self):  
        """
        Executa o pipeline completo

        Este é o Método de Modelo que define a execução padrão
        flow: extract -> transform -> save, com tratamento de erros.

        Returns:
            bool: True se o pipeline foi concluído com sucesso, False caso contrário
        """
        self.logger.info("Iniciando pipeline...")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            data = self.extract()
            # Capturamos a contagem inicial se os dados não forem nulos
            input_count = data.count() if data else 0
            self.logger.info(f"Extraídos {input_count} registros")

            transformed_data = self.transform(data)
            output_count = transformed_data.count() if transformed_data else 0
            self.logger.info(f"Dados transformados possuem {output_count} registros")
            self.save(transformed_data)
            self.logger.info("Pipeline concluído com sucesso!")

            last_process_run = now_str if output_count > 0 else None
            self._enviar_telemetria(status="SUCESSO", count=output_count, last_run=now_str, last_process_run=last_process_run)
            return True

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Erro durante a execução do pipeline: {error_msg}")
            
            self._enviar_telemetria(status="ERRO CRITICO", desc_error=error_msg, last_run=now_str)
            raise
            

class PipelineProcess(ABC):
    """
    Classe base abstrata para pipelines de dados

    Implementa o padão Template Method para pipelines de processamento de dados. 
    Define o esqueleto de preparação de dados para e execução.
    """

    def __init__(self, spark, config_application=None, config_params=None, config_manager=None): 
        """
        Inicializa o pipeline

        Args:
            spark: SparkSession para usar no pipeline
        """
        self.spark = spark
        self.config_application = config_application
        self.config_params = config_params
        self.logger = initialize_logger()

        if config_manager is None:
        # Tenta criar um ConfigManager padrão se não for fornecido
            from utils.config_manager import ConfigManager
            config_manager = ConfigManager()
        
        self.config_manager = config_manager
        
        # Inicializamos o sender (Se as configs existirem)
        self.sender = None
        if self.config_application:
            try:
                # Busque as credenciais onde você as guarda (ex: yaml, env)
                url = self.config_manager.get("sqlserver.url")
                user = self.config_manager.get("sqlserver.user")
                password = self.config_manager.get("sqlserver.password")
                self.sender = GerenciamentoIngestoesSender(url, user, password)
            except Exception as e:
                self.logger.warning(f"Aviso: Não foi possível instanciar a API de Ingestões: {e}")
    
    def _enviar_telemetria(self, status: str, count: int = None, desc_error: str = None, last_run: str = None, last_process_run: str = None, filial: str = None, db_name_override: str = None):
        """Método interno para padronizar o envio para a API."""
        if not self.sender or not self.config_params:
            return

        try:
            nome_config = self.config_params.config_name

            # O Payload que indica se rodou ou quebrou
            itens = [{"status_execucao": status}]

            self.sender.enviar_async(
                itens=itens,
                identificador="pipeline_orquestrador",
                tenant=nome_config,
                filial=filial if filial else nome_config.upper(),
                db_name=db_name_override if db_name_override else f"dw_{nome_config}",
                table_name=self.config_params.table_name,
                camada=self.config_params.pipeline_type,
                count=count,
                desc_error=desc_error,
                last_error_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S") if desc_error else None,
                last_run=last_run,
                last_process_run=last_process_run
            )
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Erro durante o envio de telemetria: {error_msg}")
    
    @abstractmethod 
    def preparer(self):
        """
        Prepara os dados para a execução

        Returns:
            DataFrame: Dados extraídos da origem
        """
        pass

    @abstractmethod 
    def process(self):
        """
        Processa os dados

        Returns:
            int: Quantidade de itens processados
        """
        pass

    def run(self):  
        """
        Executa o pipeline completo

        Este é o Método de Modelo que define a execução padrão
        flow: preparer -> process, com tratamento de erros.

        Returns:
            bool: True se o pipeline foi concluído com sucesso, False caso contrário
        """
        self.logger.info("Iniciando pipeline...")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            data = self.preparer()

            process_count, process_filial, process_dbname = self.process()
            self.logger.info("Pipeline concluído com sucesso!")

            last_process_run = now_str if process_count and process_count > 0 else None
            self._enviar_telemetria(
                status="SUCESSO",
                count=process_count,
                last_run=now_str,
                last_process_run=last_process_run,
                filial=process_filial,
                db_name_override=process_dbname
            )
            return True

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Erro durante a execução do pipeline: {error_msg}")
            
            self._enviar_telemetria(status="ERRO CRITICO", desc_error=error_msg, last_run=now_str)
            return False