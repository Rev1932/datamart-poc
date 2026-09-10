import logging
from abc import ABC, abstractmethod
from utils.delta_maintenance import DeltaMaintenance
from utils.handler_logger import initialize_logger

class Pipeline(ABC):
    """
    Classe base abstrata para pipelines de dados

    Implementa o padão Template Method para pipelines de processamento de dados. 
    Define o esqueleto do processo ETL enquanto permite que subclasses substituam etapas específicas.
    """

    def __init__(self, spark): 
        """
        Inicializa o pipeline

        Args:
            spark: SparkSession para usar no pipeline
        """
        self.spark = spark
        
        self.logger = initialize_logger()

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

        try:
            data = self.extract()
            self.logger.info(f"Extraídos {data.count()} registros")

            transformed_data = self.transform(data)
            self.logger.info(f"Dados transformados possuem {transformed_data.count()} registros")

            self.save(transformed_data)
            self.logger.info("Pipeline concluído com sucesso!")
            return True

        except Exception as e:
            self.logger.error(f"Erro durante a execução do pipeline: {str(e)}")
            raise
            

class PipelineProcess(ABC):
    """
    Classe base abstrata para pipelines de dados

    Implementa o padão Template Method para pipelines de processamento de dados. 
    Define o esqueleto de preparação de dados para e execução.
    """

    def __init__(self, spark): 
        """
        Inicializa o pipeline

        Args:
            spark: SparkSession para usar no pipeline
        """
        self.spark = spark
        self.logger = initialize_logger()

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

        Args:
            data: DataFrame para carregar
        """
        pass

    def run(self):  
        """
        Executa o pipeline completo

        Este é o Método de Modelo que define a execução padrão
        flow: preparer -> process, com tratamento de erros.

        Returns:
            bool: True se o pipeline foi concluído com sucesso.

        Raises:
            Exception: qualquer falha em preparer/process é RELANÇADA.
        """
        self.logger.info("Iniciando pipeline...")

        try:
            data = self.preparer()

            self.process()
            self.logger.info("Pipeline concluído com sucesso!")
            return True

        except Exception as e:
            # Relanca (antes: `return False`). O chamador precisa distinguir sucesso de falha:
            # no modo single-table quem decide o exit code do driver e o main(), e no modo
            # multi-tabela e o TableRunner que contabiliza a tabela como falha. Engolir a
            # excecao aqui fazia um job quebrado sair com exit code 0 — a SparkApplication
            # ficava COMPLETED e a limpeza dos arquivos de origem seguia como se tivesse dado
            # certo (perda de dado silenciosa).
            self.logger.error(f"Erro durante a execução do pipeline: {str(e)}")
            raise