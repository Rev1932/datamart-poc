from abc import ABC, abstractmethod

class Repository(ABC):
    """
    Classe base abstrata para repositórios

    Implementa o Repository Pattern base definindo interfaces padrão
    para leitura e escrita de dados, independentemente do armazenamento subjacente.
    """
    def __init__(self, spark):
        """
        Initialize the repository (Inicializa o repositório)
        Args:
            spark: SparkSession to use for data access (spark: SparkSession para usar para acesso a dados)
        """
        self.spark = spark

    @abstractmethod
    def read(self, **kwargs):
        """
        Lê dados da origem

        Args:
            **kwargs: Parâmetros adicionais para leitura

        Returns:
            DataFrame: Dados lidos da origem
        """
        pass

    @abstractmethod
    def write(self, data, **kwargs):
        """
        Escreve dados para o destino

        Args:
            data: DataFrame para escrever
            **kwargs: Parâmetros adicionais para escrita
        """
        pass