from abc import ABC, abstractmethod

class Transformer(ABC): 
    """
    Classe base abstrata para transformadores de dados

    Implementa o Padrão Strategy definindo uma interface comum
    para diferentes algoritmos de transformação que podem ser selecionados
    e trocados em tempo de execução.
    """
    @abstractmethod
    def transform(self, data):
        """
        Transforma os dados de entrada

        Args:
            data: DataFrame para transformar

        Returns:
            DataFrame: Dados transformados
        """
        pass