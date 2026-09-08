from pyspark.sql import SparkSession, functions as F
from typing import Optional
from datetime import datetime
import math
import logging

class Helper:
    logger = logging.getLogger("AppLogger")

    @staticmethod
    def get_max_load_timestamp(spark: SparkSession, col_name: str, origin_path: str, partition_filter: dict=None) -> Optional[datetime]:
        """Recupera o timestamp máximo de uma coluna em uma tabela Delta.

        Args:
            spark: Sessão Spark ativa.
            table_path: Caminho ou nome da tabela Delta no catálogo.
            col_name: Nome da coluna de timestamp para busca.
            partition_filter: Filtro de partição para aplicar na leitura da tabela.

        Returns:
            O valor máximo encontrado ou None se a tabela estiver vazia/inexistente.
        """
        try:
            Helper.logger.info(f"[DEBUG] {origin_path}")
            df = spark.read.format("delta").load(origin_path)
            Helper.logger.info(f"[DEBUG] {df.schema}")
            Helper.logger.info(f"[DEBUG] {df.count()}")
            if partition_filter and isinstance(partition_filter, dict):
                for key, value in partition_filter.items():
                    df = df.where(f"{key}='{value}'")
                
            max_val = df.select(F.max(col_name)).collect()[0][0]
            return max_val
        except Exception as e:
            Helper.logger.warning(f"Aviso: Não foi possível ler o timestamp (tabela nova?): {e}")
            return None

    @staticmethod
    def calculate_num_chunks(total_records: int, records_per_chunk: int = 1_000_000) -> int:
        """Calcula a quantidade de lotes (chunks) baseada no volume de dados.

        Args:
            total_records: Total de registros.
            records_per_chunk: Valor de referência para o tamanho de cada lote.

        Returns:
            Um inteiro representando o número de chunks (mínimo 1).
        """
        if total_records <= records_per_chunk:
            num_chunks = 1
        else:
            num_chunks = math.ceil(total_records / records_per_chunk)
            
        Helper.logger.info(f"Total de registros: {total_records}. "
            f"Quantidade de Chunks definida: {num_chunks}")
            
        return num_chunks              