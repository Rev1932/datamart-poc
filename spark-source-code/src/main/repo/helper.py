from pyspark.sql import Column, SparkSession, functions as F
from pyspark.sql.types import NullType, StructField, StructType
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import math
import logging

class Helper:
    logger = logging.getLogger("AppLogger")

    @staticmethod
    def build_schema_projection(present_columns: List[str], target_schema: StructType) -> List[Column]:
        """Alinha um DataFrame a um schema alvo: casta o que existe, injeta NULL tipado no resto.

        Extraido de `RepositoryBronzeToSilver.__build_schema_projection`, que era privado com
        name mangling. Virou funcao compartilhada porque o RepositorySilverSuperTenant precisa
        da MESMA logica — e chamar o metodo da outra classe exigiria escrever
        `_RepositoryBronzeToSilver__build_schema_projection`, um acoplamento que quebra em
        silencio se a classe for renomeada.

        Args:
            present_columns: colunas existentes no DataFrame de origem.
            target_schema: schema que o DataFrame precisa ter na saida.

        Returns:
            list[Column]: projecao pronta para `df.select(...)`.
        """
        projection = []

        for field in target_schema:
            column_name = field.name
            target_type = field.dataType

            if column_name in present_columns:
                projection.append(F.col(column_name).cast(target_type).alias(column_name))
            else:
                projection.append(F.lit(None).cast(target_type).alias(column_name))

        return projection

    @staticmethod
    def unificar_schemas(schemas_por_origem: List[Tuple[str, StructType]]) -> StructType:
        """Uniao dos schemas de N origens, preservando a ordem da primeira aparicao.

        Clientes diferentes rodam versoes diferentes do sistema embarcado: a mesma tabela tem
        colunas a mais num cliente e a menos no outro. Quem tem a mais NAO pode ser truncado (o
        dado sumiria); quem tem a menos recebe NULL tipado em `build_schema_projection`.

        Tipo DIVERGENTE para a mesma coluna FALHA, nomeando as duas origens. Castar em silencio
        (bigint -> string, por exemplo) nao quebra a escrita: quebra os JOINs do gold, que casam
        por igualdade, e o estrago aparece semanas depois como linha faltando num relatorio.

        NullType perde para qualquer tipo concreto: uma coluna 100% nula numa origem nao carrega
        informacao de tipo e nao pode ditar o schema do destino.

        Raises:
            ValueError: nomeando coluna, tipos e as duas origens em conflito.
        """
        campos: Dict[str, StructField] = {}
        origem_do_campo: Dict[str, str] = {}

        for origem, schema in schemas_por_origem:
            for field in schema.fields:
                anterior = campos.get(field.name)

                if anterior is None or isinstance(anterior.dataType, NullType):
                    campos[field.name] = field
                    origem_do_campo[field.name] = origem
                    continue

                if isinstance(field.dataType, NullType) or field.dataType == anterior.dataType:
                    continue

                raise ValueError(
                    f"Coluna '{field.name}' com tipos divergentes entre origens: "
                    f"{origem_do_campo[field.name]}={anterior.dataType.simpleString()} vs "
                    f"{origem}={field.dataType.simpleString()}. "
                    f"Alinhe a origem antes de consolidar."
                )

        return StructType(list(campos.values()))

    @staticmethod
    def colunas_faltantes(target_schema: StructType, schema_atual: StructType) -> List[StructField]:
        """Campos presentes em `target_schema` e ausentes em `schema_atual`, na ordem do alvo."""
        nomes = {f.name for f in schema_atual.fields}
        return [f for f in target_schema.fields if f.name not in nomes]

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