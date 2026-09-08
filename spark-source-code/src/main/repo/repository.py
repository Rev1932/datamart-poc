from repo.base import Repository
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from typing import Dict
import ast
import random
import time
from delta.exceptions import ConcurrentAppendException
from functools import reduce
from utils.delta_maintenance import DeltaMaintenance
from utils.minio_admin import MinioAdmin
from typing import List
from pyspark.sql import DataFrame, Column, functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable
from utils.queryutils import QueryUtils
from utils.handler_logger import initialize_logger


class RepositoryGoldDataVaults(Repository):
    def __init__(self, spark, config_application, params_config, queries_dir: str):
        self.logger = initialize_logger()
        self.spark = spark
        table_name = params_config.get("table_name")
        self.query = QueryUtils.build_query(queries_dir, config_application, table_name)
        self.output_path = (f"{config_application.get('minio.base_path')}/"f"{config_application.get('trino.schema_folder_gold')}/"f"{table_name}")
        self.config_application = config_application

        self.chave_pk = f"{params_config.get('chave_pk')}"
        self.colunas_zorder = ast.literal_eval(f"{params_config.get('colunas_zorder')}")
        self.colunas_zorder = ast.literal_eval(f"{params_config.get('colunas_zorder')}")

        try:
            self.chave_pk = ast.literal_eval(f"{params_config.get('chave_pk')}")
            if not isinstance(self.chave_pk, list):
                raise TypeError("A chave_pk não foi parseada como uma lista.")
        except (ValueError, SyntaxError, TypeError):
            self.chave_pk = []

    def read(self, **kwargs):
        """
        Lê dados da origem

        Args:
            **kwargs: Parâmetros adicionais para leitura

        Returns:
            DataFrame: Dados lidos da origem
        """
        #self.logger.debug(f"FINAL {self.query}")
        microBatchDF = self.spark.sql(self.query)

        #concatenação das chaves, compondo o hash
        key_columns = [F.col(c).cast("string") for c in self.chave_pk]
        #criando Hash e Data da carga
        microBatchDF = microBatchDF \
            .withColumn(
                "hk_business_id", 
                F.sha2(F.concat_ws("_", *key_columns), 256)
            ).withColumn("load_dts", F.current_timestamp())
        return microBatchDF
                        
    def write(self, data, **kwargs):
        """
        Escreve dados para o destino

        Args:
            data: DataFrame para escrever
            **kwargs: Parâmetros adicionais para escrita
        """
        if (DeltaTable.isDeltaTable(self.spark, self.output_path)) :
            delta_table = DeltaTable.forPath(self.spark, self.output_path)
            (delta_table.alias("tgt").merge(
                source=data.alias("src"),
                condition=f"tgt.hk_business_id = src.hk_business_id"
            )
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute())
        else:
            data.write \
            .format("delta") \
            .mode("overwrite") \
            .save(self.output_path)

            # Primeira carga: depois do overwrite precisa criar o DeltaTable, senão o optimize quebra.
            delta_table = DeltaTable.forPath(self.spark, self.output_path)
        
        if self.colunas_zorder:
            delta_table.optimize().executeZOrderBy(*self.colunas_zorder)
        else:
            delta_table.optimize().executeCompaction()


class RepositoryBronzeToSilver(Repository):
    """
    Repositório responsável pela ingestão de arquivos Parquet de grande volume para a Landing Zone.
    
    Esta classe gerencia a leitura de origens MinIO, garante a conformidade do esquema (Schema Alignment)
    e realiza a escrita em formato Delta, suportando Change Data Feed (CDF).
    """

    def __init__(self, spark, config_application, config_params):
        """
        Inicializa o repositório com configurações de caminho e utilitários.

        Args:
            spark (SparkSession): Sessão ativa do Spark.
            config_application (dict): Configurações globais da aplicação.
            config_params (dict): Parâmetros específicos da tabela e execução.
        """
        self.logger = initialize_logger()
        super().__init__(spark)
        self.base_path = config_application.get("minio.base_path")
        unit = f"data-bee_{config_params['config_name']}"
        table_name = config_params["table_name"]
        
        self.input_path = f"{self.base_path}/data-bee_replication/{unit}/{table_name}"
        self.output_path = f"{self.base_path}/business_datavault_data-bee/{table_name}"
        self.minio_utils = MinioAdmin(config_application.get("spark.s3"))
        
        self.logger.debug(f"Repositório inicializado para a tabela: {table_name}")
        self.logger.debug(f"Input Path: {self.input_path}")
        self.logger.debug(f"Output Path: {self.output_path}")

    def __build_schema_projection(self, present_columns: List[str], target_schema: StructType) -> List[Column]:
        """
        Cria uma lista de expressões Column para alinhar o DataFrame de origem ao esquema Delta.

        Args:
            present_columns (list[str]): Colunas existentes no arquivo físico.
            target_schema (StructType): Esquema esperado da tabela Delta de destino.

        Returns:
            list[Column]: Lista de colunas com casts e tratamentos de nulos.
        """
        self.logger.debug(f"Iniciando mapeamento de esquema para {len(target_schema)} campos alvo.")
        projection = []
        
        for field in target_schema:
            column_name = field.name
            target_type = field.dataType

            if column_name in present_columns:
                projection.append(F.col(column_name).cast(target_type).alias(column_name))
            else:
                self.logger.debug(f"Coluna '{column_name}' ausente na origem. Injetando NULL (Type: {target_type}).")
                projection.append(F.lit(None).cast(target_type).alias(column_name))
                
        return projection

    def __load_and_align_data(self, source_path: str, delta_schema: StructType) -> DataFrame:
        """
        Lê o Parquet e aplica o alinhamento de esquema para evitar quebras no merge/append.

        Args:
            source_path (str): Caminho do arquivo parquet específico.
            delta_schema (StructType): Esquema da tabela Delta existente.

        Returns:
            DataFrame: DataFrame normalizado.
        """
        try:
            self.logger.debug(f"Lendo e alinhando arquivo: {source_path}")
            raw_df = self.spark.read.parquet(source_path)
            
            projection = self.__build_schema_projection(raw_df.columns, delta_schema)
            return raw_df.select(projection)

        except Exception as e:
            self.logger.error(f"Falha ao alinhar dados de {source_path}: {str(e)}")
            raise

    def read(self, source_path: str = None) -> DataFrame:
        path = source_path if source_path else self.input_path
        self.logger.debug(f"Executando leitura bruta em: {path}")

        try:
            if DeltaTable.isDeltaTable(self.spark, self.output_path):
                self.logger.debug(f"Destino Delta detectado. Carregando esquema para alinhamento.")
                delta_schema = self.spark.read.format("delta").load(self.output_path).schema
                return self.__load_and_align_data(path, delta_schema)
            else:
                self.logger.debug(f"Tabela Delta não existe em {self.output_path}. Leitura sem alinhamento.")
                return self.spark.read.parquet(path)
        except Exception:
            self.logger.debug(f"Nenhum arquivo encontrado em: {path}")
            return None

    def read_file(self, file_name: str) -> DataFrame:
        """
        Lê um arquivo específico aplicando Schema Enforcement se a tabela destino já existir.

        Args:
            file_name (str): Nome do arquivo ou path relativo dentro do base_path.

        Returns:
            DataFrame: Dados carregados e validados.
        """
        source_path = f"{self.base_path}/{file_name}"
        self.logger.debug(f"Iniciando leitura processada: {source_path}")
        
        try:
            if DeltaTable.isDeltaTable(self.spark, self.output_path):
                self.logger.debug(f"Destino Delta detectado. Carregando esquema para alinhamento.")
                delta_schema = self.spark.read.format("delta").load(self.output_path).schema
                return self.__load_and_align_data(source_path, delta_schema)
            else:
                self.logger.debug(f"Tabela Delta não existe em {self.output_path}. Leitura sem alinhamento.")
                return self.spark.read.parquet(source_path)
                
        except Exception as e:
            self.logger.error(f"Erro crítico ao ler arquivo {file_name}: {str(e)}")
            raise
        
    def write(self, data):
        
        retry_count = 0
        base_delay = 2  # segundos
        max_retries = 5
        
        data = (data
            .withColumn("row_number", F.row_number().over(Window.partitionBy("hk_business_id").orderBy(F.col("load_dts").desc())))
            .where("row_number == 1")
            .drop("row_number")
        )
        if (DeltaTable.isDeltaTable(self.spark, self.output_path)) :
            while retry_count < max_retries:
                try:
                    delta_table = DeltaTable.forPath(self.spark, self.output_path)
                    delta_table.alias("tgt").merge(
                        source=data.alias("src"),
                        condition="tgt.hk_business_id = src.hk_business_id"
                    ).whenNotMatchedInsertAll().execute()
                    
                    return True
                    
                except ConcurrentAppendException as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        self.logger.error(f"Falha após {max_retries} tentativas: {str(e)}")
                        raise
                    
                    delay = base_delay * (2 ** retry_count) + random.uniform(0, 1)
                    self.logger.warning(
                        f"ConcurrentAppendException detectado. Tentativa {retry_count}/{max_retries}. "
                        f"Aguardando {delay:.2f}s antes de retry..."
                    )
                    time.sleep(delay)
        else:
            data = data.distinct()
            (data.write
                .format("delta")
                .option("delta.enableChangeDataFeed", "true")
                .partitionBy("source")
                .mode("overwrite")
                .save(self.output_path)    
            )

    def get_files_grouped_by_schema(self) -> Dict[str, List[str]]:
        """
        Lista todos os arquivos Parquet e os agrupa por schema físico.

        Lê apenas os metadados (footer) de cada arquivo para extrair o schema,
        sem carregar os dados em memória. Arquivos com schema idêntico são agrupados
        para serem processados juntos, reduzindo o número de transformações e I/O.

        Returns:
            Dict[str, List[str]]: Mapeamento de schema (JSON) para lista de object_names.
        """
        files = self.get_origin_list_file_names()

        if not files:
            self.logger.info("Nenhum arquivo encontrado para agrupamento por schema.")
            return {}

        self.logger.info(f"Lendo schemas de {len(files)} arquivo(s)...")
        schema_groups: Dict[str, List[str]] = {}

        for file_name in files:
            source_path = f"{self.base_path}/{file_name}"
            try:
                schema = self.spark.read.parquet(source_path).schema
                schema_key = schema.json()
                if schema_key not in schema_groups:
                    schema_groups[schema_key] = []
                schema_groups[schema_key].append(file_name)
            except Exception as e:
                self.logger.warning(f"Não foi possível ler schema de {file_name}: {str(e)}")

        self.logger.info(f"Schemas distintos detectados: {len(schema_groups)}")
        return schema_groups

    def read_files_group(self, file_names: List[str]) -> DataFrame:
        """
        Lê um grupo de arquivos Parquet com schema idêntico, aplicando alinhamento se necessário.

        Como todos os arquivos do grupo compartilham o mesmo schema físico, a leitura
        é feita em uma única operação, combinando os dados sem conflitos de schema.

        Args:
            file_names (List[str]): Lista de object_names dos arquivos a serem lidos.

        Returns:
            DataFrame: Dados combinados e normalizados.
        """
        source_paths = [f"{self.base_path}/{f}" for f in file_names]
        self.logger.debug(f"Lendo grupo de {len(source_paths)} arquivo(s)...")

        try:
            if DeltaTable.isDeltaTable(self.spark, self.output_path):
                self.logger.debug("Destino Delta detectado. Aplicando alinhamento de schema.")
                delta_schema = self.spark.read.format("delta").load(self.output_path).schema
                raw_df = self.spark.read.parquet(*source_paths)
                projection = self.__build_schema_projection(raw_df.columns, delta_schema)
                return raw_df.select(projection)
            else:
                self.logger.debug("Tabela Delta não existe. Leitura direta sem alinhamento.")
                return self.spark.read.parquet(*source_paths)
        except Exception as e:
            self.logger.error(f"Erro ao ler grupo de arquivos: {str(e)}")
            raise

    def get_origin_list_file_names(self) -> List[str]:
        """
        Lista todos os arquivos Parquet disponíveis no diretório de entrada.

        Returns:
            List[str]: Lista de nomes de objetos (object_name).
        """
        self.logger.debug(f"Listando arquivos em: {self.input_path}")
        try:
            files = [
                obj.object_name 
                for obj in self.minio_utils.list_path_files(self.input_path) 
                if obj.object_name.endswith('.parquet')
            ]
            self.logger.debug(f"{len(files)} arquivos encontrados para processamento.")
            return files
        except Exception as e:
            self.logger.error(f"Erro ao listar arquivos no MinIO: {str(e)}")
            return []

    def cleaning_processed(self, data: DataFrame):
        """
        Remove os arquivos já processados da origem (MinIO).

        Args:
            data (DataFrame): DataFrame contendo controle de processamento.
        """
        self.logger.debug(f"Iniciando limpeza de arquivos processados.")
        try:
            processed_tables = data \
                .filter(F.col("processed_file") == "Y") \
                .select("source_file") \
                .distinct() \
                .rdd.flatMap(lambda x: x) \
                .collect()

            if processed_tables:
                self.logger.debug(f"Solicitando remoção de {len(processed_tables)} arquivos.")
                self.minio_utils.cleaning_processed(processed_tables)
                self.logger.debug(f"Limpeza concluída.")
            else:
                self.logger.debug(f"Nenhum arquivo marcado para limpeza.")
        except Exception as e:
            self.logger.error(f"Falha durante a limpeza de arquivos: {str(e)}")