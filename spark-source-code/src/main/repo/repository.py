from repo.base import Repository
from repo.helper import Helper
from delta.tables import DeltaTable
from pyspark.sql import Window
from typing import Dict
from concurrent.futures import ThreadPoolExecutor
import ast
import random
import time
from delta.exceptions import ConcurrentAppendException
from utils.minio_admin import MinioAdmin
from typing import List
from pyspark.sql import DataFrame, Column, functions as F
from pyspark.sql.types import StringType, StructField, StructType
from delta.tables import DeltaTable
from utils.postgres_utils import ConnectionPostgres, GeneratePostgresQueryUtils
from utils.handler_logger import initialize_logger


class RepositoryGoldDataVaults(Repository):
    def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir: str):
        self.logger = initialize_logger()
        self.spark = spark
        table_name = runtime_parameters.get("table_name")
        from utils.queryutils import QueryUtils
        self.query = QueryUtils.build_query(queries_dir, environment_parameters, table_name, runtime_parameters)
        self.output_path = (f"{environment_parameters.get('minio.base_path')}/"f"{environment_parameters.get('trino.schema_folder_gold')}/"f"{table_name}")
        self.environment_parameters = environment_parameters

        self.primary_key = f"{runtime_parameters.get('primary_key')}"
        self.colunas_zorder = ast.literal_eval(f"{runtime_parameters.get('colunas_zorder')}")

        try:
            self.primary_key = ast.literal_eval(f"{runtime_parameters.get('primary_key')}")
            if not isinstance(self.primary_key, list):
                raise TypeError("A primary_key não foi parseada como uma lista.")
        except (ValueError, SyntaxError, TypeError):
            self.primary_key = []

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
        key_columns = [F.col(c).cast("string") for c in self.primary_key]
        #criando Hash e Data da carga
        microBatchDF = microBatchDF \
            .withColumn(
                "hk_business_id", 
                F.sha2(F.concat_ws("_", *key_columns), 256)
            ).withColumn("load_dts", F.current_timestamp())
        return microBatchDF
                        
    def transform(self, data, **kwargs):
        transformed_data = data.dropDuplicates(["hk_business_id"])
        return transformed_data            

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


class RepositoryDatamart(Repository):
    """Leitura e transformação comuns aos destinos do datamart.

    Cada destino herda daqui e sobrescreve apenas `write`: dois destinos que leiam ou
    transformem diferente não são comparáveis entre si.
    """
    UNIQUE_KEY = "hk_business_id"

    def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir: str):
        self.logger = initialize_logger()
        super().__init__(spark)
        self.table_name = runtime_parameters.get("table_name")
        from utils.queryutils import QueryUtils
        self.query = QueryUtils.build_query(queries_dir, environment_parameters, self.table_name, runtime_parameters)
        self.primary_key = runtime_parameters.get("primary_key") or []
        self.colunas_obrigatorias = runtime_parameters.get("colunas_obrigatorias") or []

    def read(self, **kwargs):
        """
        Lê dados da origem

        Args:
            **kwargs: Parâmetros adicionais para leitura

        Returns:
            DataFrame: Dados lidos da origem
        """
        microBatchDF = self.spark.sql(self.query)

        #concatenação das chaves, compondo o hash
        key_columns = [F.col(c).cast("string") for c in self.primary_key]
        #criando Hash e Data da carga
        microBatchDF = microBatchDF \
            .withColumn(
                self.UNIQUE_KEY,
                F.sha2(F.concat_ws("_", *key_columns), 256)
            ).withColumn("load_dts", F.current_timestamp())
        return microBatchDF

    def transform(self, data, **kwargs):
        transformed_data = data.dropDuplicates([self.UNIQUE_KEY])
        if not self.colunas_obrigatorias:
            return transformed_data

        # Se só um destino limpar, as contagens divergem e a diferença é atribuída ao motor.
        antes = transformed_data.count()
        limpo = transformed_data.na.drop(subset=self.colunas_obrigatorias)
        descartadas = antes - limpo.count()
        if descartadas:
            self.logger.warning(
                f"{descartadas} linha(s) descartada(s) por nulo em {self.colunas_obrigatorias}")
        return limpo


class RepositoryGoldDatamart(RepositoryDatamart):
    """
    Repositório da camada gold com destino PostgreSQL (datamart).

    Reproduz o padrão de merge do Delta por hk_business_id: o Spark carrega
    uma tabela staging via JDBC e o banco executa o upsert set-based
    (INSERT ... ON CONFLICT DO UPDATE), garantindo atomicidade no destino.
    """

    def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir: str):
        super().__init__(spark, environment_parameters, runtime_parameters, queries_dir)

        self.config_postgres = environment_parameters.get("postgres")
        self.pg_schema = self.config_postgres.get("schema", "public")
        self.jdbc_url = (
            f"jdbc:postgresql://{self.config_postgres['host']}:{self.config_postgres.get('port', 5432)}"
            f"/{self.config_postgres['database']}?reWriteBatchedInserts=true"
        )

    def write(self, data, **kwargs):
        """
        Escreve dados para o destino

        Args:
            data: DataFrame para escrever
            **kwargs: Parâmetros adicionais para escrita
        """
        staging_name = GeneratePostgresQueryUtils.get_staging_table_name(self.table_name)
        target = GeneratePostgresQueryUtils.get_qualified_name(self.pg_schema, self.table_name)
        staging = GeneratePostgresQueryUtils.get_qualified_name(self.pg_schema, staging_name)

        with ConnectionPostgres(self.config_postgres) as conn:
            conn.execution_query(GeneratePostgresQueryUtils.get_drop_table(staging))
            conn.execution_query(GeneratePostgresQueryUtils.generate_sql_create_table(data, staging, unlogged=True))
            conn.execution_query(GeneratePostgresQueryUtils.generate_sql_create_table(data, target, unique_key=self.UNIQUE_KEY))

        self.logger.info(f"Carregando staging {staging} via JDBC...")
        (data.write
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", f"{self.pg_schema}.{staging_name}")
            .option("user", self.config_postgres["user"])
            .option("password", self.config_postgres["password"])
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", 100000)
            .option("numPartitions", 8)
            .mode("append")
            .save()
        )

        self.logger.info(f"Executando merge de {staging} para {target}...")
        with ConnectionPostgres(self.config_postgres) as conn:
            conn.execution_query(GeneratePostgresQueryUtils.generate_sql_merge(data, target, staging, self.UNIQUE_KEY))
            conn.execution_query(GeneratePostgresQueryUtils.get_drop_table(staging))


class RepositoryDatamartClickhouse(RepositoryDatamart):
    """Repositorio da camada gold com destino ClickHouse.

    Grava por staging e troca de particao; `read` e `transform` vem da base.
    """

    def __init__(self, spark, environment_parameters, runtime_parameters, queries_dir: str):
        super().__init__(spark, environment_parameters, runtime_parameters, queries_dir)
        self.janela_inicio = runtime_parameters.get("janela_inicio")

        self.config_ch = environment_parameters.get("datamart_clickhouse")
        self.catalog = self.config_ch.get("catalog", "clickhouse")
        self.database = self.config_ch["database"]
        from utils.clickhouse_datamart import ClickHouseDatamartClient
        self.cliente = ClickHouseDatamartClient(self.config_ch)

    def _particoes(self, data) -> List[str]:
        linhas = data.select(F.date_format("timestamp", "yyyyMM").alias("p")).distinct().collect()
        return sorted(linha["p"] for linha in linhas)

    def _guarda_cobertura(self, data, particoes) -> None:
        """Recusa a troca quando o DataFrame nao cobre o mes inteiro que vai substituir.

        Sem isto, REPLACE PARTITION de um mes com dado parcial APAGA o restante do mes,
        sem erro: a contagem da particao simplesmente cai.
        """
        if not self.janela_inicio:
            raise ValueError("janela_inicio ausente: a troca de particao exige janela explicita.")

        inicio = str(self.janela_inicio)
        if inicio[8:10] != "01" or inicio[11:] not in ("", "00:00:00", "00:00:00.000"):
            raise ValueError(
                f"janela_inicio '{inicio}' nao esta na fronteira do mes; "
                "REPLACE PARTITION trocaria um mes parcial."
            )

        minimo = data.agg(F.min("timestamp").alias("m")).collect()[0]["m"]
        if minimo is not None and str(minimo) < inicio:
            raise ValueError(f"dado anterior a janela: min(timestamp)={minimo} < {inicio}")

        mes_inicial = inicio[0:4] + inicio[5:7]
        if particoes and min(particoes) < mes_inicial:
            raise ValueError(f"particao {min(particoes)} anterior ao inicio da janela {mes_inicial}")

    def write(self, data, **kwargs):
        particoes = self._particoes(data)
        if not particoes:
            self.logger.warning("Nenhuma particao a trocar: DataFrame vazio.")
            return

        self._guarda_cobertura(data, particoes)

        staging = self.cliente.nome_staging(self.table_name)
        self.cliente.criar_staging(self.table_name, staging)
        try:
            esperado = data.count()
            self.logger.info(f"Carregando staging {staging} com {esperado} linha(s)...")
            (data
                .repartition(F.date_format("timestamp", "yyyyMM"))
                .writeTo(f"{self.catalog}.{self.database}.{staging}")
                .append())

            gravado = self.cliente.contar(staging)
            if gravado != esperado:
                raise RuntimeError(
                    f"staging {staging} com {gravado} linha(s), esperado {esperado}: troca abortada."
                )

            self.cliente.trocar_particoes(self.table_name, staging, particoes)
        finally:
            self.cliente.descartar(staging)


class RepositoryBronzeToSilver(Repository):
    """
    Repositório responsável pela ingestão de arquivos Parquet de grande volume para a Landing Zone.
    
    Esta classe gerencia a leitura de origens MinIO, garante a conformidade do esquema (Schema Alignment)
    e realiza a escrita em formato Delta, suportando Change Data Feed (CDF).
    """

    # Threads para a leitura de footers em get_files_grouped_by_schema. Baixo de proposito:
    # varias tabelas podem estar em execucao no mesmo pod e o total se multiplica.
    SCHEMA_READ_WORKERS = 4

    def __init__(self, spark, environment_parameters, runtime_parameters):
        """
        Inicializa o repositório com configurações de caminho e utilitários.

        Args:
            spark (SparkSession): Sessão ativa do Spark.
            environment_parameters (dict): Configurações globais da aplicação.
            runtime_parameters (dict): Parâmetros específicos da tabela e execução.
        """
        self.logger = initialize_logger()
        super().__init__(spark)
        self.base_path = environment_parameters.get("minio.base_path")
        unit = f"data-bee_{runtime_parameters['filial_name']}"
        table_name = runtime_parameters["table_name"]

        # Mesma derivacao usada por RawDataVaultInitTransformerAuto para preencher a coluna
        # `source`, que e a chave de particao do destino. Precisa bater EXATAMENTE, senao o
        # predicado de particao do merge (write) nao casa com nenhuma linha.
        self.source = unit

        self.input_path = f"{self.base_path}/data-bee_replication/{unit}/{table_name}"
        self.output_path = f"{self.base_path}/business_datavault_data-bee/{table_name}"
        # `shared` e nao o construtor: este __init__ roda uma vez por TABELA.
        self.minio_utils = MinioAdmin.shared(environment_parameters.get("spark.s3"))

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
        # Implementacao movida para Helper.build_schema_projection (repo/helper.py) para ser
        # reusada pelo RepositorySilverSuperTenant. O metodo privado permanece como fachada
        # porque e chamado em dois pontos desta classe (read_file e read_files_group).
        return Helper.build_schema_projection(present_columns, target_schema)

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

    def read(self, **kwargs) -> DataFrame:
        """
        Le os parquet pendentes da origem. Devolve None quando nao ha nenhum.

        A ORDEM DAS OPERACOES E O PONTO DESTE METODO. A verificacao de existencia vem PRIMEIRO,
        por listagem de metadados no MinIO — barata e conclusiva.

        Antes, essa pergunta era respondida pelo EFEITO COLATERAL da operacao cara: o metodo
        checava a tabela Delta de destino, carregava o schema dela e so entao tocava na origem,
        onde `spark.read.parquet()` de um diretorio vazio estoura
        `AnalysisException: Unable to infer schema for Parquet`. Um `except Exception`
        abrangente capturava isso e devolvia None.

        O efeito era confundir "diretorio vazio" (normal, esperado) com "falha real de leitura"
        (S3 fora, credencial invalida, parquet corrompido, schema do destino ilegivel): TODA
        falha virava None, e None e lido por `process_data()` como "nada a processar" —
        ou seja, a tabela era reportada como SUCESSO sem ter feito nada. Ver
        docs/validacao-kubernetes.md secao 7.

        Agora o unico caminho que devolve None e a origem genuinamente vazia; qualquer outra
        falha sobe e e contabilizada como falha da tabela.
        """
        arquivos = self.get_origin_list_file_names()
        if not arquivos:
            # INFO, nao DEBUG: e informacao operacional legitima. Em DEBUG (o logger roda em
            # INFO) essa mensagem simplesmente nao existia em producao.
            self.logger.info(f"Nenhum arquivo .parquet em {self.input_path}. Nada a processar.")
            return None

        self.logger.info(f"{len(arquivos)} arquivo(s) a processar.")
        # Lista EXPLICITA, nao o diretorio: a verificacao acima e a leitura passam a usar a
        # mesma fonte de verdade, e nao ha como o Spark descer recursivamente em `processed/`
        # e reler os `.bkp` ja arquivados.
        return self.read_files_group(arquivos)

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
        """
        Grava na tabela Delta de destino, que e COMPARTILHADA entre as filiais.

        O destino (`business_datavault_data-bee/<tabela>`) nao e escopado por filial: as N
        filiais escrevem na mesma tabela, particionada por `source`. Duas consequencias
        governam o desenho deste metodo:

        1. PRIMEIRA CARGA e uma corrida. Varias filiais podem ver a tabela como inexistente
           ao mesmo tempo. Por isso a criacao usa `errorifexists` e, se perder a corrida,
           cai no caminho de merge — com `overwrite` a ultima filial apagava as anteriores.
        2. MERGE precisa de PREDICADO DE PARTICAO. Sem `tgt.source = ...` o controle otimista
           de concorrencia do Delta considera a tabela inteira em conflito e as filiais se
           atropelam com ConcurrentAppendException. Com o predicado, cada filial toca so a
           sua particao e o conflito desaparece na origem.
        """
        data = (data
            .withColumn("row_number", F.row_number().over(Window.partitionBy("hk_business_id").orderBy(F.col("load_dts").desc())))
            .where("row_number == 1")
            .drop("row_number")
        )

        if not DeltaTable.isDeltaTable(self.spark, self.output_path):
            try:
                (data.distinct().write
                    .format("delta")
                    .option("delta.enableChangeDataFeed", "true")
                    .partitionBy("source")
                    # `errorifexists`, nao `overwrite`: se outra filial criou a tabela entre o
                    # isDeltaTable acima e este save, overwrite descartaria os dados dela.
                    .mode("errorifexists")
                    .save(self.output_path)
                )
                return True
            except Exception as e:
                if not DeltaTable.isDeltaTable(self.spark, self.output_path):
                    # A tabela continua inexistente: a falha nao foi a corrida, e sim um erro
                    # real de escrita (permissao, schema, S3). Relanca.
                    raise
                self.logger.info(
                    f"Tabela criada concorrentemente por outra filial durante a primeira "
                    f"carga de {self.output_path}. Seguindo por merge. Detalhe: {e}"
                )

        # Retry mantido como rede de seguranca: o predicado de particao elimina o conflito
        # entre filiais, mas nao cobre um vacuum/optimize concorrente sobre a mesma particao.
        retry_count = 0
        base_delay = 2  # segundos
        max_retries = 5

        while retry_count < max_retries:
            try:
                delta_table = DeltaTable.forPath(self.spark, self.output_path)
                delta_table.alias("tgt").merge(
                    source=data.alias("src"),
                    condition=(
                        "tgt.hk_business_id = src.hk_business_id "
                        f"AND tgt.source = '{self.source}'"
                    )
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

        def _ler_schema(file_name: str):
            source_path = f"{self.base_path}/{file_name}"
            try:
                return file_name, self.spark.read.parquet(source_path).schema.json()
            except Exception as e:
                self.logger.warning(f"Não foi possível ler schema de {file_name}: {str(e)}")
                return file_name, None

        # Cada leitura e uma ida ao S3 para buscar o footer do parquet: I/O puro no driver, sem
        # CPU. Em serie, com centenas de arquivos, isto dominava o tempo do fallback.
        #
        # Paralelismo conservador de proposito: este metodo pode estar rodando dentro de uma
        # thread de tabela, entao o total real e SCHEMA_READ_WORKERS x max_workers do pod.
        workers = min(self.SCHEMA_READ_WORKERS, len(files))
        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix="schema-read") as pool:
            # `map` preserva a ordem de entrada — o agrupamento precisa ser deterministico
            # entre execucoes para que a ordem de processamento dos grupos nao varie.
            resultados = list(pool.map(_ler_schema, files))

        schema_groups: Dict[str, List[str]] = {}
        for file_name, schema_key in resultados:
            if schema_key is None:
                continue
            schema_groups.setdefault(schema_key, []).append(file_name)

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

        Lista NAO-recursiva (`MinioAdmin.list_path_files` usa recursive=False), o que exclui
        de proposito a subpasta `processed/`, onde ficam os arquivos ja consumidos.

        Returns:
            List[str]: Lista de nomes de objetos (object_name). Lista vazia significa,
            inequivocamente, que nao ha o que processar.

        Raises:
            Exception: falha ao falar com o MinIO e RELANCADA.
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
            # Relanca (antes: `return []`). Devolver lista vazia tornava "MinIO fora do ar"
            # indistinguivel de "pasta vazia" para os tres chamadores — e todos tratam lista
            # vazia como "nada a fazer, sucesso". Uma falha de infra deixava a tabela verde
            # com o dado nao processado parado na origem.
            self.logger.error(f"Erro ao listar arquivos no MinIO: {str(e)}")
            raise

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


class RepositorySilverSuperTenant(Repository):
    """
    Consolida as camadas Silver JA PROCESSADAS de N tenants numa Silver de Super Tenant.

    Nao existe camada bronze aqui: a origem e o DESTINO do `bronze_silver` de cada cliente, num
    bucket proprio. O dado ja passou por transformacao, quarentena e Data Vault no tenant dele —
    o trabalho deste repositorio e de REIDENTIFICACAO, nao de transformacao: cada tenant de
    origem vira uma FILIAL do super tenant.

    A unidade de trabalho e o par (tabela, tenant de origem), e NAO um union das N origens.
    Deliberado, por tres razoes:
      1. o predicado de particao do merge (`tgt.source = 'data-bee_<tenant>'`) so e valido para
         UM tenant — e e ele que faz o Delta tocar apenas a particao daquele cliente;
      2. uma origem que ainda nao tem a tabela e pulada sem afetar as demais;
      3. o alinhamento de schema de cada origem e independente.

    NAO instancia MinioAdmin, e isso e uma GUARDA, nao um esquecimento: este pipeline nao lista
    nem move arquivo de origem. Os parquets de `data-bee_replication` pertencem ao cliente e ja
    foram consumidos pelo `bronze_silver` dele — um `cleaning_processed` aqui apagaria dado de um
    bucket que nao e nosso.
    """

    # Colunas de metadado que o super tenant ACRESCENTA a Silver de origem. Preservam a
    # identidade que a reidentificacao sobrescreve; ver `reidentify`.
    COLUNAS_LINHAGEM = ("filial_origem", "tenant_origem")

    # Sem estas colunas a origem nao e uma Silver produzida pelo `bronze_silver`.
    COLUNAS_OBRIGATORIAS = ("hk_business_id", "source", "load_dts")

    def __init__(self, spark, environment_parameters, runtime_parameters):
        """
        Args:
            spark (SparkSession): Sessão ativa do Spark.
            environment_parameters (dict): Configurações globais (bucket de DESTINO).
            runtime_parameters (dict): Parâmetros da tabela, incluindo `source_tenants`.
        """
        self.logger = initialize_logger()
        super().__init__(spark)
        # Import tardio, espelhando o que o pipeline_orchestrator ja faz na direcao oposta
        # (`from repo.repository import ...` dentro dos __init__): com os dois lados tardios o
        # ciclo de import e impossivel.
        from core.pipeline_orchestrator import resolve_delta_path, SILVER_FOLDER_FALLBACK

        self.environment_parameters = environment_parameters
        self.table_name = runtime_parameters["table_name"]

        # A pasta da camada silver e convencao GLOBAL: todo `bronze_silver` escreve nela, em
        # qualquer bucket. Por isso a env do pod do super tenant resolve tambem o path das
        # origens.
        self.silver_folder = (
            environment_parameters.get("trino.schema_folder_silver") or SILVER_FOLDER_FALLBACK
        )
        # MESMA resolucao que o `delta_maintenance` usa. Se este repositorio hardcodasse a pasta
        # (como o RepositoryBronzeToSilver faz), OPTIMIZE/VACUUM poderiam apontar para um path
        # diferente do que foi escrito.
        self.output_path = resolve_delta_path(environment_parameters, self.table_name, "silver")

        # Copia local: a lista vem da config BASE, compartilhada entre as threads de tabela do
        # TableRunner (`dataclasses.replace` copia a REFERENCIA).
        self.source_tenants = list(runtime_parameters.get("source_tenants") or [])

        self.logger.debug(f"Super tenant inicializado para a tabela: {self.table_name}")
        self.logger.debug(f"Output Path: {self.output_path}")

    def caminho_origem(self, base_path: str) -> str:
        """Path da Silver de um tenant de origem."""
        return f"{base_path}/{self.silver_folder}/{self.table_name}"

    def origens_disponiveis(self) -> List[dict]:
        """
        Separa origem AUSENTE (pular) de origem INACESSIVEL (falhar).

        Nem todo cliente tem toda tabela — versoes diferentes do sistema embarcado —, entao
        ausencia e estado legitimo. Ja uma falha ao INSPECIONAR (403 do MinIO por policy sem
        leitura no bucket do cliente, S3 fora) nao pode virar "origem ausente": a tabela ficaria
        VERDE sem ter consolidado nada. E a mesma regressao que
        `tests/unit/test_repository_read.py` ancora para o `bronze_silver`.

        Returns:
            list[dict]: origens existentes, cada uma com a chave extra `path`.

        Raises:
            Exception: falha de inspecao e RELANCADA.
            ValueError: nenhuma das origens tem a tabela.
        """
        disponiveis: List[dict] = []
        ausentes: List[str] = []

        for origem in self.source_tenants:
            path = self.caminho_origem(origem["base_path"])
            try:
                existe = DeltaTable.isDeltaTable(self.spark, path)
            except Exception as e:
                self.logger.error(
                    f"Falha ao inspecionar a origem '{origem['tenant']}' em {path}: {str(e)}"
                )
                raise

            if existe:
                disponiveis.append({**origem, "path": path})
            else:
                ausentes.append(origem["tenant"])

        if ausentes:
            # WARNING e nao DEBUG: o operador precisa da lista para distinguir "versao antiga do
            # sistema embarcado" de "o bronze_silver daquele cliente nao rodou hoje".
            self.logger.warning(
                f"[{self.table_name}] sem tabela nas origens {ausentes} — puladas."
            )

        if not disponiveis:
            # Falha, nao no-op: "nenhum cliente tem esta tabela" e indistinguivel de "todos os
            # base_path estao errados" ou "a policy do S3 nao libera nenhum bucket". Sucesso
            # silencioso e o pior desfecho possivel.
            raise ValueError(
                f"Nenhuma das {len(self.source_tenants)} origens tem a tabela "
                f"'{self.table_name}'. Verifique --source_tenants e o nome da tabela."
            )

        return disponiveis

    def resolver_schema_alvo(self, origens: List[dict]) -> StructType:
        """
        Uniao dos schemas das origens (mais o do destino, se ja existir).

        O DESTINO entra PRIMEIRO na uniao por dois motivos: fixa a ordem das colunas ja
        existentes e impede que uma coluna trazida por um cliente REMOVIDO de `--source_tenants`
        desapareca do schema — o dado dele continua na tabela, porque o merge nunca deleta.

        Raises:
            ValueError: coluna obrigatoria ausente, ou tipo divergente entre origens.
        """
        schemas: List = []

        if DeltaTable.isDeltaTable(self.spark, self.output_path):
            schemas.append(
                ("<destino>", self.spark.read.format("delta").load(self.output_path).schema)
            )

        for origem in origens:
            schemas.append(
                (origem["tenant"], self.spark.read.format("delta").load(origem["path"]).schema)
            )

        alvo = Helper.unificar_schemas(schemas)
        nomes = set(alvo.fieldNames())

        for obrigatoria in self.COLUNAS_OBRIGATORIAS:
            if obrigatoria not in nomes:
                raise ValueError(
                    f"Coluna '{obrigatoria}' ausente nas origens de '{self.table_name}': a "
                    f"origem nao parece ser uma Silver produzida pelo bronze_silver."
                )

        # `unidade_origem` e ESCRITO pela reidentificacao. Sem o campo no schema alvo, o
        # DataFrame de origem teria uma coluna a mais que o destino e o whenNotMatchedInsertAll
        # rejeitaria a escrita. O mesmo vale para as colunas de linhagem.
        extras = [
            StructField(nome, StringType(), True)
            for nome in ("unidade_origem",) + self.COLUNAS_LINHAGEM
            if nome not in nomes
        ]

        return StructType(alvo.fields + extras) if extras else alvo

    def evoluir_schema_destino(self, target_schema: StructType):
        """
        Acrescenta ao destino as colunas novas trazidas pelas origens.

        `ALTER TABLE ... ADD COLUMNS` explicito em vez de
        `spark.databricks.delta.schema.autoMerge.enabled`: aquela conf e da SparkSession, que no
        modo multi-tabela e COMPARTILHADA pelas N threads — liga-la aqui mudaria o comportamento
        de escrita das tabelas que as outras threads estao processando.
        """
        if not DeltaTable.isDeltaTable(self.spark, self.output_path):
            return  # primeira carga: o schema nasce do write

        atual = self.spark.read.format("delta").load(self.output_path).schema
        novas = Helper.colunas_faltantes(target_schema, atual)
        if not novas:
            return

        colunas = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in novas)
        self.logger.warning(
            f"[{self.table_name}] novas colunas vindas das origens: {[f.name for f in novas]}"
        )
        self.spark.sql(f"ALTER TABLE delta.`{self.output_path}` ADD COLUMNS ({colunas})")

    def read(self, tenant: str, path: str, target_schema: StructType, **kwargs) -> DataFrame:
        """
        Le a Silver de UM tenant de origem e a reidentifica como filial do super tenant.

        A assinatura diverge da ABC (`read(self, **kwargs)`) de proposito: a unidade de trabalho
        aqui e o par (tabela, tenant). Mesmo precedente de `RepositoryBronzeToSilver.write`, que
        tambem nao segue a assinatura da base.

        Args:
            tenant (str): nome do tenant de origem — vira o nome da filial no super tenant.
            path (str): path da Silver de origem.
            target_schema (StructType): schema unificado, resolvido antes de qualquer leitura.

        Returns:
            DataFrame: dados reidentificados, com EXATAMENTE o schema do destino.
        """
        df = self.spark.read.format("delta").load(path)

        # Projecao ANTES da reidentificacao: garante que o DataFrame tenha exatamente o schema do
        # destino, que e o que permite whenNotMatchedInsertAll/whenMatchedUpdateAll sem ligar o
        # autoMerge na sessao compartilhada.
        df = df.select(Helper.build_schema_projection(df.columns, target_schema))

        return (df
            # `concat_ws` IGNORA nulos: com hk_business_id nulo, TODAS as linhas nulas deste
            # cliente colapsariam no mesmo sha2(tenant) e o merge deixaria uma so. Na pratica a
            # Silver de origem nunca tem hk nulo, mas o custo do filtro e zero e o da colisao e
            # perda de dado.
            .filter(F.col("hk_business_id").isNotNull())
            # Linhagem ANTES do overwrite. Sem estas colunas, duas filiais de um mesmo cliente
            # ficam indistinguiveis e os JOINs do gold por `unidade_origem` passam a casar entre
            # elas. Hoje os clientes sao de filial unica; no dia em que nao forem, esta coluna e
            # a unica saida sem reprocessar tudo.
            .withColumn("filial_origem", F.col("unidade_origem"))
            .withColumn("tenant_origem", F.lit(tenant))
            # Re-hash sobre o hk ORIGINAL: preserva a granularidade de filial que a Silver de
            # origem ja carrega e garante unicidade entre clientes. O hk de origem tem 64 chars
            # fixos, entao nao ha ambiguidade de concatenacao entre um tenant 'a_b' e um 'a'.
            .withColumn(
                "hk_business_id",
                F.sha2(F.concat_ws("_", F.col("hk_business_id"), F.lit(tenant)), 256)
            )
            # `source` e a chave de PARTICAO do destino e precisa bater EXATAMENTE com o
            # predicado do merge em `write`.
            .withColumn("source", F.lit(f"data-bee_{tenant}"))
            # `unidade_origem` e o que os .sql do gold leem como `filial`. `dataset_origem` e
            # PRESERVADO: e o banco do cliente, e o par (unidade_origem, dataset_origem) e o que
            # os JOINs do gold usam.
            .withColumn("unidade_origem", F.lit(tenant))
        )

    def write(self, data: DataFrame, source: str = None, **kwargs):
        """
        MERGE idempotente na particao de UM tenant de origem.

        Args:
            data (DataFrame): dados ja reidentificados.
            source (str): valor da coluna de particao (`data-bee_<tenant>`).
        """
        if not source:
            raise ValueError("write() do super tenant exige `source` (a particao do tenant).")

        # Dedup OBRIGATORIO aqui, nao apenas desejavel: com `whenMatchedUpdateAll`, duas linhas
        # de origem com o mesmo hk_business_id abortam o MERGE inteiro
        # ("Cannot perform Merge as multiple source rows matched"). Custa um shuffle; e o preco
        # de nao derrubar o job por um duplicado residual na origem.
        data = (data
            .withColumn("row_number", F.row_number().over(
                Window.partitionBy("hk_business_id").orderBy(F.col("load_dts").desc())))
            .where("row_number == 1")
            .drop("row_number")
        )

        if not DeltaTable.isDeltaTable(self.spark, self.output_path):
            try:
                (data.write
                    .format("delta")
                    # CDF e particionamento IDENTICOS ao bronze_silver: e o que torna a Silver do
                    # super tenant indistinguivel das demais para o gold, o delta_maintenance e o
                    # Trino.
                    .option("delta.enableChangeDataFeed", "true")
                    .partitionBy("source")
                    # `errorifexists`, nao `overwrite`: se outra origem criou a tabela entre o
                    # isDeltaTable acima e este save, overwrite descartaria os dados dela.
                    .mode("errorifexists")
                    .save(self.output_path)
                )
                return True
            except Exception as e:
                if not DeltaTable.isDeltaTable(self.spark, self.output_path):
                    # A tabela continua inexistente: a falha nao foi a corrida, e sim um erro
                    # real de escrita. Relanca.
                    raise
                self.logger.info(
                    f"Tabela criada concorrentemente durante a primeira carga de "
                    f"{self.output_path}. Seguindo por merge. Detalhe: {e}"
                )

        retry_count = 0
        base_delay = 2  # segundos
        max_retries = 5

        while retry_count < max_retries:
            try:
                delta_table = DeltaTable.forPath(self.spark, self.output_path)
                (delta_table.alias("tgt").merge(
                    source=data.alias("src"),
                    condition=(
                        "tgt.hk_business_id = src.hk_business_id "
                        f"AND tgt.source = '{source}'"
                    )
                )
                .whenNotMatchedInsertAll()
                # Update-all (o bronze_silver e insert-only): a origem aqui e uma Silver que pode
                # ter sido CORRIGIDA depois. Sem o update, a correcao nunca chegaria ao super
                # tenant e ele divergiria do cliente para sempre.
                .whenMatchedUpdateAll()
                .execute())

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
