import logging
from urllib.parse import urlparse

from pyspark.sql import SparkSession
class SparkSessionFactory:
    """
    Esta fábrica centraliza a criação de objetos SparkSession, 
    garantindo uma configuração consistente em toda a aplicação e
    garante que a SparkSession esteja configurada corretamente para Spark, MinIO.
    """
    @staticmethod
    def create_spark_session(app_name: str, config: dict) -> SparkSession:
        """
        Cria uma SparkSession com as configurações fornecidas.

        Args:
            app_name (str): Nome da aplicação.
            config (dict): Configurações para a SparkSession.

        Returns:
            SparkSession: Instância da SparkSession configurada.
        """
        spark = None
        logger = logging.getLogger(__name__)
        if spark is None:
            logger.info(f"Criando SparkSession para a aplicação: {app_name}")
            builder = (
                SparkSession.builder 
                .appName(app_name)
            ) 

            # Configuração S3 (MinIO) customizada.
            # Credenciais NAO entram aqui: vem de AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY pelo
            # EnvironmentVariableCredentialsProvider. Setar access.key/secret.key explicitamente
            # sobrescreve o provider declarado no spark-defaults.conf da plataforma.
            s3_config = config.get("spark.s3", {})
            if s3_config:
                if s3_config.get("endpoint"):
                    logger.info("Configuração Spark para S3/MinIO customizada endpoint.")
                    builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_config.get("endpoint"))

            # O catálogo do ClickHouse só existe quando o destino está configurado. Sem ele o
            # `writeTo` do braço datamart_ch não resolve o nome da tabela.
            datamart_ch = config.get("datamart_clickhouse", None)
            if datamart_ch and datamart_ch.get("url"):
                builder = SparkSessionFactory._configurar_catalogo_clickhouse(builder, datamart_ch)

            # Configuração Spark customizada
            for key, value in config.get("spark.conf", {}).items():
                logger.info(f"Configuração Spark customizada: {key}={value}")
                builder = builder.config(key, value)

            spark = builder.getOrCreate()
            spark.sparkContext.setLogLevel("WARN") 
            spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
            logger.info("SparkSession criada com sucesso.")
        return spark

    @staticmethod
    def _configurar_catalogo_clickhouse(builder, config_ch):
        """Registra o ClickHouseCatalog a partir de `datamart_clickhouse`."""
        catalogo = config_ch.get("catalog") or "clickhouse"
        url = urlparse(config_ch["url"])
        prefixo = f"spark.sql.catalog.{catalogo}"

        builder = (
            builder
            .config(prefixo, "com.clickhouse.spark.ClickHouseCatalog")
            .config(f"{prefixo}.host", url.hostname)
            .config(f"{prefixo}.protocol", url.scheme or "http")
            .config(f"{prefixo}.http_port", str(url.port or 8123))
            .config(f"{prefixo}.user", config_ch["user"])
            .config(f"{prefixo}.password", config_ch["password"])
            .config(f"{prefixo}.database", config_ch["database"])
        )
        # Incidente #11: o framing LZ4 do cliente diverge do servidor e o catálogo falha no
        # initialize, ANTES de gravar linha — e a SparkApplication ainda termina COMPLETED.
        return builder.config("spark.clickhouse.write.compression.codec", "none")

    def stop_spark_session(self):
        """
        Encerra a SparkSession ativa.
        """
        if self.spark:
            logger.info("Encerrando SparkSession.")
            self.spark.stop()
            self.spark = None
            logger.info("SparkSession encerrada.")

