from utils.handler_logger import initialize_logger


class ClickHouseConnection:
    """
    Destino de delivery ClickHouse (análogo ao TrinoConnection, porém para escrita Spark).

    A conexão em si (host/porta/usuário/senha) é configurada no catálogo Spark
    `spark.sql.catalog.<catalog>.*` (aplicado pelo SparkSessionFactory a partir do
    bloco `spark.conf` do HOCON). Esta classe só resolve o nome totalmente
    qualificado da tabela e delega a escrita ao connector oficial ClickHouse-Spark
    via DataFrameWriterV2 (`writeTo(...).append()`).
    """

    def __init__(self, config_manager):
        self.logger = initialize_logger()
        self.catalog = config_manager.get("clickhouse.catalog", "clickhouse")
        self.database = config_manager.get("clickhouse.database", "datamart")

    def _target(self, table_name: str) -> str:
        return f"{self.catalog}.{self.database}.{table_name}"

    def write(self, data, table_name: str):
        """
        Escreve o DataFrame na tabela ClickHouse já existente (pré-criada via ddl/01).

        Usa `append` porque a dedup por chave (hk_business_id) é responsabilidade do
        engine ReplacingMergeTree no lado do ClickHouse — é justamente o merge que a
        POC quer medir. A tabela precisa existir; a criação/engine/partição fica no DDL
        para controlar os settings de merge.
        """
        target = self._target(table_name)
        self.logger.info(f"Escrevendo DataFrame no ClickHouse: {target}")
        data.writeTo(target).append()
        self.logger.info(f"Escrita no ClickHouse concluída: {target}")
