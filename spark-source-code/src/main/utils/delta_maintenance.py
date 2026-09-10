from delta.tables import DeltaTable

from utils.handler_logger import initialize_logger


class DeltaMaintenance():
    """
    OPTIMIZE e VACUUM a nivel de tabela Delta, enderecada por PATH.

    Por que os erros SOBEM daqui: o status COMPLETED/FAILED da SparkApplication vem do exit
    code do driver. Enquanto esta classe engolia as excecoes, um VACUUM que falhava deixava o
    job verde no Airflow sem ter apagado nada — o mesmo modo de falha silenciosa descrito em
    core/pipeline.py (PipelineProcess.run) e repo/repository.py (RepositoryBronzeToSilver.read).
    """

    def __init__(self, spark, output_path):
        self.spark = spark
        self.output_path = output_path
        self.logger = initialize_logger()
        self.delta_table = DeltaTable.forPath(self.spark, self.output_path)

    def run_optimize(self, z_order_columns=None):
        """
            Executa OPTIMIZE a nivel de tabela
        """

        # Lista vazia nao e `None`: sem esta normalizacao, `--colunas_zorder` declarado sem
        # valores cairia em `executeZOrderBy()` sem colunas, que estoura.
        if not z_order_columns:
            z_order_columns = None

        try:
            if z_order_columns is None:
                self.logger.info(f"Executando OPTIMIZE generico em {self.output_path}")
                self.delta_table.optimize().executeCompaction()
            else:
                self.logger.info(
                    f"Executando OPTIMIZE Z-ORDER em {self.output_path} "
                    f"nas colunas: {z_order_columns}"
                )
                self.delta_table.optimize().executeZOrderBy(*z_order_columns)
            self.logger.info(f"OPTIMIZE concluido para {self.output_path}.")
        except Exception as e:
            self.logger.error(
                f"Erro ao otimizar tabela {self.output_path}: {str(e)}", exc_info=True
            )
            raise

    def run_vacuum(self, retention_period=168):
        """
        Executa VACUUM a nível de tabela

        Args:
            retention_period (float): janela de retencao em HORAS. Arquivos removidos
                logicamente ha menos tempo que isso sao preservados. O default 168 (7 dias) e o
                mesmo do Delta; abaixo dele so funciona porque utils/session.py desliga o
                `retentionDurationCheck` — e apaga versoes ainda referenciadas por time travel.
        """
        self.logger.info(
            f"Iniciando VACUUM para a tabela: {self.output_path} "
            f"(retencao de {retention_period}h)"
        )

        try:
            self.delta_table.vacuum(retention_period)
            self.logger.info(f"VACUUM concluído com sucesso para a tabela {self.output_path}!")

        except Exception as e:
            self.logger.error(
                f"ERRO de VACUUM ao processar a tabela {self.output_path}: {str(e)}",
                exc_info=True
            )
            raise
