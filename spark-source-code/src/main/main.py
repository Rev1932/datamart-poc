import os
import argparse
import time
import logging

from core.pipeline_factory import PipelineFactory
from utils.config_manager import ConfigManager
from utils.session import SparkSessionFactory
from utils.spark_utils import SparkUtils
from utils.pipeline_config import PipelineConfig
from utils.handler_logger import initialize_logger, apply_and_trace_context

# init global logger
logger_base = initialize_logger()


def get_spark_param():
    parser = argparse.ArgumentParser(description='Processa dados de ingestão.')

    parser.add_argument('--pipeline', type=str, required=False,
                        help='Metodo de processamento a ser utilizado')
    parser.add_argument('--chave-pk', type=str, required=True, nargs='*',
                        help='Conjunto de colunas que serão aplicadas o Hash')
    parser.add_argument('--config-name', type=str, required=True,
                        help='client/filial a ser processada')
    parser.add_argument('--table-name', type=str, required=True,
                        help='tabela alvo do processo')
    parser.add_argument('--is_merge_schema', type=str, default="false", choices=["true", "false"],
                        help='opção para habilitar ou não o recurso merge_schema do Delta Table')
    parser.add_argument('--colunas_zorder', type=str, nargs='*',
                        help='Colunas que dever ser utilizadas para o processo de z-order, caso não seja passado é executado um optimize generico')
    return parser.parse_args()


def get_config_application(file_name="application", unique_env_name=False):
    return ConfigManager(file_name=file_name, unique_env_name=unique_env_name)


def _aguardando_criacao_pastas(spark, path_str):
    
    while not SparkUtils.path_exists(spark, path_str):
        logger_base.info("Aguardando a criação da pastas...")
        time.sleep(15)


def main():
    args = get_spark_param()
    config_params = PipelineConfig.from_args(args)
    config_application = get_config_application()

    try:
        queries_dir = os.path.join(config_application.config_base_path, "queries")

        spark = SparkSessionFactory.create_spark_session(
            f"ingestao_{config_params.topic}",
            config_application
        )

        if config_params is None:
            raise Exception("config_params está None antes de criar a pipeline")
        logger = apply_and_trace_context(logger_base, config_params, spark)

        logger.info("Inicio da ingestão spark.")
        factory = PipelineFactory()
        instance_pipeline = factory.new_instance(
            key=config_params.pipeline_type,
            spark=spark,
            config_application=config_application,
            config_params=config_params,
            query_config=queries_dir
        )

        if instance_pipeline:
            instance_pipeline.run()

    except Exception as e:

        logger.error(f"Falha crítica: {str(e)}")

    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Processo finalizado.")

if __name__ == "__main__":
    main()