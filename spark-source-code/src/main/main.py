import os
import json
import argparse
import time
import logging

from core.pipeline_factory import PipelineFactory
from core.table_runner import TableRunner, format_report
from utils.config_manager import ConfigManager
from utils.session import SparkSessionFactory
from utils.pipeline_config import PipelineConfig
from utils.handler_logger import initialize_logger, apply_and_trace_context

# init global logger
logger_base = initialize_logger()

def get_spark_param():
    parser = argparse.ArgumentParser(description='Processa dados de ingestão.')

    parser.add_argument('--pipeline', type=str, required=False,
                        help='Define qual camada de processamento será executada')
    parser.add_argument('--tenant_name', type=str, required=True,
                        help='cliente referente ao processo')
    parser.add_argument('--filial_name', type=str, required=True,
                        help='filial a ser processada, deve ser igual ao tenant caso não exista filiais')

    # MODO MULTI-TABELA (padrão novo): um pod por filial processa N tabelas.
    parser.add_argument('--tables_json', type=str, required=False,
                        help='JSON com [{"name": "<tabela>", "chave_pk": ["col", ...]}, ...]. '
                             'Presente => modo multi-tabela.')
    parser.add_argument('--max_workers', type=int, default=4,
                        help='Threads de tabela simultâneas dentro do driver (modo multi-tabela). '
                             'Casar com os slots de task dos executors (instances x cores).')

    # MODO SINGLE-TABLE (legado): um pod por tabela. Mantido de propósito — é o caminho de
    # rollback do modo multi-tabela e o formato ainda usado pelos pipelines gold.
    parser.add_argument('--table_name', type=str, required=False,
                        help='tabela alvo do processo (modo single-table)')
    parser.add_argument('--primary_key', type=str, required=False, nargs='*',
                        help='Conjunto de colunas que serão aplicadas o Hash unico (modo single-table)')

    parser.add_argument('--is_merge_schema', type=str, default="false", choices=["true", "false"],
                        help='Opção para habilitar ou não o recurso merge_schema do Delta Table')
    parser.add_argument('--colunas_zorder', type=str, nargs='*',
                        help='Colunas que dever ser utilizadas para o processo de z-order, caso não seja passado é executado um optimize generico')

    # MANUTENCAO DELTA (--pipeline delta_maintenance). Ignorados pelos demais pipelines.
    parser.add_argument('--delta_layer', type=str, default='silver', choices=['silver', 'gold'],
                        help='Camada Delta alvo da manutenção (OPTIMIZE/VACUUM).')
    parser.add_argument('--retention_hours', type=int, default=168,
                        help='Horas de retenção do VACUUM. Default 168 (7 dias). Valores menores '
                             'apagam versões mais recentes e quebram o time travel nessa janela.')

    # SUPER TENANT (--pipeline silver_super_tenant). Ignorado pelos demais pipelines.
    parser.add_argument('--janela_inicio', type=str, required=False,
                        help='Inicio da janela de carga, inclusivo (yyyy-MM-dd HH:mm:ss)')
    parser.add_argument('--janela_fim', type=str, required=False,
                        help='Fim da janela de carga, exclusivo (yyyy-MM-dd HH:mm:ss)')
    parser.add_argument('--source_tenants', type=str, required=False,
                        help='JSON com [{"tenant": "<cliente>", "base_path": "s3a://<bucket>"}, ...]. '
                             'Cada cliente de origem vira uma FILIAL do super tenant.')

    args = parser.parse_args()

    # Os dois modos sao mutuamente exclusivos. Sem esta guarda, passar ambos por engano faria
    # o --table_name ser silenciosamente ignorado.
    if args.tables_json and args.table_name:
        parser.error("--tables_json e --table_name sao mutuamente exclusivos: escolha um modo.")
    if not args.tables_json and not args.table_name:
        parser.error("informe --tables_json (multi-tabela) ou --table_name (single-table).")
    if args.table_name and not args.primary_key:
        parser.error("--primary_key e obrigatorio no modo single-table.")

    # Guardas simetricas de proposito. A primeira evita rodar a consolidacao sem origem: sem
    # ela a falha sairia la embaixo, sem dizer o que falta. A segunda evita o silencio inverso —
    # passar --source_tenants para bronze_silver por copiar/colar a DAG errada rodaria a ingestao
    # normal ignorando o argumento, e ninguem perceberia.
    if args.pipeline == "silver_super_tenant" and not args.source_tenants:
        parser.error("--source_tenants e obrigatorio para --pipeline silver_super_tenant.")
    if args.source_tenants and args.pipeline != "silver_super_tenant":
        parser.error("--source_tenants so se aplica a --pipeline silver_super_tenant.")

    return args

def get_environment_parameters():
    return ConfigManager()

def main():
    args = get_spark_param()
    tabelas = json.loads(args.tables_json) if args.tables_json else None

    runtime_parameters = PipelineConfig.from_args(args) #runtime parameters = config_parans
    environment_parameters = get_environment_parameters() # .env parameters = environment_parameters

    try:
        queries_dir = os.path.join(environment_parameters.config_base_path, "queries")

        app_name = (
            f"ingestao_{runtime_parameters.filial_name}_{len(tabelas)}tabelas"
            if tabelas else
            f"ingestao_{runtime_parameters.filial_name}_{runtime_parameters.table_name}"
        )
        spark = SparkSessionFactory.create_spark_session(app_name, environment_parameters)

        if runtime_parameters is None:
            raise Exception("runtime_parameters está None antes de criar a pipeline")
        logger = apply_and_trace_context(logger_base, runtime_parameters, spark)

        logger.info("Inicio da ingestão spark.")

        if tabelas:
            resultados = TableRunner(
                spark=spark,
                environment_parameters=environment_parameters,
                base_config=runtime_parameters,
                queries_dir=queries_dir,
                max_workers=args.max_workers,
            ).run(tabelas)

            logger.info(format_report(runtime_parameters.filial_name, resultados))

            falhas = [r for r in resultados if not r.ok]
            if falhas:
                # Relanca para que o exit code do driver marque a SparkApplication como FAILED
                # e a task fique vermelha no Airflow. O retry re-executa a filial INTEIRA, o que
                # e aceitavel: as tabelas que ja concluiram tiveram seus arquivos de origem
                # movidos para `processed/` e viram no-op na segunda passada.
                raise RuntimeError(
                    f"{len(falhas)}/{len(resultados)} tabela(s) falharam: "
                    f"{[r.name for r in falhas]}"
                )
        else:
            factory = PipelineFactory()
            instance_pipeline = factory.new_instance(
                key=runtime_parameters.pipeline,
                spark=spark,
                environment_parameters=environment_parameters,
                runtime_parameters=runtime_parameters,
                queries_dir=queries_dir
            )

            if instance_pipeline:
                instance_pipeline.run()

    except Exception as e:
        # Relanca: o status COMPLETED/FAILED da SparkApplication vem do exit code do driver.
        # logger_base (global) porque `logger` so existe apos apply_and_trace_context.
        logger_base.exception(f"Falha crítica: {str(e)}")
        raise

    finally:
        if 'spark' in locals():
            spark.stop()
            logger_base.info("Processo finalizado.")

if __name__ == "__main__":
    main()
