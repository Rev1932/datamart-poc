import logging

import pyspark.sql.functions as F
from tranformer.base import Transformer
from pyspark.sql import DataFrame
from utils.handler_logger import initialize_logger

# init global logger
logger_base = initialize_logger()


class ValidateBusinessRulesTransformer(Transformer):
    """
    Prepara dados para formato hub data vaults

    Implementa lógica para remover colunas datalake.
    """
       
    def transform(self, data) -> DataFrame:
        data = (data
            .filter("quarentena == False")
            .drop("quarentena")
        )
        return data     


class HardBusinessRulesTransformerAuto(Transformer):
    """
    Aplica regras de negócio rígidas (Hard Business Rules) para garantir a integridade do Data Vault.

    Esta classe foca na validação da Business Key. Registros que possuem qualquer parte 
    da chave primária nula são marcados com a flag 'quarentena', impedindo que chaves 
    inválidas corrompam os Hubs e Satélites do Data Vault.
    """

    def __init__(self, environment_parameters: dict, runtime_parameters: dict) -> None:
        """
        Inicializa o transformador com base nas chaves de negócio configuradas.

        Args:
            environment_parameters (dict): Configurações globais da aplicação.
            runtime_parameters (dict): Parâmetros contendo 'primary_key' (lista de colunas da PK).
        """
        self.logger = initialize_logger()

        self.business_key = runtime_parameters.get("primary_key", [])
        self.logger.debug(f"pipeline_orchestrator HardBusinessRulesTransformerAuto inicializado.")
        self.logger.debug(f"[DEBUG] Business Keys configuradas: {self.business_key}")

    def transform(self, data: DataFrame) -> DataFrame:
        """
        Identifica registros com Business Keys nulas e aplica a coluna de quarentena.

        Args:
            data (DataFrame): DataFrame de entrada (Raw Vault Initialized).

        Returns:
            DataFrame: DataFrame com a coluna booleana 'quarentena' adicionada.
        """
        if not self.business_key:
            self.logger.error("[ERROR] Nenhuma Business Key (primary_key) foi definida nas configurações.")
            raise ValueError("A primary_key não pode estar vazia para esta transformação.")

        self.logger.debug(f"pipeline_orchestrator Iniciando validação de Hard Rules (Quarentena) em {len(self.business_key)} colunas.")
        
        try:
            # Converte strings de nomes de colunas em objetos Column do Spark
            colunas_chave = [F.col(c) for c in self.business_key]
            self.logger.debug(f"[DEBUG] Aplicando filtro de nulidade sobre: {self.business_key}")

            # Lógica: Se qualquer coluna da chave composta for nula, o registro vai para quarentena
            # F.array(*colunas_chave) agrupa os valores, F.filter remove o que não é nulo. 
            # Se o tamanho final for > 0, existia pelo menos um nulo.
            condicao_quarentena = F.size(F.filter(F.array(*colunas_chave), lambda x: x.isNull())) > 0

            data_with_quarantine = data.withColumn(
                "quarentena",
                F.when(condicao_quarentena, F.lit(True)).otherwise(F.lit(False))
            )

            # Debug de Volumetria (Data Quality). O count() dispara um Job Spark COMPLETO —
            # com N tabelas por pod isso vira N jobs so para uma mensagem de log. Por isso so
            # roda quando o nivel DEBUG esta de fato habilitado.
            if self.logger.isEnabledFor(logging.DEBUG):
                quarentena_count = data_with_quarantine.filter(F.col("quarentena") == True).count()

                if quarentena_count > 0:
                    self.logger.debug(f"pipeline_orchestrator Validação concluída: {quarentena_count} registros marcados para QUARENTENA.")
                else:
                    self.logger.debug(f"pipeline_orchestrator Validação concluída: Nenhum registro inválido detectado.")

            return data_with_quarantine

        except Exception as e:
            self.logger.error(f"[ERROR] Falha ao aplicar regras de quarentena: {str(e)}")
            raise

class RawDataVaultInitTransformerAuto(Transformer):        
    """
    Inicializa a estrutura de Raw Data Vault para o ecossistema Data-Bee.

    Este transformador aplica as colunas de metadados padrões (audit columns) e 
    gera a Hash Key (hk_business_id) baseada na concatenação das chaves de negócio 
    e o sistema de origem, garantindo a unicidade e integridade no Data Vault.
    """

    def __init__(self, environment_parameters: dict, runtime_parameters: dict):
        """
        Inicializa o transformador configurando a origem e as chaves de negócio.

        Args:
            environment_parameters (dict): Configurações globais (contém mapeamento de unidades).
            runtime_parameters (dict): Parâmetros da tabela (contém primary_key e config_name).
        """
        self.logger = initialize_logger() 
        
        unit_name = runtime_parameters["filial_name"]
        self.source_system = f"data-bee_{unit_name}"
        self.business_key = runtime_parameters.get("primary_key", [])
        
        self.logger.debug(f"Pipeline_orchestrator RawDataVaultInitTransformerAuto inicializado para a unidade: {unit_name}")
        self.logger.debug(f"Source System definido: {self.source_system}")
        self.logger.debug(f"Colunas base para Hash Key: {self.business_key}")

    def transform(self, data: DataFrame) -> DataFrame:
        """
        Aplica as transformações de inicialização de Raw Vault.

        Gera:
            - hk_business_id: SHA-256 da concatenação das chaves de negócio + source.
            - source: Identificador do sistema de origem.
            - load_dts: Timestamp do processamento.
            - source_file: Nome do arquivo físico de origem (rastreabilidade).
            - processed_file: Flag de status para controle de limpeza.

        Args:
            data (DataFrame): DataFrame de entrada alinhado.

        Returns:
            DataFrame: DataFrame com metadados de Data Vault.
        """
        self.logger.debug(f"Iniciando geração de metadados Raw Vault.")
        
        if not self.business_key:
            self.logger.error(" Impossível gerar Hash Key: 'primary_key' não configurada.")
            raise ValueError("A lista de business_key está vazia.")

        try:
            # Prepara a lista de colunas para o Hash: PKs + Sistema de Origem
            key_columns = [F.col(c) for c in self.business_key] + [F.lit(self.source_system)]
            
            self.logger.debug(f"Gerando hk_business_id usando SHA-256 e separador '_'")

            transformed_df = (data
                .withColumn("source", F.lit(self.source_system))
                .withColumn(
                    "hk_business_id", 
                    F.sha2(F.concat_ws("_", *key_columns), 256)
                ) 
                .withColumn("source_file", F.input_file_name()) 
                .withColumn("processed_file", F.lit("Y"))
                .withColumn("load_dts", F.current_timestamp())
            )

            self.logger.debug(f"Metadados e Hash Keys gerados com sucesso.")
            return transformed_df

        except Exception as e:
            self.logger.error(f"Erro ao processar transformações de Raw Vault: {str(e)}")
            raise
