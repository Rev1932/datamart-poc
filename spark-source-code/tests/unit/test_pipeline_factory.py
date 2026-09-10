import pytest

from core.pipeline_factory import (
    PipelineBronzeToSilverWrapper,
    PipelineDatamartClickhouseWrapper,
    PipelineDeltaMaintenanceWrapper,
    PipelineFactory,
    PipelineGoldDatamartWrapper,
    PipelineGoldWrapper,
    PipelineSilverSuperTenantWrapper,
)
from utils.pipeline_config import PipelineConfig


def _runtime_parameters(pipeline):
    """PipelineGoldWrapper lê runtime_parameters['pipeline'] no __init__."""
    return PipelineConfig(
        pipeline=pipeline,
        tenant_name="tenant",
        filial_name="filial",
        table_name="fact_200_cep",
        primary_key=["andon_peso_id"],
    )


@pytest.mark.parametrize("key,expected_class", [
    ("gold", PipelineGoldWrapper),
    ("gold_datamart", PipelineGoldDatamartWrapper),
    ("datamart_pg", PipelineGoldDatamartWrapper),
    ("datamart_ch", PipelineDatamartClickhouseWrapper),
    ("bronze_silver", PipelineBronzeToSilverWrapper),
    ("silver_super_tenant", PipelineSilverSuperTenantWrapper),
    ("delta_maintenance", PipelineDeltaMaintenanceWrapper),
])
def test_new_instance_retorna_o_wrapper_da_chave(key, expected_class):
    instance = PipelineFactory().new_instance(
        key=key,
        spark=None,
        environment_parameters=None,
        runtime_parameters=_runtime_parameters(key),
        queries_dir="/opt/spark/app/resources/queries",
    )
    assert isinstance(instance, expected_class)


def test_new_instance_repassa_queries_dir_ao_wrapper():
    """
    Regressão: main.py chama new_instance(queries_dir=...) e o factory declarava
    o parâmetro como `query_config` — TypeError em toda execução.
    """
    instance = PipelineFactory().new_instance(
        key="gold_datamart",
        spark=None,
        environment_parameters=None,
        runtime_parameters=_runtime_parameters("gold_datamart"),
        queries_dir="/opt/spark/app/resources/queries",
    )
    assert instance.queries_dir == "/opt/spark/app/resources/queries"


def test_new_instance_chave_desconhecida_erra_nomeando_a_chave():
    with pytest.raises(Exception, match="silver_gold"):
        PipelineFactory().new_instance(
            key="silver_gold",
            spark=None,
            environment_parameters=None,
            runtime_parameters=_runtime_parameters("silver_gold"),
            queries_dir="/opt/spark/app/resources/queries",
        )
