import pytest

from core.pipeline_orchestrator import (
    PipelineDeltaMaintenance,
    SILVER_FOLDER_FALLBACK,
    normalize_zorder_columns,
    resolve_delta_path,
)
from utils.pipeline_config import PipelineConfig

# ConfigManager expõe get("caminho.pontilhado"); um dict de chaves pontilhadas satisfaz o
# mesmo contrato sem precisar montar env vars.
ENV = {
    "minio.base_path": "s3a://datawake-unipac",
    "trino.schema_folder_silver": "business_datavault_data-bee",
    "trino.schema_folder_gold": "service_data_data-bee",
}


def _config(**overrides):
    base = dict(
        pipeline="delta_maintenance",
        tenant_name="unipac",
        filial_name="paulinia",
        table_name="dw_refugo",
        primary_key=["id"],
    )
    base.update(overrides)
    return PipelineConfig(**base)


# --------------------------------------------------------------------------------------
# resolve_delta_path — a convenção de path é a única fonte de verdade (não há metastore)
# --------------------------------------------------------------------------------------

def test_path_silver_usa_a_pasta_do_schema_silver():
    assert resolve_delta_path(ENV, "dw_refugo", "silver") == (
        "s3a://datawake-unipac/business_datavault_data-bee/dw_refugo"
    )


def test_path_gold_usa_a_pasta_do_schema_gold():
    assert resolve_delta_path(ENV, "fact_200_cep", "gold") == (
        "s3a://datawake-unipac/service_data_data-bee/fact_200_cep"
    )


def test_path_silver_cai_no_fallback_quando_a_env_nao_vem():
    """
    RepositoryBronzeToSilver monta esse path com a pasta HARDCODED. Sem o fallback, uma env
    ausente montaria 's3a://bucket/None/tabela' e a manutenção rodaria no lugar errado.
    """
    env = {"minio.base_path": "s3a://datawake-unipac"}
    assert resolve_delta_path(env, "dw_refugo", "silver") == (
        f"s3a://datawake-unipac/{SILVER_FOLDER_FALLBACK}/dw_refugo"
    )


def test_path_gold_sem_env_falha_explicitamente():
    """Gold não tem fallback: a pasta varia por tenant, chutar apagaria a tabela errada."""
    env = {"minio.base_path": "s3a://datawake-unipac"}
    with pytest.raises(ValueError, match="TRINO_SCHEMA_FOLDER_GOLD"):
        resolve_delta_path(env, "fact_200_cep", "gold")


def test_path_sem_base_path_falha():
    with pytest.raises(ValueError, match="MINIO_BASE_PATH"):
        resolve_delta_path({}, "dw_refugo", "silver")


def test_camada_desconhecida_falha_nomeando_a_camada():
    with pytest.raises(ValueError, match="bronze"):
        resolve_delta_path(ENV, "dw_refugo", "bronze")


# --------------------------------------------------------------------------------------
# normalize_zorder_columns — None significa OPTIMIZE genérico em DeltaMaintenance
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize("entrada", [None, [], "", False, [" ", ""]])
def test_zorder_vazio_vira_none(entrada):
    """Lista vazia NÃO pode virar executeZOrderBy() sem colunas — isso estoura no Delta."""
    assert normalize_zorder_columns(entrada) is None


def test_zorder_lista_do_argparse_e_preservada():
    assert normalize_zorder_columns(["data_referencia", "filial"]) == [
        "data_referencia", "filial"
    ]


def test_zorder_string_separada_por_virgula_vira_lista():
    assert normalize_zorder_columns("data_referencia, filial") == [
        "data_referencia", "filial"
    ]


# --------------------------------------------------------------------------------------
# PipelineDeltaMaintenance
# --------------------------------------------------------------------------------------

def test_init_resolve_path_camada_e_retencao():
    pipeline = PipelineDeltaMaintenance(None, ENV, _config())

    assert pipeline.output_path == "s3a://datawake-unipac/business_datavault_data-bee/dw_refugo"
    assert pipeline.layer == "silver"
    assert pipeline.retention_hours == 168
    assert pipeline.z_order_columns is None


def test_init_respeita_camada_retencao_e_zorder():
    pipeline = PipelineDeltaMaintenance(None, ENV, _config(
        table_name="fact_200_cep",
        delta_layer="gold",
        retention_hours=24,
        colunas_zorder=["data_referencia"],
    ))

    assert pipeline.output_path == "s3a://datawake-unipac/service_data_data-bee/fact_200_cep"
    assert pipeline.retention_hours == 24
    assert pipeline.z_order_columns == ["data_referencia"]


def test_preparer_falha_nomeando_a_tabela_quando_o_destino_nao_e_delta(monkeypatch):
    """
    Uma tabela digitada errada no --tables_json precisa falhar AQUI, nomeada, e ser
    contabilizada pelo TableRunner — não passar como sucesso silencioso.
    """
    import delta.tables

    class FakeDeltaTable:
        @staticmethod
        def isDeltaTable(spark, path):
            return False

    monkeypatch.setattr(delta.tables, "DeltaTable", FakeDeltaTable)
    pipeline = PipelineDeltaMaintenance(None, ENV, _config(table_name="dw_typo"))

    with pytest.raises(ValueError, match="dw_typo"):
        pipeline.preparer()


def test_process_roda_optimize_antes_de_vacuum(monkeypatch):
    """
    OPTIMIZE primeiro (reescreve e marca os antigos como removidos), VACUUM depois (apaga o
    que já passou da retenção). Invertido, o VACUUM não vê nada do que o OPTIMIZE produziu.
    """
    chamadas = []

    class FakeDeltaMaintenance:
        def __init__(self, spark, output_path):
            chamadas.append(("init", output_path))

        def run_optimize(self, z_order_columns=None):
            chamadas.append(("optimize", z_order_columns))

        def run_vacuum(self, retention_period=168):
            chamadas.append(("vacuum", retention_period))

    import utils.delta_maintenance

    monkeypatch.setattr(utils.delta_maintenance, "DeltaMaintenance", FakeDeltaMaintenance)
    pipeline = PipelineDeltaMaintenance(None, ENV, _config(
        retention_hours=48, colunas_zorder=["data_referencia"]))
    pipeline.process()

    assert chamadas == [
        ("init", "s3a://datawake-unipac/business_datavault_data-bee/dw_refugo"),
        ("optimize", ["data_referencia"]),
        ("vacuum", 48),
    ]
