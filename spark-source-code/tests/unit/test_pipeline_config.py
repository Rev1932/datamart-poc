from argparse import Namespace
from dataclasses import replace

from utils.pipeline_config import PipelineConfig


def _args(**overrides):
    """Espelha os argumentos reais de main.get_spark_param()."""
    base = dict(
        pipeline="gold_datamart",
        tenant_name="unipac",
        filial_name="limeira",
        table_name="fact_200_cep",
        primary_key=["andon_peso_id", "filial", "banco"],
        is_merge_schema="false",
        colunas_zorder=None,
        delta_layer="silver",
        retention_hours=168,
    )
    base.update(overrides)
    return Namespace(**base)


def test_campos_vem_dos_args():
    config = PipelineConfig.from_args(_args())
    assert config.pipeline == "gold_datamart"
    assert config.tenant_name == "unipac"
    assert config.filial_name == "limeira"
    assert config.table_name == "fact_200_cep"


def test_is_merge_schema_true():
    assert PipelineConfig.from_args(_args(is_merge_schema="true")).is_merge_schema is True


def test_is_merge_schema_false():
    assert PipelineConfig.from_args(_args(is_merge_schema="false")).is_merge_schema is False


def test_is_merge_schema_ausente_vira_false():
    """from_args usa getattr(args, 'is_merge_schema', 'false') — atributo ausente vira False."""
    args = _args()
    del args.is_merge_schema
    assert PipelineConfig.from_args(args).is_merge_schema is False


def test_primary_key_mantida_como_lista():
    config = PipelineConfig.from_args(_args())
    assert config.primary_key == ["andon_peso_id", "filial", "banco"]


def test_get_retorna_default_para_chave_inexistente():
    config = PipelineConfig.from_args(_args())
    assert config.get("nao_existe", "padrao") == "padrao"


def test_getitem_de_chave_inexistente_retorna_none():
    config = PipelineConfig.from_args(_args())
    assert config["nao_existe"] is None


def test_setitem_atualiza_o_campo():
    config = PipelineConfig.from_args(_args())
    config["table_name"] = "dim_turno"
    assert config.table_name == "dim_turno"


def test_to_dict_expoe_todos_os_campos():
    config = PipelineConfig.from_args(_args())
    assert set(config.to_dict().keys()) == {
        "pipeline", "tenant_name", "filial_name", "table_name",
        "primary_key", "is_merge_schema", "colunas_zorder",
        "delta_layer", "retention_hours", "source_tenants",
        "janela_inicio", "janela_fim", "colunas_obrigatorias",
    }


def test_janela_vem_dos_args():
    config = PipelineConfig.from_args(
        _args(janela_inicio="2026-09-01 00:00:00", janela_fim="2026-10-01 00:00:00"))
    assert config.janela_inicio == "2026-09-01 00:00:00"
    assert config.janela_fim == "2026-10-01 00:00:00"


def test_janela_ausente_fica_none():
    config = PipelineConfig.from_args(_args())
    assert config.janela_inicio is None
    assert config.janela_fim is None


def test_colunas_obrigatorias_vem_dos_args():
    config = PipelineConfig.from_args(_args(colunas_obrigatorias=["timestamp", "filial"]))
    assert config.colunas_obrigatorias == ["timestamp", "filial"]


def test_colunas_obrigatorias_ausente_fica_lista_vazia():
    """Vazio desliga a limpeza: quem nao declara mantem o comportamento antigo."""
    assert PipelineConfig.from_args(_args()).colunas_obrigatorias == []


def test_parametros_de_manutencao_vem_dos_args():
    config = PipelineConfig.from_args(_args(delta_layer="gold", retention_hours=24))
    assert config.delta_layer == "gold"
    assert config.retention_hours == 24


def test_parametros_de_manutencao_ausentes_caem_no_default():
    """Os pipelines de ingestao nao passam estes args — a config precisa nascer valida."""
    args = _args()
    del args.delta_layer
    del args.retention_hours

    config = PipelineConfig.from_args(args)
    assert config.delta_layer == "silver"
    assert config.retention_hours == 168


# --------------------------------------------------------------------------------------
# Modo multi-tabela: a config base nasce sem tabela e o TableRunner deriva uma por tabela
# --------------------------------------------------------------------------------------

def test_modo_multi_tabela_nasce_sem_tabela_nem_pk():
    """No modo multi-tabela o argparse deixa table_name e primary_key como None."""
    args = _args(table_name=None, primary_key=None)
    config = PipelineConfig.from_args(args)

    assert config.table_name is None
    # `[]` e nao None: e o que HardBusinessRulesTransformerAuto e os repositorios esperam.
    assert config.primary_key == []


def test_replace_produz_instancias_independentes():
    """
    A dataclass é MUTÁVEL e as camadas de baixo leem os campos no __init__. Se as threads
    compartilhassem a instância, uma tabela sobrescreveria os parâmetros da outra.
    """
    base = PipelineConfig.from_args(_args(table_name=None, primary_key=None))

    a = replace(base, table_name="dw_a", primary_key=["id"])
    b = replace(base, table_name="dw_b", primary_key=["cod", "filial"])

    assert (a.table_name, a.primary_key) == ("dw_a", ["id"])
    assert (b.table_name, b.primary_key) == ("dw_b", ["cod", "filial"])
    # a base permanece intacta
    assert base.table_name is None
    assert base.primary_key == []
    assert a is not b is not base


def test_source_tenants_sobrevive_ao_replace_de_tabela():
    """
    `--source_tenants` e config de NIVEL DE JOB: vale para todas as tabelas do lote. Se o
    `replace` do TableRunner a perdesse, cada thread montaria um repositorio sem origem alguma
    e a consolidacao falharia tabela a tabela sem motivo aparente.
    """
    base = PipelineConfig.from_args(_args(
        pipeline="silver_super_tenant",
        table_name=None,
        primary_key=None,
        source_tenants='[{"tenant": "belafatia", "base_path": "s3a://datawake-belafatia"}]',
    ))

    derivada = replace(base, table_name="dw_ordem_producao", primary_key=["id"])

    assert derivada.source_tenants == [
        {"tenant": "belafatia", "base_path": "s3a://datawake-belafatia"}
    ]
