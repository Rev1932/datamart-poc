import json
import sys
import types

import pytest

import main as main_module


class FakeSpark:
    def __init__(self):
        self.parou = False
        self.sparkContext = types.SimpleNamespace(
            applicationId="app-teste",
            setLocalProperty=lambda *a, **k: None,
        )

    def stop(self):
        self.parou = True


@pytest.fixture
def spark_falso(monkeypatch):
    fake = FakeSpark()
    monkeypatch.setattr(
        main_module.SparkSessionFactory,
        "create_spark_session",
        staticmethod(lambda *args, **kwargs: fake),
    )
    return fake


def _argv(monkeypatch, *extras):
    monkeypatch.setattr(sys, "argv", ["main.py", *extras])


@pytest.fixture
def argv_de_pipeline_inexistente(monkeypatch):
    """PipelineFactory.new_instance levanta para um --pipeline fora do mapa."""
    _argv(
        monkeypatch,
        "--pipeline", "pipeline-que-nao-existe",
        "--tenant_name", "unipac",
        "--filial_name", "limeira",
        "--table_name", "fact_200_cep",
        "--primary_key", "andon_peso_id", "filial", "banco",
    )


@pytest.fixture(autouse=True)
def config_falsa(monkeypatch):
    """
    ConfigManager real localizaria resources/ no disco e leria o ambiente; aqui só
    interessa o config_base_path, usado por main() para montar queries_dir.
    """
    monkeypatch.setattr(
        main_module, "get_environment_parameters",
        lambda *args, **kwargs: types.SimpleNamespace(config_base_path="/tmp"),
    )


# --------------------------------------------------------------------------------------
# Contrato de exit code (vale para os dois modos)
# --------------------------------------------------------------------------------------

def test_falha_propaga_em_vez_de_virar_exit_zero(argv_de_pipeline_inexistente, spark_falso):
    """
    O status COMPLETED/FAILED da SparkApplication vem do exit code do driver: se
    main() engolir a exceção, um job quebrado reporta sucesso ao operator.
    """
    with pytest.raises(Exception, match="não encontrado"):
        main_module.main()


def test_sessao_spark_e_encerrada_mesmo_em_falha(argv_de_pipeline_inexistente, spark_falso):
    with pytest.raises(Exception):
        main_module.main()

    assert spark_falso.parou is True


# --------------------------------------------------------------------------------------
# Seleção de modo (multi-tabela x single-table)
# --------------------------------------------------------------------------------------

def _argv_multi(monkeypatch, tabelas, **extras):
    args = [
        "--pipeline", "bronze_silver",
        "--tenant_name", "unipac",
        "--filial_name", "limeira",
        "--tables_json", json.dumps(tabelas),
    ]
    for chave, valor in extras.items():
        args += [f"--{chave}", str(valor)]
    _argv(monkeypatch, *args)


def test_modo_multi_tabela_roda_todas_as_tabelas(monkeypatch, spark_falso):
    _argv_multi(monkeypatch, [
        {"name": "dw_a", "chave_pk": ["id"]},
        {"name": "dw_b", "chave_pk": ["cod"]},
    ])

    executadas = []

    class FakeFactory:
        def new_instance(self, key, spark, environment_parameters, runtime_parameters,
                         queries_dir):
            executadas.append(runtime_parameters.table_name)
            return types.SimpleNamespace(run=lambda: None)

    monkeypatch.setattr(main_module, "PipelineFactory", FakeFactory)
    monkeypatch.setattr("core.table_runner.PipelineFactory", FakeFactory)

    main_module.main()

    assert sorted(executadas) == ["dw_a", "dw_b"]
    assert spark_falso.parou is True


def test_modo_multi_tabela_falha_no_fim_quando_uma_tabela_quebra(monkeypatch, spark_falso):
    """
    Não é fail-fast: as demais tabelas rodam, e só então o driver sai com erro para que a
    task fique vermelha no Airflow. O retry re-executa a filial inteira (idempotente).
    """
    _argv_multi(monkeypatch, [
        {"name": "dw_a", "chave_pk": ["id"]},
        {"name": "dw_b", "chave_pk": ["id"]},
        {"name": "dw_c", "chave_pk": ["id"]},
    ])

    executadas = []

    class FakeFactory:
        def new_instance(self, key, spark, environment_parameters, runtime_parameters,
                         queries_dir):
            nome = runtime_parameters.table_name

            def run():
                executadas.append(nome)
                if nome == "dw_b":
                    raise RuntimeError("boom")

            return types.SimpleNamespace(run=run)

    monkeypatch.setattr("core.table_runner.PipelineFactory", FakeFactory)

    with pytest.raises(RuntimeError, match=r"1/3 tabela\(s\) falharam.*dw_b"):
        main_module.main()

    assert sorted(executadas) == ["dw_a", "dw_b", "dw_c"]
    assert spark_falso.parou is True


def test_modo_single_table_continua_funcionando(monkeypatch, spark_falso):
    """É o caminho de rollback do modo multi-tabela — não pode regredir."""
    _argv(
        monkeypatch,
        "--pipeline", "bronze_silver",
        "--tenant_name", "unipac",
        "--filial_name", "limeira",
        "--table_name", "dw_a",
        "--primary_key", "id", "filial",
    )

    recebidos = {}

    class FakeFactory:
        def new_instance(self, key, spark, environment_parameters, runtime_parameters,
                         queries_dir):
            recebidos["table_name"] = runtime_parameters.table_name
            recebidos["primary_key"] = runtime_parameters.primary_key
            return types.SimpleNamespace(run=lambda: None)

    monkeypatch.setattr(main_module, "PipelineFactory", FakeFactory)

    main_module.main()

    assert recebidos == {"table_name": "dw_a", "primary_key": ["id", "filial"]}


# --------------------------------------------------------------------------------------
# Guardas de argumento
# --------------------------------------------------------------------------------------

def test_recusa_os_dois_modos_juntos(monkeypatch):
    """Sem a guarda, --table_name seria silenciosamente ignorado."""
    _argv(
        monkeypatch,
        "--pipeline", "bronze_silver",
        "--tenant_name", "unipac",
        "--filial_name", "limeira",
        "--table_name", "dw_a",
        "--tables_json", '[{"name":"dw_a","chave_pk":["id"]}]',
    )

    with pytest.raises(SystemExit):
        main_module.get_spark_param()


def test_recusa_nenhum_modo(monkeypatch):
    _argv(
        monkeypatch,
        "--pipeline", "bronze_silver",
        "--tenant_name", "unipac",
        "--filial_name", "limeira",
    )

    with pytest.raises(SystemExit):
        main_module.get_spark_param()


def test_recusa_single_table_sem_primary_key(monkeypatch):
    _argv(
        monkeypatch,
        "--pipeline", "bronze_silver",
        "--tenant_name", "unipac",
        "--filial_name", "limeira",
        "--table_name", "dw_a",
    )

    with pytest.raises(SystemExit):
        main_module.get_spark_param()


# --------------------------------------------------------------------------------------
# Guardas do super tenant — simétricas de propósito
# --------------------------------------------------------------------------------------

def test_recusa_silver_super_tenant_sem_source_tenants(monkeypatch):
    """Sem a guarda, a falta de origem só apareceria lá embaixo, sem dizer o que falta."""
    _argv(
        monkeypatch,
        "--pipeline", "silver_super_tenant",
        "--tenant_name", "lakatos",
        "--filial_name", "lakatos",
        "--tables_json", '[{"name":"dw_a","chave_pk":["id"]}]',
    )

    with pytest.raises(SystemExit):
        main_module.get_spark_param()


def test_recusa_source_tenants_em_outro_pipeline(monkeypatch):
    """
    O silêncio inverso: passar --source_tenants para bronze_silver por copiar/colar a DAG errada
    rodaria a ingestão normal ignorando o argumento, e ninguém perceberia.
    """
    _argv(
        monkeypatch,
        "--pipeline", "bronze_silver",
        "--tenant_name", "unipac",
        "--filial_name", "limeira",
        "--tables_json", '[{"name":"dw_a","chave_pk":["id"]}]',
        "--source_tenants", '[{"tenant":"belafatia","base_path":"s3a://datawake-belafatia"}]',
    )

    with pytest.raises(SystemExit):
        main_module.get_spark_param()


def test_aceita_silver_super_tenant_com_source_tenants(monkeypatch):
    _argv(
        monkeypatch,
        "--pipeline", "silver_super_tenant",
        "--tenant_name", "lakatos",
        "--filial_name", "lakatos",
        "--tables_json", '[{"name":"dw_a","chave_pk":["id"]}]',
        "--source_tenants", '[{"tenant":"belafatia","base_path":"s3a://datawake-belafatia"}]',
    )

    args = main_module.get_spark_param()

    assert args.pipeline == "silver_super_tenant"
    assert "belafatia" in args.source_tenants
