"""Testes do TableRunner — a peça que permite N tabelas num pod só.

Duas propriedades justificam a existência deste módulo e são o foco aqui:
isolamento de CONFIG entre threads e isolamento de FALHA entre tabelas.
"""
import threading
import types

import pytest

from core.table_runner import TableResult, TableRunner, format_report, normalize_tables
from utils.pipeline_config import PipelineConfig


class FakeSparkContext:
    def __init__(self):
        self.pools = []
        self.lock = threading.Lock()

    def setLocalProperty(self, chave, valor):
        with self.lock:
            self.pools.append((threading.current_thread().name, chave, valor))


class FakeSpark:
    def __init__(self):
        self.sparkContext = FakeSparkContext()


@pytest.fixture
def config_base():
    return PipelineConfig(
        pipeline="bronze_silver",
        tenant_name="unipac",
        filial_name="limeira",
    )


def _runner(spark, config_base, executar, max_workers=4):
    """Runner com o PipelineFactory trocado por um duplo que chama `executar(config)`."""
    runner = TableRunner(
        spark=spark,
        environment_parameters={},
        base_config=config_base,
        queries_dir="/tmp/queries",
        max_workers=max_workers,
    )

    class FakeFactory:
        def new_instance(self, key, spark, environment_parameters, runtime_parameters,
                         queries_dir):
            return types.SimpleNamespace(run=lambda: executar(runtime_parameters))

    runner.factory = FakeFactory()
    return runner


# --------------------------------------------------------------------------------------
# normalize_tables — validação do contrato de --tables_json
# --------------------------------------------------------------------------------------

def test_normaliza_lista_valida():
    assert normalize_tables([{"name": " dw_a ", "chave_pk": ["id", " filial "]}]) == [
        {"name": "dw_a", "chave_pk": ["id", "filial"]}
    ]


def test_normaliza_chave_pk_escalar():
    """O config do Mongo pode trazer a PK como string em vez de lista."""
    assert normalize_tables([{"name": "dw_a", "chave_pk": "id"}]) == [
        {"name": "dw_a", "chave_pk": ["id"]}
    ]


@pytest.mark.parametrize("entrada, trecho", [
    ([], "lista não vazia"),
    ({"name": "dw_a"}, "lista não vazia"),
    (["dw_a"], "não é um objeto"),
    ([{"chave_pk": ["id"]}], "sem 'name'"),
    ([{"name": "dw_a", "chave_pk": []}], "sem 'chave_pk'"),
])
def test_rejeita_entrada_invalida(entrada, trecho):
    with pytest.raises(ValueError, match=trecho):
        normalize_tables(entrada)


def test_rejeita_tabela_duplicada():
    """Duas threads na mesma tabela disputariam o mesmo destino Delta sem necessidade."""
    with pytest.raises(ValueError, match="duplicada"):
        normalize_tables([
            {"name": "dw_a", "chave_pk": ["id"]},
            {"name": "dw_a", "chave_pk": ["id"]},
        ])


# --------------------------------------------------------------------------------------
# Isolamento de falha
# --------------------------------------------------------------------------------------

def test_falha_de_uma_tabela_nao_impede_as_outras(config_base):
    """
    Esta é a propriedade que o modelo de 1 pod por tabela dava de graça (cada tabela era
    uma task independente do Airflow) e que precisa ser reconstruída aqui.
    """
    executadas = []
    trava = threading.Lock()

    def executar(config):
        with trava:
            executadas.append(config.table_name)
        if config.table_name == "dw_b":
            raise RuntimeError("coluna X ausente")

    tabelas = [
        {"name": "dw_a", "chave_pk": ["id"]},
        {"name": "dw_b", "chave_pk": ["id"]},
        {"name": "dw_c", "chave_pk": ["id"]},
    ]
    resultados = _runner(FakeSpark(), config_base, executar).run(tabelas)

    assert sorted(executadas) == ["dw_a", "dw_b", "dw_c"]
    assert [(r.name, r.ok) for r in resultados] == [
        ("dw_a", True), ("dw_b", False), ("dw_c", True)
    ]
    (falha,) = [r for r in resultados if not r.ok]
    assert "RuntimeError: coluna X ausente" == falha.error


def test_ordem_do_relatorio_segue_a_entrada(config_base):
    """
    `as_completed` devolve por ordem de TÉRMINO. Sem a reordenação, o relatório final
    mudaria de ordem a cada execução, dificultando o diff entre runs.
    """
    def executar(config):
        # a primeira tabela demora mais que as outras
        if config.table_name == "dw_a":
            threading.Event().wait(0.05)

    tabelas = [{"name": n, "chave_pk": ["id"]} for n in ("dw_a", "dw_b", "dw_c")]
    resultados = _runner(FakeSpark(), config_base, executar).run(tabelas)

    assert [r.name for r in resultados] == ["dw_a", "dw_b", "dw_c"]


# --------------------------------------------------------------------------------------
# Isolamento de config entre threads
# --------------------------------------------------------------------------------------

def test_cada_tabela_recebe_a_propria_config(config_base):
    """
    PipelineConfig é uma dataclass MUTÁVEL e as camadas de baixo (repositories,
    transformers) leem table_name/primary_key no __init__. Compartilhar a instância faria
    uma tabela sobrescrever os parâmetros da outra.
    """
    vistos = []
    trava = threading.Lock()

    def executar(config):
        with trava:
            # guarda a REFERÊNCIA: comparar id() seria enganoso, porque o CPython reusa o
            # endereço de um objeto já coletado para o próximo.
            vistos.append(config)

    tabelas = [
        {"name": "dw_a", "chave_pk": ["id"]},
        {"name": "dw_b", "chave_pk": ["cod", "filial"]},
    ]
    _runner(FakeSpark(), config_base, executar).run(tabelas)

    assert sorted((c.table_name, tuple(c.primary_key)) for c in vistos) == [
        ("dw_a", ("id",)),
        ("dw_b", ("cod", "filial")),
    ]
    # instâncias distintas entre si E distintas da base
    assert len({id(c) for c in vistos}) == 2
    assert all(c is not config_base for c in vistos)


def test_config_base_nao_e_mutada(config_base):
    _runner(FakeSpark(), config_base, lambda c: None).run(
        [{"name": "dw_a", "chave_pk": ["id"]}]
    )

    assert config_base.table_name is None
    assert config_base.primary_key == []


# --------------------------------------------------------------------------------------
# Pools FAIR
# --------------------------------------------------------------------------------------

def test_define_e_limpa_o_pool_fair_por_tabela(config_base):
    """
    Sem `setLocalProperty("spark.scheduler.pool", ...)` por thread, o FAIR configurado no
    manifesto não separa nada: todos os jobs caem no pool default.

    A limpeza no final importa porque o worker do ThreadPoolExecutor é REUTILIZADO entre
    tabelas — sem ela, logs/jobs fora do escopo de uma tabela herdariam o pool anterior.
    """
    spark = FakeSpark()
    tabelas = [{"name": n, "chave_pk": ["id"]} for n in ("dw_a", "dw_b")]
    _runner(spark, config_base, lambda c: None, max_workers=1).run(tabelas)

    chamadas = [(chave, valor) for _, chave, valor in spark.sparkContext.pools]
    assert chamadas == [
        ("spark.scheduler.pool", "dw_a"),
        ("spark.scheduler.pool", None),
        ("spark.scheduler.pool", "dw_b"),
        ("spark.scheduler.pool", None),
    ]


def test_workers_limitado_ao_numero_de_tabelas(config_base):
    """Não faz sentido abrir 4 threads para 1 tabela."""
    spark = FakeSpark()
    _runner(spark, config_base, lambda c: None, max_workers=8).run(
        [{"name": "dw_a", "chave_pk": ["id"]}]
    )

    threads = {nome for nome, _, _ in spark.sparkContext.pools}
    assert len(threads) == 1


# --------------------------------------------------------------------------------------
# Relatório
# --------------------------------------------------------------------------------------

def test_relatorio_lista_as_falhas():
    """É o texto que o operador lê no log da task do Airflow."""
    relatorio = format_report("limeira", [
        TableResult("dw_a", True, 1.0),
        TableResult("dw_b", False, 2.0, "ValueError: pk vazia"),
    ])

    assert "RESUMO limeira: 1 OK, 1 FALHA" in relatorio
    assert "FALHA dw_b (2.0s) -> ValueError: pk vazia" in relatorio
