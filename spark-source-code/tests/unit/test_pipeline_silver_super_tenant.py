"""Orquestração da consolidação do super tenant (`PipelineSilverSuperTenant`).

Duas propriedades importam mais que a velocidade, e são as mesmas que o TableRunner protege um
nível acima:

1. ISOLAMENTO DE FALHA POR ORIGEM. Um cliente com problema não pode impedir a consolidação dos
   outros — mas a falha precisa chegar ao exit code do driver, senão a SparkApplication fica
   COMPLETED com dado faltando.
2. SCHEMA RESOLVIDO ANTES DE ESCREVER. Se a primeira origem criasse a tabela com o schema dela,
   toda coluna exclusiva dos demais clientes sumiria em silêncio.
"""
import types

import pytest

from core.pipeline_orchestrator import PipelineSilverSuperTenant


BELAFATIA = {"tenant": "belafatia", "base_path": "s3a://b", "path": "s3a://b/silver/dw_a"}
CLIENTE_N = {"tenant": "clienteN", "base_path": "s3a://c", "path": "s3a://c/silver/dw_a"}


class FakeRepository:
    """Duplo do RepositorySilverSuperTenant: registra a ordem das chamadas."""

    def __init__(self, origens, erros_por_tenant=None):
        self._origens = origens
        self._erros = erros_por_tenant or {}
        self.chamadas = []
        self.escritas = []

    def origens_disponiveis(self):
        self.chamadas.append("origens_disponiveis")
        return self._origens

    def resolver_schema_alvo(self, origens):
        self.chamadas.append("resolver_schema_alvo")
        return ["hk_business_id", "source", "load_dts"]

    def evoluir_schema_destino(self, target_schema):
        self.chamadas.append("evoluir_schema_destino")

    def read(self, tenant, path, target_schema):
        self.chamadas.append(f"read:{tenant}")
        if tenant in self._erros:
            raise self._erros[tenant]
        return f"DATAFRAME:{tenant}"

    def write(self, data, source):
        self.chamadas.append(f"write:{source}")
        self.escritas.append((data, source))


@pytest.fixture
def pipeline(monkeypatch):
    def _build(origens, erros_por_tenant=None):
        repo = FakeRepository(origens, erros_por_tenant)
        monkeypatch.setattr(
            "repo.repository.RepositorySilverSuperTenant",
            lambda *a, **k: repo,
        )
        instancia = PipelineSilverSuperTenant(
            spark=types.SimpleNamespace(),
            environment_parameters={},
            runtime_parameters={"table_name": "dw_a", "source_tenants": []},
        )
        return instancia, repo

    return _build


# --------------------------------------------------------------------------------------
# Ordem: descobrir e resolver schema ANTES de tocar em dado
# --------------------------------------------------------------------------------------

def test_preparer_resolve_o_schema_antes_de_qualquer_leitura_de_dado(pipeline):
    instancia, repo = pipeline([BELAFATIA])

    instancia.preparer()

    assert repo.chamadas == [
        "origens_disponiveis", "resolver_schema_alvo", "evoluir_schema_destino"
    ]
    # nenhuma leitura de dado aconteceu
    assert not any(c.startswith("read:") for c in repo.chamadas)


def test_cada_origem_e_escrita_na_sua_particao(pipeline):
    """
    O predicado de partição do merge só é válido para UM tenant — é ele que faz o Delta tocar
    apenas a partição daquele cliente e evita o ConcurrentAppendException.
    """
    instancia, repo = pipeline([BELAFATIA, CLIENTE_N])

    instancia.preparer()
    instancia.process()

    assert repo.escritas == [
        ("DATAFRAME:belafatia", "data-bee_belafatia"),
        ("DATAFRAME:clienteN", "data-bee_clienteN"),
    ]


def test_origens_sao_processadas_sequencialmente_na_ordem_declarada(pipeline):
    """
    Sequencial de propósito: as N origens escrevem na MESMA tabela Delta. Paralelizar aqui
    recriaria dentro de um pod a corrida entre filiais que o predicado de partição resolve
    entre pods.
    """
    instancia, repo = pipeline([BELAFATIA, CLIENTE_N])

    instancia.preparer()
    instancia.process()

    leituras_e_escritas = [c for c in repo.chamadas if ":" in c]
    assert leituras_e_escritas == [
        "read:belafatia", "write:data-bee_belafatia",
        "read:clienteN", "write:data-bee_clienteN",
    ]


# --------------------------------------------------------------------------------------
# Isolamento de falha
# --------------------------------------------------------------------------------------

def test_falha_de_uma_origem_nao_impede_as_demais(pipeline):
    instancia, repo = pipeline(
        [BELAFATIA, CLIENTE_N],
        erros_por_tenant={"belafatia": RuntimeError("bucket fora do ar")},
    )
    instancia.preparer()

    with pytest.raises(RuntimeError):
        instancia.process()

    # clienteN foi consolidado apesar da falha de belafatia
    assert repo.escritas == [("DATAFRAME:clienteN", "data-bee_clienteN")]


def test_falha_de_origem_relanca_nomeando_o_tenant(pipeline):
    """
    Nunca engolida: o status COMPLETED/FAILED da SparkApplication vem do exit code do driver.
    Uma origem que falha em silêncio deixaria a tabela verde com o cliente faltando no relatório
    do super tenant — e ninguém perceberia até o cliente reclamar.
    """
    instancia, repo = pipeline(
        [BELAFATIA, CLIENTE_N],
        erros_por_tenant={"belafatia": RuntimeError("bucket fora do ar")},
    )
    instancia.preparer()

    with pytest.raises(RuntimeError) as excinfo:
        instancia.process()

    mensagem = str(excinfo.value)
    assert "1/2" in mensagem
    assert "belafatia" in mensagem
    assert "bucket fora do ar" in mensagem


def test_todas_as_origens_ok_nao_levanta(pipeline):
    instancia, _ = pipeline([BELAFATIA, CLIENTE_N])
    instancia.preparer()
    instancia.process()  # não levanta
