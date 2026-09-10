"""Simetria dos dois destinos do datamart e limpeza compartilhada.

O que estes testes ancoram: se um destino ler ou transformar diferente do outro, a comparação
entre Postgres e ClickHouse mede a diferença de código, não a do motor.
"""
import logging

import pytest

from repo.repository import (
    RepositoryDatamart,
    RepositoryDatamartClickhouse,
    RepositoryGoldDatamart,
)


class FakeNa:
    def __init__(self, df):
        self._df = df

    def drop(self, subset):
        self._df.registro.append(("na.drop", list(subset)))
        return FakeDF(self._df.linhas - self._df.nulos, 0, self._df.registro)


class FakeDF:
    """Duplo de DataFrame: só conta linhas e registra as chamadas."""

    def __init__(self, linhas, nulos, registro):
        self.linhas = linhas
        self.nulos = nulos
        self.registro = registro

    def dropDuplicates(self, subset):
        self.registro.append(("dropDuplicates", list(subset)))
        return self

    def count(self):
        return self.linhas

    @property
    def na(self):
        return FakeNa(self)


class RepoStub(RepositoryDatamart):
    """Instancia sem Spark nem config: só `transform` está sob teste."""

    def __init__(self, colunas_obrigatorias):
        self.logger = logging.getLogger("teste")
        self.colunas_obrigatorias = colunas_obrigatorias

    def write(self, data, **kwargs):
        raise AssertionError("write não deve ser chamado aqui")


def test_sem_colunas_obrigatorias_nao_limpa():
    """Default vazio: quem não declara mantém o comportamento anterior."""
    registro = []
    RepoStub([]).transform(FakeDF(10, 3, registro))
    assert registro == [("dropDuplicates", ["hk_business_id"])]


def test_com_colunas_obrigatorias_descarta_nulo():
    registro = []
    resultado = RepoStub(["timestamp", "filial"]).transform(FakeDF(10, 3, registro))
    assert registro == [
        ("dropDuplicates", ["hk_business_id"]),
        ("na.drop", ["timestamp", "filial"]),
    ]
    assert resultado.count() == 7


def test_dedup_vem_antes_da_limpeza():
    """Invertido, o dedup escolheria entre linhas que a limpeza já teria descartado."""
    registro = []
    RepoStub(["timestamp"]).transform(FakeDF(5, 1, registro))
    assert [chamada for chamada, _ in registro] == ["dropDuplicates", "na.drop"]


@pytest.mark.parametrize("metodo", ["read", "transform"])
def test_os_dois_destinos_compartilham_leitura_e_transformacao(metodo):
    """O aceite de T2.4: a diferença entre os braços é só `write`."""
    assert getattr(RepositoryGoldDatamart, metodo) is getattr(RepositoryDatamart, metodo)
    assert getattr(RepositoryDatamartClickhouse, metodo) is getattr(RepositoryDatamart, metodo)


def test_so_o_write_difere():
    assert RepositoryGoldDatamart.write is not RepositoryDatamartClickhouse.write
