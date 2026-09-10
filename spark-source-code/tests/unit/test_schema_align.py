"""União de schemas divergentes entre clientes (`Helper.unificar_schemas`).

Clientes de um super tenant rodam versões diferentes do sistema embarcado: a mesma tabela tem
colunas a mais num cliente e a menos no outro. Duas invariantes governam o desenho:

1. Ninguém pode ser truncado. Se a união fosse "o schema do primeiro cliente", toda coluna
   exclusiva dos demais sumiria da Silver do super tenant — perda de dado silenciosa.
2. Tipo divergente FALHA. Castar em silêncio (bigint -> string) não quebra a escrita: quebra os
   JOINs do gold, que casam por igualdade, e o estrago aparece semanas depois como linha faltando
   num relatório.
"""
import pytest
from pyspark.sql.types import (
    LongType, NullType, StringType, StructField, StructType,
)

from repo.helper import Helper


def _schema(*campos):
    return StructType([StructField(nome, tipo, True) for nome, tipo in campos])


# --------------------------------------------------------------------------------------
# União
# --------------------------------------------------------------------------------------

def test_uniao_soma_colunas_de_clientes_com_schemas_diferentes():
    """A coluna exclusiva de cada cliente precisa sobreviver — é o ponto todo da união."""
    unificado = Helper.unificar_schemas([
        ("belafatia", _schema(("id", LongType()), ("lote", StringType()))),
        ("clienteN", _schema(("id", LongType()), ("turno", StringType()))),
    ])

    assert unificado.fieldNames() == ["id", "lote", "turno"]


def test_ordem_da_primeira_aparicao_e_preservada():
    """
    Determinismo entre execuções: o schema alvo alimenta o `ALTER TABLE ADD COLUMNS`, e uma ordem
    instável produziria diffs espúrios na tabela de destino.
    """
    unificado = Helper.unificar_schemas([
        ("a", _schema(("z", StringType()), ("y", StringType()))),
        ("b", _schema(("y", StringType()), ("x", StringType()))),
    ])

    assert unificado.fieldNames() == ["z", "y", "x"]


def test_uma_unica_origem_devolve_o_proprio_schema():
    origem = _schema(("id", LongType()), ("lote", StringType()))
    assert Helper.unificar_schemas([("belafatia", origem)]) == origem


# --------------------------------------------------------------------------------------
# Conflito de tipo
# --------------------------------------------------------------------------------------

def test_tipo_divergente_falha_nomeando_a_coluna_e_os_dois_tenants():
    with pytest.raises(ValueError) as excinfo:
        Helper.unificar_schemas([
            ("belafatia", _schema(("id", LongType()))),
            ("clienteN", _schema(("id", StringType()))),
        ])

    mensagem = str(excinfo.value)
    # A mensagem precisa dizer O QUE conflita e ONDE — sem isso o operador não sabe qual origem
    # corrigir.
    assert "'id'" in mensagem
    assert "belafatia" in mensagem and "clienteN" in mensagem
    assert "bigint" in mensagem and "string" in mensagem


def test_coluna_toda_nula_num_cliente_nao_sobrescreve_o_tipo_concreto():
    """
    Uma coluna 100% nula numa origem chega como NullType e não carrega informação de tipo. Se
    ditasse o schema, a coluna viraria NullType no destino e o dado do outro cliente não teria
    onde entrar.
    """
    unificado = Helper.unificar_schemas([
        ("belafatia", _schema(("lote", NullType()))),
        ("clienteN", _schema(("lote", StringType()))),
    ])

    assert unificado["lote"].dataType == StringType()


def test_tipo_concreto_nao_e_rebaixado_por_nulltype_posterior():
    """A ordem inversa do teste acima precisa dar o mesmo resultado."""
    unificado = Helper.unificar_schemas([
        ("belafatia", _schema(("lote", StringType()))),
        ("clienteN", _schema(("lote", NullType()))),
    ])

    assert unificado["lote"].dataType == StringType()


# --------------------------------------------------------------------------------------
# Colunas faltantes (alimenta o ALTER TABLE do destino)
# --------------------------------------------------------------------------------------

def test_colunas_faltantes_lista_o_que_o_destino_ainda_nao_tem():
    alvo = _schema(("id", LongType()), ("lote", StringType()), ("turno", StringType()))
    atual = _schema(("id", LongType()), ("lote", StringType()))

    assert [f.name for f in Helper.colunas_faltantes(alvo, atual)] == ["turno"]


def test_destino_em_dia_nao_gera_alteracao():
    alvo = _schema(("id", LongType()))
    assert Helper.colunas_faltantes(alvo, alvo) == []
