from datetime import datetime
from decimal import Decimal

import pytest

pytestmark = pytest.mark.integration

LOAD_TS = datetime(2026, 7, 16, 12, 30, 0)


def _schema():
    from pyspark.sql.types import (
        DecimalType, StringType, StructField, StructType, TimestampType,
    )

    return StructType([
        StructField("hk_business_id", StringType()),
        StructField("filial", StringType()),
        StructField("quantidade", DecimalType(12, 4)),
        StructField("load_dts", TimestampType()),
    ])


def _df(spark, rows):
    data = [(hk, filial, Decimal(quantidade), LOAD_TS) for hk, filial, quantidade in rows]
    return spark.createDataFrame(data, schema=_schema())


def test_primeira_carga_cria_alvo_com_unique_e_remove_staging(spark, make_repository, pg_query, pg_schema):
    repository = make_repository("fact_carga", ["filial"])
    repository.write(_df(spark, [("k1", "A", "1.5"), ("k2", "B", "2.0"), ("k3", "C", "3.0")]))

    assert pg_query(f'SELECT count(*) FROM "{pg_schema}"."fact_carga"')[0][0] == 3

    unique = pg_query(
        "SELECT 1 FROM information_schema.table_constraints "
        "WHERE table_name = 'fact_carga' AND constraint_type = 'UNIQUE'")
    assert unique, "alvo deveria ter constraint UNIQUE em hk_business_id"

    staging = pg_query(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'stg_fact_carga'")
    assert not staging, "staging deveria ser removida ao final do merge"


def test_reexecucao_com_mesmos_dados_e_idempotente(spark, make_repository, pg_query, pg_schema):
    repository = make_repository("fact_idem", ["filial"])
    df = _df(spark, [("k1", "A", "1.5"), ("k2", "B", "2.0")])

    repository.write(df)
    repository.write(df)

    assert pg_query(f'SELECT count(*) FROM "{pg_schema}"."fact_idem"')[0][0] == 2


def test_mesma_chave_com_valor_novo_atualiza_sem_duplicar(spark, make_repository, pg_query, pg_schema):
    repository = make_repository("fact_upd", ["filial"])
    repository.write(_df(spark, [("k1", "A", "1.5"), ("k2", "B", "2.0")]))
    repository.write(_df(spark, [("k1", "A", "9.9")]))

    assert pg_query(f'SELECT count(*) FROM "{pg_schema}"."fact_upd"')[0][0] == 2
    quantidade = pg_query(
        f'SELECT quantidade FROM "{pg_schema}"."fact_upd" WHERE hk_business_id = \'k1\'')[0][0]
    assert quantidade == Decimal("9.9")


def test_staging_orfa_com_schema_antigo_nao_quebra_a_execucao(spark, make_repository, pg_query, pg_schema):
    pg_query(f'CREATE TABLE "{pg_schema}"."stg_fact_orfa" ( "coluna_antiga" INT )')

    repository = make_repository("fact_orfa", ["filial"])
    repository.write(_df(spark, [("k1", "A", "1.5")]))

    assert pg_query(f'SELECT count(*) FROM "{pg_schema}"."fact_orfa"')[0][0] == 1


def test_tipos_preservados_no_destino(spark, make_repository, pg_query, pg_schema):
    repository = make_repository("fact_tipos", ["filial"])
    repository.write(_df(spark, [("k1", "A", "1234.5678")]))

    numeric = pg_query(
        "SELECT numeric_precision, numeric_scale FROM information_schema.columns "
        "WHERE table_name = 'fact_tipos' AND column_name = 'quantidade'")[0]
    assert numeric == (12, 4)

    tipo_ts = pg_query(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'fact_tipos' AND column_name = 'load_dts'")[0][0]
    assert tipo_ts == "timestamp without time zone"

    quantidade, load_dts = pg_query(
        f'SELECT quantidade, load_dts FROM "{pg_schema}"."fact_tipos"')[0]
    assert quantidade == Decimal("1234.5678")
    assert load_dts == LOAD_TS


def test_falha_no_merge_mantem_o_alvo_intacto(spark, make_repository, pg_query, pg_schema):
    # Alvo pré-existente sem a coluna "quantidade": o merge falha e a transação
    # não pode ter tocado nos dados já presentes.
    pg_query(
        f'CREATE TABLE "{pg_schema}"."fact_atomico" ('
        ' "hk_business_id" TEXT, "filial" TEXT, "load_dts" TIMESTAMP,'
        ' UNIQUE ("hk_business_id") )')
    pg_query(
        f'INSERT INTO "{pg_schema}"."fact_atomico" VALUES (\'seed\', \'Z\', now())')

    repository = make_repository("fact_atomico", ["filial"])
    with pytest.raises(Exception):
        repository.write(_df(spark, [("k1", "A", "1.5")]))

    rows = pg_query(f'SELECT hk_business_id, filial FROM "{pg_schema}"."fact_atomico"')
    assert rows == [("seed", "Z")]
