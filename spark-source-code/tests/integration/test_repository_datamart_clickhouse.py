"""Troca de partição no ClickHouse: sequência completa e recarga da mesma janela.

Exige um ClickHouse acessível, informado por DATAMART_CH_*. Sem ele, a suíte é pulada.
"""
import os
import uuid

import pytest

pytestmark = pytest.mark.integration

_OBRIGATORIAS = ("DATAMART_CH_URL", "DATAMART_CH_USER", "DATAMART_CH_PASSWORD", "DATAMART_CH_DATABASE")


def _config_ch():
    faltando = [v for v in _OBRIGATORIAS if not os.environ.get(v)]
    if faltando:
        pytest.skip(f"ClickHouse não configurado: {', '.join(faltando)}")
    return {
        "url": os.environ["DATAMART_CH_URL"],
        "user": os.environ["DATAMART_CH_USER"],
        "password": os.environ["DATAMART_CH_PASSWORD"],
        "database": os.environ["DATAMART_CH_DATABASE"],
        "catalog": os.environ.get("DATAMART_CH_CATALOG", "clickhouse"),
    }


class FakeEnvironment:
    def __init__(self, data):
        self._data = data

    def get(self, key, default=None):
        return self._data.get(key, default)


@pytest.fixture(scope="module")
def config_ch():
    return _config_ch()


@pytest.fixture(scope="module")
def cliente(config_ch):
    from utils.clickhouse_datamart import ClickHouseDatamartClient

    return ClickHouseDatamartClient(config_ch)


@pytest.fixture(scope="module")
def spark_ch(config_ch):
    from pyspark.sql import SparkSession

    catalogo = config_ch["catalog"]
    sessao = (
        SparkSession.builder
        .appName("teste-datamart-clickhouse")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config(f"spark.sql.catalog.{catalogo}", "com.clickhouse.spark.ClickHouseCatalog")
        .config(f"spark.sql.catalog.{catalogo}.host", config_ch["url"].split("//")[1].split(":")[0])
        .config(f"spark.sql.catalog.{catalogo}.protocol", "http")
        .config(f"spark.sql.catalog.{catalogo}.http_port", config_ch["url"].rsplit(":", 1)[1])
        .config(f"spark.sql.catalog.{catalogo}.user", config_ch["user"])
        .config(f"spark.sql.catalog.{catalogo}.password", config_ch["password"])
        .config(f"spark.sql.catalog.{catalogo}.database", config_ch["database"])
        .config("spark.clickhouse.write.compression.codec", "none")
        .getOrCreate()
    )
    yield sessao
    sessao.stop()


@pytest.fixture
def tabela(cliente, config_ch):
    """Fato efêmera com a mesma engine/partição da DDL de produção."""
    nome = f"fact_teste_{uuid.uuid4().hex[:8]}"
    cliente.executar(f"""
        CREATE TABLE {config_ch['database']}.{nome} (
            hk_business_id String,
            load_dts DateTime64(3),
            filial LowCardinality(String),
            banco LowCardinality(String),
            unidade_producao_id Int64,
            timestamp DateTime64(3),
            valor Decimal(9,3)
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (filial, banco, unidade_producao_id, timestamp)
    """)
    yield nome
    cliente.descartar(nome)


def _repositorio(spark_ch, config_ch, tabela, tmp_path, janela_inicio, janela_fim="2026-10-01 00:00:00"):
    from repo.repository import RepositoryDatamartClickhouse
    from utils.pipeline_config import PipelineConfig

    (tmp_path / f"{tabela}.sql").write_text("SELECT 1 AS um", encoding="utf-8")
    environment = FakeEnvironment({
        "datamart_clickhouse": config_ch,
        "minio.base_path": "s3a://bucket-teste",
        "trino.schema_folder_silver": "silver",
        "trino.schema_folder_gold": "gold",
    })
    runtime = PipelineConfig(
        pipeline="datamart_ch",
        tenant_name="teste",
        filial_name="teste",
        table_name=tabela,
        primary_key=["filial", "banco", "unidade_producao_id"],
        janela_inicio=janela_inicio,
        janela_fim=janela_fim,
    )
    return RepositoryDatamartClickhouse(spark_ch, environment, runtime, str(tmp_path))


def _dados(spark_ch, linhas):
    from pyspark.sql import functions as F

    return (spark_ch.createDataFrame(linhas, "filial string, banco string, unidade_producao_id long, ts string, valor double")
            .withColumn("timestamp", F.to_timestamp("ts"))
            .drop("ts")
            .withColumn("valor", F.col("valor").cast("decimal(9,3)"))
            .withColumn("hk_business_id", F.sha2(F.concat_ws("_", "filial", "banco", "unidade_producao_id"), 256))
            .withColumn("load_dts", F.current_timestamp()))


def test_troca_de_particao_grava_e_limpa_a_staging(spark_ch, config_ch, cliente, tabela, tmp_path):
    repo = _repositorio(spark_ch, config_ch, tabela, tmp_path, "2026-09-01 00:00:00")
    dados = _dados(spark_ch, [
        ("LIMEIRA", "dw_limeira", 1, "2026-09-05 10:00:00", 10.5),
        ("LIMEIRA", "dw_limeira", 2, "2026-09-20 10:00:00", 20.5),
    ])

    repo.write(dados)

    assert cliente.contar(tabela) == 2
    stagings = cliente.executar(
        f"SELECT count() FROM system.tables WHERE database='{config_ch['database']}' AND name LIKE '{tabela}_stg_%'")
    assert stagings == "0", "staging não foi descartada"


def test_recarregar_a_mesma_janela_nao_muda_a_contagem(spark_ch, config_ch, cliente, tabela, tmp_path):
    repo = _repositorio(spark_ch, config_ch, tabela, tmp_path, "2026-09-01 00:00:00")
    dados = _dados(spark_ch, [
        ("LIMEIRA", "dw_limeira", 1, "2026-09-05 10:00:00", 10.5),
        ("LIMEIRA", "dw_limeira", 2, "2026-09-20 10:00:00", 20.5),
    ])

    repo.write(dados)
    primeira = cliente.contar(tabela)
    repo.write(dados)
    segunda = cliente.contar(tabela)

    assert primeira == segunda == 2


def test_janela_fora_da_fronteira_do_mes_e_recusada(spark_ch, config_ch, tabela, tmp_path):
    repo = _repositorio(spark_ch, config_ch, tabela, tmp_path, "2026-09-10 00:00:00")
    dados = _dados(spark_ch, [("LIMEIRA", "dw_limeira", 1, "2026-09-15 10:00:00", 10.5)])

    with pytest.raises(ValueError, match="fronteira do mes"):
        repo.write(dados)


def test_sem_janela_a_troca_e_recusada(spark_ch, config_ch, tabela, tmp_path):
    repo = _repositorio(spark_ch, config_ch, tabela, tmp_path, None, None)
    dados = _dados(spark_ch, [("LIMEIRA", "dw_limeira", 1, "2026-09-15 10:00:00", 10.5)])

    with pytest.raises(ValueError, match="janela_inicio ausente"):
        repo.write(dados)


def test_dado_anterior_a_janela_e_recusado(spark_ch, config_ch, tabela, tmp_path):
    repo = _repositorio(spark_ch, config_ch, tabela, tmp_path, "2026-09-01 00:00:00")
    dados = _dados(spark_ch, [
        ("LIMEIRA", "dw_limeira", 1, "2026-09-05 10:00:00", 10.5),
        ("LIMEIRA", "dw_limeira", 2, "2026-08-20 10:00:00", 20.5),
    ])

    with pytest.raises(ValueError, match="anterior"):
        repo.write(dados)
