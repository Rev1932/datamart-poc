"""Consolidação ponta a ponta do super tenant, com Spark e Delta reais.

Os testes unitários provam a ORQUESTRAÇÃO (quem é pulado, quem falha, em que ordem). O que só
Delta real prova é o resultado: que a Silver do super tenant sai com a mesma FORMA de qualquer
outra Silver — partição por `source`, colunas de negócio intactas, `hk_business_id` sem colisão —
e que rodar duas vezes não duplica nada.

Rode com: pytest -m integration tests/integration/test_silver_super_tenant.py
"""
import pytest

pytestmark = pytest.mark.integration

TABELA = "dw_ordem_producao"
SILVER_FOLDER = "business_datavault_data-bee"


@pytest.fixture(scope="module")
def spark_delta():
    """
    Sessão própria com as extensões do Delta.

    O fixture `spark` compartilhado (tests/conftest.py) não configura Delta — os testes que o
    usam trabalham com temp views e JDBC. Mesmo padrão do `spark_fair` em
    test_table_runner_concorrencia.py: quem precisa de conf específica traz a sua sessão.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName("honeycomb-tests-super-tenant")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _escreve_silver_de_origem(spark, base_path, linhas, colunas):
    """Materializa uma Silver de origem como o `bronze_silver` a deixaria."""
    df = spark.createDataFrame(linhas, colunas)
    (df.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .partitionBy("source")
        .mode("overwrite")
        .save(f"{base_path}/{SILVER_FOLDER}/{TABELA}"))


@pytest.fixture
def cenario(spark_delta, tmp_path_factory):
    """
    Dois clientes com schemas divergentes e uma FILIAL HOMÔNIMA.

    A filial `matriz` existindo nos dois clientes é o cenário que quebraria o desenho ingênuo:
    o `hk_business_id` da origem é `sha2(pk + 'data-bee_matriz')` nos dois, então sem o re-hash
    as duas linhas colidiriam e o dedup do write descartaria uma — perda silenciosa.
    """
    raiz = tmp_path_factory.mktemp("super_tenant")

    colunas_belafatia = (
        "id long, hk_business_id string, source string, load_dts timestamp, "
        "unidade_origem string, dataset_origem string, lote string"
    )
    colunas_clienten = (
        "id long, hk_business_id string, source string, load_dts timestamp, "
        "unidade_origem string, dataset_origem string, turno string"
    )

    import datetime
    agora = datetime.datetime(2026, 8, 4, 12, 0, 0)

    _escreve_silver_de_origem(
        spark_delta, f"{raiz}/belafatia",
        [
            (1, "hash_matriz_1", "data-bee_matriz", agora, "matriz", "producao", "L1"),
            (2, "hash_loja2_2", "data-bee_loja2", agora, "loja2", "producao", "L2"),
        ],
        colunas_belafatia,
    )
    _escreve_silver_de_origem(
        spark_delta, f"{raiz}/clienteN",
        [
            # MESMO hk da linha 1 de belafatia: filial homônima, mesma PK.
            (1, "hash_matriz_1", "data-bee_matriz", agora, "matriz", "producao", "T1"),
        ],
        colunas_clienten,
    )

    return {
        "raiz": str(raiz),
        "env": {
            "minio.base_path": f"{raiz}/lakatos",
            "trino.schema_folder_silver": SILVER_FOLDER,
        },
        "origens": [
            {"tenant": "belafatia", "base_path": f"{raiz}/belafatia"},
            {"tenant": "clienteN", "base_path": f"{raiz}/clienteN"},
        ],
        "destino": f"{raiz}/lakatos/{SILVER_FOLDER}/{TABELA}",
    }


def _consolida(spark, cenario):
    from core.pipeline_orchestrator import PipelineSilverSuperTenant

    pipeline = PipelineSilverSuperTenant(
        spark,
        cenario["env"],
        {"table_name": TABELA, "source_tenants": cenario["origens"]},
    )
    pipeline.run()


@pytest.fixture
def destino(spark_delta, cenario):
    _consolida(spark_delta, cenario)
    return spark_delta.read.format("delta").load(cenario["destino"])


# --------------------------------------------------------------------------------------
# Reidentificação
# --------------------------------------------------------------------------------------

def test_cada_cliente_vira_uma_particao_source_do_super_tenant(destino):
    fontes = {r["source"] for r in destino.select("source").distinct().collect()}
    assert fontes == {"data-bee_belafatia", "data-bee_clienteN"}


def test_unidade_origem_passa_a_ser_o_nome_do_cliente(destino):
    unidades = {r["unidade_origem"] for r in destino.select("unidade_origem").distinct().collect()}
    assert unidades == {"belafatia", "clienteN"}


def test_dataset_origem_e_preservado(destino):
    """É o `banco` dos JOINs do gold — o par (unidade_origem, dataset_origem) discrimina."""
    datasets = {r["dataset_origem"] for r in destino.select("dataset_origem").distinct().collect()}
    assert datasets == {"producao"}


def test_filial_de_origem_e_preservada_em_coluna_propria(destino):
    """
    O overwrite de `unidade_origem` apaga o discriminador entre filiais de um MESMO cliente.
    `filial_origem` é a única saída para reconstruir essa granularidade sem reprocessar tudo.
    """
    linhas = destino.select("tenant_origem", "filial_origem").collect()
    assert sorted((r["tenant_origem"], r["filial_origem"]) for r in linhas) == [
        ("belafatia", "loja2"),
        ("belafatia", "matriz"),
        ("clienteN", "matriz"),
    ]


# --------------------------------------------------------------------------------------
# Hash — a razão de existir do re-hash
# --------------------------------------------------------------------------------------

def test_filiais_homonimas_de_clientes_distintos_nao_colidem(destino):
    """
    Sem o re-hash as duas linhas `matriz` teriam o MESMO hk_business_id e o dedup por
    `row_number() over (partitionBy hk_business_id)` descartaria uma — perda silenciosa.
    """
    hashes = [r["hk_business_id"] for r in destino.select("hk_business_id").collect()]
    assert len(hashes) == len(set(hashes)) == 3


def test_filiais_do_mesmo_cliente_seguem_como_linhas_distintas(destino):
    """
    O re-hash usa o hk ORIGINAL, que já distingue as filiais da origem. Recalcular do zero a
    partir da PK colapsaria matriz e loja2 de belafatia numa linha só.
    """
    belafatia = destino.where("source = 'data-bee_belafatia'")
    assert belafatia.count() == 2


# --------------------------------------------------------------------------------------
# Schema divergente entre clientes
# --------------------------------------------------------------------------------------

def test_coluna_exclusiva_de_um_cliente_sobrevive_e_fica_nula_no_outro(destino):
    colunas = set(destino.columns)
    assert {"lote", "turno"} <= colunas

    lotes = {r["source"]: r["lote"] for r in destino.select("source", "lote").collect()}
    assert lotes["data-bee_clienteN"] is None

    turnos = {r["source"]: r["turno"] for r in destino.select("source", "turno").collect()}
    assert turnos["data-bee_clienteN"] == "T1"


def test_destino_nasce_particionado_e_com_cdf(spark_delta, cenario, destino):
    """Paridade com o bronze_silver: é o que torna a Silver indistinguível para o gold e o
    delta_maintenance."""
    from delta.tables import DeltaTable

    detalhe = DeltaTable.forPath(spark_delta, cenario["destino"]).detail().collect()[0]
    assert detalhe["partitionColumns"] == ["source"]
    assert detalhe["properties"].get("delta.enableChangeDataFeed") == "true"


# --------------------------------------------------------------------------------------
# Idempotência
# --------------------------------------------------------------------------------------

def test_segunda_execucao_nao_duplica_linhas(spark_delta, cenario, destino):
    antes = destino.count()

    _consolida(spark_delta, cenario)

    depois = spark_delta.read.format("delta").load(cenario["destino"]).count()
    assert depois == antes == 3


def test_correcao_na_origem_chega_ao_super_tenant(spark_delta, cenario, destino):
    """
    O merge é insert+update (o bronze_silver é insert-only). A origem aqui é uma Silver que pode
    ter sido CORRIGIDA depois; sem o update a correção nunca chegaria e o super tenant divergiria
    do cliente para sempre.
    """
    import datetime
    from delta.tables import DeltaTable

    origem = f"{cenario['raiz']}/belafatia/{SILVER_FOLDER}/{TABELA}"
    (DeltaTable.forPath(spark_delta, origem).update(
        condition="id = 1",
        set={"lote": "'L1-CORRIGIDO'",
             "load_dts": f"timestamp('{datetime.datetime(2026, 8, 5, 12, 0, 0)}')"},
    ))

    _consolida(spark_delta, cenario)

    corrigida = (spark_delta.read.format("delta").load(cenario["destino"])
                 .where("filial_origem = 'matriz' AND tenant_origem = 'belafatia'")
                 .collect())
    assert len(corrigida) == 1
    assert corrigida[0]["lote"] == "L1-CORRIGIDO"
