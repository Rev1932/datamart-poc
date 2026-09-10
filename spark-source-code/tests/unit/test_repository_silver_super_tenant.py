"""Consolidação da Silver de N clientes num super tenant (`RepositorySilverSuperTenant`).

O risco central deste pipeline não é escrever errado — é terminar VERDE sem ter consolidado
nada. Ele lê buckets de OUTROS tenants, e a falta de permissão S3 num deles se parece muito com
"esse cliente não tem essa tabela" (estado legítimo, que é pulado de propósito). Os testes abaixo
ancoram a fronteira entre os dois.
"""
import types

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from repo.repository import RepositorySilverSuperTenant


BELAFATIA = {"tenant": "belafatia", "base_path": "s3a://datawake-belafatia"}
CLIENTE_N = {"tenant": "clienteN", "base_path": "s3a://datawake-clienten"}

SILVER_DE_ORIGEM = StructType([
    StructField("id", LongType(), True),
    StructField("hk_business_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("load_dts", StringType(), True),
    StructField("unidade_origem", StringType(), True),
    StructField("dataset_origem", StringType(), True),
])


class FakeReader:
    """Duplo de `spark.read`: devolve o schema registrado para cada path."""

    def __init__(self, schemas_por_path, registro):
        self._schemas = schemas_por_path
        self._registro = registro
        self._formato = None

    def format(self, fmt):
        self._formato = fmt
        return self

    def load(self, path):
        self._registro.append(("load", path))
        return types.SimpleNamespace(schema=self._schemas.get(path, SILVER_DE_ORIGEM))


class FakeSpark:
    def __init__(self, schemas_por_path=None):
        self.registro = []
        self.sql_executado = []
        self.read = FakeReader(schemas_por_path or {}, self.registro)

    def sql(self, comando):
        self.sql_executado.append(comando)


@pytest.fixture
def repositorio(monkeypatch):
    """Constrói o repositório sem tocar em MinIO, Delta ou Spark reais."""
    def _build(source_tenants, paths_delta=(), erro_is_delta=None, schemas_por_path=None):
        paths_delta = set(paths_delta)
        chamadas_is_delta = []

        def _is_delta(spark, path):
            chamadas_is_delta.append(path)
            if erro_is_delta and path in erro_is_delta:
                raise erro_is_delta[path]
            return path in paths_delta

        monkeypatch.setattr(
            "repo.repository.DeltaTable",
            types.SimpleNamespace(isDeltaTable=_is_delta),
        )

        spark = FakeSpark(schemas_por_path)
        repo = RepositorySilverSuperTenant(
            spark,
            {
                "minio.base_path": "s3a://datawake-lakatos",
                "trino.schema_folder_silver": "business_datavault_data-bee",
            },
            {"table_name": "dw_ordem_producao", "source_tenants": source_tenants},
        )
        return repo, spark, chamadas_is_delta

    return _build


# --------------------------------------------------------------------------------------
# Convenção de path — precisa bater com o delta_maintenance
# --------------------------------------------------------------------------------------

def test_output_path_usa_a_mesma_convencao_do_delta_maintenance(repositorio):
    """
    Se este repositório derivasse o path por conta própria (como o RepositoryBronzeToSilver, que
    hardcoda a pasta), OPTIMIZE/VACUUM poderiam apontar para um path diferente do que foi escrito
    e a manutenção rodaria sobre uma tabela que não existe.
    """
    from core.pipeline_orchestrator import resolve_delta_path

    repo, _, _ = repositorio([BELAFATIA])
    esperado = resolve_delta_path(
        {"minio.base_path": "s3a://datawake-lakatos",
         "trino.schema_folder_silver": "business_datavault_data-bee"},
        "dw_ordem_producao",
        "silver",
    )
    assert repo.output_path == esperado


def test_caminho_de_origem_segue_a_convencao_global_da_camada_silver(repositorio):
    """A pasta silver é convenção global: todo bronze_silver escreve nela, em qualquer bucket."""
    repo, _, _ = repositorio([BELAFATIA])
    assert repo.caminho_origem(BELAFATIA["base_path"]) == (
        "s3a://datawake-belafatia/business_datavault_data-bee/dw_ordem_producao"
    )


def test_repositorio_nao_instancia_cliente_minio(repositorio):
    """
    O super tenant NÃO é dono dos parquets de origem: eles pertencem ao cliente e já foram
    consumidos pelo bronze_silver dele. Um `cleaning_processed` aqui apagaria dado de um bucket
    que não é nosso. A ausência do cliente MinIO é a guarda que torna isso impossível.
    """
    repo, _, _ = repositorio([BELAFATIA])
    assert not hasattr(repo, "minio_utils")


def test_source_tenants_e_copiado_e_nao_referenciado(repositorio):
    """
    A lista vem da config BASE, compartilhada entre as threads de tabela do TableRunner
    (`dataclasses.replace` copia a REFERÊNCIA). Cada repositório precisa da sua cópia.
    """
    origens = [BELAFATIA]
    repo, _, _ = repositorio(origens)
    assert repo.source_tenants == origens
    assert repo.source_tenants is not origens


# --------------------------------------------------------------------------------------
# Origem ausente x origem inacessível — o cerne
# --------------------------------------------------------------------------------------

def test_origem_sem_a_tabela_e_pulada_sem_falhar_a_consolidacao(repositorio):
    """Nem todo cliente tem toda tabela: versões diferentes do sistema embarcado."""
    repo, _, _ = repositorio(
        [BELAFATIA, CLIENTE_N],
        paths_delta=["s3a://datawake-belafatia/business_datavault_data-bee/dw_ordem_producao"],
    )

    disponiveis = repo.origens_disponiveis()

    assert [o["tenant"] for o in disponiveis] == ["belafatia"]


def test_todas_as_origens_ausentes_falha_em_vez_de_reportar_sucesso(repositorio):
    """
    O teste mais importante do conjunto. "Nenhum cliente tem esta tabela" é indistinguível de
    "todos os base_path estão errados" e de "a policy do S3 não libera nenhum bucket". Seguir
    verde seria a mesma regressão que `test_repository_read.py` ancora para o bronze_silver.
    """
    repo, _, _ = repositorio([BELAFATIA, CLIENTE_N], paths_delta=[])

    with pytest.raises(ValueError, match="dw_ordem_producao"):
        repo.origens_disponiveis()


def test_erro_ao_inspecionar_origem_propaga_em_vez_de_virar_origem_ausente(repositorio):
    """
    403 do MinIO (policy sem leitura no bucket do cliente) tratado como ausência deixaria a
    tabela VERDE sem ter consolidado nada — exatamente o modo de falha que a leitura cross-bucket
    torna provável.
    """
    path = "s3a://datawake-belafatia/business_datavault_data-bee/dw_ordem_producao"
    repo, _, _ = repositorio(
        [BELAFATIA],
        erro_is_delta={path: PermissionError("403 Access Denied")},
    )

    with pytest.raises(PermissionError, match="403"):
        repo.origens_disponiveis()


# --------------------------------------------------------------------------------------
# Schema alvo
# --------------------------------------------------------------------------------------

def test_schema_alvo_injeta_as_colunas_que_a_reidentificacao_escreve(repositorio):
    """
    `unidade_origem`, `filial_origem` e `tenant_origem` são ESCRITAS pela reidentificação. Sem
    elas no schema alvo o DataFrame teria colunas a mais que o destino e o
    `whenNotMatchedInsertAll` rejeitaria a escrita.
    """
    path = "s3a://datawake-belafatia/business_datavault_data-bee/dw_ordem_producao"
    sem_unidade = StructType([
        StructField("id", LongType(), True),
        StructField("hk_business_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("load_dts", StringType(), True),
    ])
    repo, _, _ = repositorio(
        [BELAFATIA], paths_delta=[path], schemas_por_path={path: sem_unidade}
    )

    alvo = repo.resolver_schema_alvo(repo.origens_disponiveis())

    assert {"unidade_origem", "filial_origem", "tenant_origem"} <= set(alvo.fieldNames())


def test_schema_alvo_falha_quando_a_origem_nao_e_uma_silver(repositorio):
    """Sem `hk_business_id` não há o que re-hashear: a origem não veio do bronze_silver."""
    path = "s3a://datawake-belafatia/business_datavault_data-bee/dw_ordem_producao"
    repo, _, _ = repositorio(
        [BELAFATIA],
        paths_delta=[path],
        schemas_por_path={path: StructType([StructField("id", LongType(), True)])},
    )

    with pytest.raises(ValueError, match="hk_business_id"):
        repo.resolver_schema_alvo(repo.origens_disponiveis())


def test_destino_entra_primeiro_na_uniao_de_schemas(repositorio):
    """
    O destino fixa a ordem das colunas já existentes e impede que uma coluna trazida por um
    cliente REMOVIDO de --source_tenants desapareça do schema — o dado dele continua na tabela,
    porque o merge nunca deleta.
    """
    destino = "s3a://datawake-lakatos/business_datavault_data-bee/dw_ordem_producao"
    origem = "s3a://datawake-belafatia/business_datavault_data-bee/dw_ordem_producao"
    schema_destino = StructType(
        SILVER_DE_ORIGEM.fields + [StructField("coluna_de_cliente_removido", StringType(), True)]
    )
    repo, _, _ = repositorio(
        [BELAFATIA],
        paths_delta=[destino, origem],
        schemas_por_path={destino: schema_destino, origem: SILVER_DE_ORIGEM},
    )

    alvo = repo.resolver_schema_alvo(repo.origens_disponiveis())

    assert "coluna_de_cliente_removido" in alvo.fieldNames()


def test_coluna_nova_da_origem_gera_alter_table_e_nao_conf_de_sessao(repositorio):
    """
    `ALTER TABLE ADD COLUMNS` explícito em vez de `delta.schema.autoMerge.enabled`: aquela conf
    é da SparkSession, COMPARTILHADA pelas N threads de tabela — ligá-la aqui mudaria o
    comportamento de escrita das tabelas que as outras threads estão processando.
    """
    destino = "s3a://datawake-lakatos/business_datavault_data-bee/dw_ordem_producao"
    repo, spark, _ = repositorio([BELAFATIA], paths_delta=[destino])

    alvo = StructType(SILVER_DE_ORIGEM.fields + [StructField("turno", StringType(), True)])
    repo.evoluir_schema_destino(alvo)

    assert len(spark.sql_executado) == 1
    assert "ADD COLUMNS" in spark.sql_executado[0]
    assert "`turno` string" in spark.sql_executado[0]


def test_destino_inexistente_nao_gera_alter_table(repositorio):
    """Na primeira carga o schema nasce do write; um ALTER aqui falharia."""
    repo, spark, _ = repositorio([BELAFATIA], paths_delta=[])

    repo.evoluir_schema_destino(SILVER_DE_ORIGEM)

    assert spark.sql_executado == []
