import pytest

from utils.queryutils import QueryUtils


class FakeConfig:
    def __init__(self, data):
        self._data = data

    def get(self, key, default=None):
        return self._data.get(key, default)


@pytest.fixture
def config():
    return FakeConfig({
        "minio.base_path": "s3a://bucket",
        "trino.schema_folder_silver": "silver_folder",
        "trino.schema_folder_gold": "gold_folder",
    })


def _write_sql(tmp_path, table_name, content):
    (tmp_path / f"{table_name}.sql").write_text(content, encoding="utf-8")


def test_substitui_todos_os_placeholders(tmp_path, config):
    _write_sql(tmp_path, "fact", (
        "SELECT * FROM delta.`MINIO_BASE_PATH/S3_PATH_SILVER/dw_a` a "
        "JOIN delta.`MINIO_BASE_PATH/S3_PATH_GOLD/fact_b` b ON a.id = b.id"
    ))

    query = QueryUtils.build_query(str(tmp_path), config, "fact")

    assert query == (
        "SELECT * FROM delta.`s3a://bucket/silver_folder/dw_a` a "
        "JOIN delta.`s3a://bucket/gold_folder/fact_b` b ON a.id = b.id"
    )


def test_substitui_multiplas_ocorrencias_do_mesmo_placeholder(tmp_path, config):
    _write_sql(tmp_path, "fact", "S3_PATH_SILVER S3_PATH_SILVER S3_PATH_SILVER")
    query = QueryUtils.build_query(str(tmp_path), config, "fact")
    assert query == "silver_folder silver_folder silver_folder"


def test_query_sem_placeholders_passa_intacta(tmp_path, config):
    _write_sql(tmp_path, "dim", "SELECT 1 AS um")
    assert QueryUtils.build_query(str(tmp_path), config, "dim") == "SELECT 1 AS um"


def test_arquivo_inexistente_erra_com_tabela_e_caminho(tmp_path, config):
    with pytest.raises(ValueError, match="fact_x") as exc:
        QueryUtils.build_query(str(tmp_path), config, "fact_x")
    assert str(tmp_path) in str(exc.value)


def test_config_ausente_erra_nomeando_a_chave(tmp_path):
    _write_sql(tmp_path, "fact", "SELECT * FROM delta.`MINIO_BASE_PATH/S3_PATH_GOLD/t`")
    config = FakeConfig({
        "minio.base_path": "s3a://bucket",
        "trino.schema_folder_silver": "silver_folder",
    })

    with pytest.raises(ValueError, match="S3_PATH_GOLD"):
        QueryUtils.build_query(str(tmp_path), config, "fact")


class FakeRuntime:
    def __init__(self, data):
        self._data = data

    def get(self, key, default=None):
        return self._data.get(key, default)


def test_substitui_a_janela_vinda_do_runtime(tmp_path, config):
    _write_sql(tmp_path, "fact", "WHERE t >= to_timestamp('JANELA_INICIO') AND t < to_timestamp('JANELA_FIM')")
    runtime = FakeRuntime({"janela_inicio": "2026-09-01 00:00:00", "janela_fim": "2026-10-01 00:00:00"})

    query = QueryUtils.build_query(str(tmp_path), config, "fact", runtime)

    assert query == (
        "WHERE t >= to_timestamp('2026-09-01 00:00:00') "
        "AND t < to_timestamp('2026-10-01 00:00:00')"
    )


def test_query_que_exige_janela_erra_sem_runtime(tmp_path, config):
    _write_sql(tmp_path, "fact", "WHERE t >= to_timestamp('JANELA_INICIO')")

    with pytest.raises(ValueError, match="JANELA_INICIO"):
        QueryUtils.build_query(str(tmp_path), config, "fact")


def test_query_que_exige_janela_erra_com_runtime_incompleto(tmp_path, config):
    _write_sql(tmp_path, "fact", "WHERE t >= to_timestamp('JANELA_INICIO') AND t < to_timestamp('JANELA_FIM')")
    runtime = FakeRuntime({"janela_inicio": "2026-09-01 00:00:00"})

    with pytest.raises(ValueError, match="JANELA_FIM"):
        QueryUtils.build_query(str(tmp_path), config, "fact", runtime)


def test_query_sem_janela_ignora_o_runtime(tmp_path, config):
    _write_sql(tmp_path, "dim", "SELECT 1 AS um")
    runtime = FakeRuntime({"janela_inicio": "2026-09-01 00:00:00", "janela_fim": "2026-10-01 00:00:00"})
    assert QueryUtils.build_query(str(tmp_path), config, "dim", runtime) == "SELECT 1 AS um"
