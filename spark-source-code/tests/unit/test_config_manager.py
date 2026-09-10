import pytest

from utils.config_manager import ConfigManager


@pytest.fixture(autouse=True)
def limpa_env(monkeypatch):
    """Isola cada teste: nenhuma env var do schema vaza entre os casos."""
    from utils.config_manager import _ENV_SCHEMA

    for env_name, _, _ in _ENV_SCHEMA:
        monkeypatch.delenv(env_name, raising=False)


def test_get_de_chave_simples(monkeypatch, tmp_path):
    monkeypatch.setenv("MINIO_BASE_PATH", "s3a://datawake-bucket")
    manager = ConfigManager(config_base_path=str(tmp_path))
    assert manager.get("minio.base_path") == "s3a://datawake-bucket"


def test_postgres_retorna_no_aninhado_com_contrato_completo(monkeypatch, tmp_path):
    monkeypatch.setenv("POSTGRES_HOST", "dw-postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DATABASE", "dw")
    monkeypatch.setenv("POSTGRES_USER", "etl")
    monkeypatch.setenv("POSTGRES_PASSWORD", "segredo")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")

    manager = ConfigManager(config_base_path=str(tmp_path))
    postgres = manager.get("postgres")

    # Subscrição, .get com default e .items() — como os consumidores usam.
    assert postgres["host"] == "dw-postgres"
    assert postgres.get("schema", "outro") == "public"
    assert dict(postgres.items())["user"] == "etl"


def test_port_coagido_para_int(monkeypatch, tmp_path):
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("TRINO_PORT", "48085")
    manager = ConfigManager(config_base_path=str(tmp_path))
    assert manager.get("postgres.port") == 5432
    assert isinstance(manager.get("postgres.port"), int)
    assert isinstance(manager.get("trino.port"), int)


def test_verify_ssl_false_vira_bool_false(monkeypatch, tmp_path):
    """String "false" é truthy; sem coerção desligaria a verificação TLS errada."""
    monkeypatch.setenv("TRINO_VERIFY_SSL", "false")
    manager = ConfigManager(config_base_path=str(tmp_path))
    assert manager.get("trino.verify_ssl") is False


def test_verify_ssl_true_vira_bool_true(monkeypatch, tmp_path):
    monkeypatch.setenv("TRINO_VERIFY_SSL", "true")
    manager = ConfigManager(config_base_path=str(tmp_path))
    assert manager.get("trino.verify_ssl") is True


def test_clickhouse_expoe_contrato_do_handler(monkeypatch, tmp_path):
    monkeypatch.setenv("CLICKHOUSE_URL", "https://api-logs.datawake.cloud")
    monkeypatch.setenv("CLICKHOUSE_USERNAME", "logger")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "segredo")
    monkeypatch.setenv("CLICKHOUSE_SERVICE_NAME", "spark-into-postgres")

    manager = ConfigManager(config_base_path=str(tmp_path))

    # É exatamente o que ClickHouseAsyncHandler.__init__ consome.
    assert manager.get("clickhouse.url").rstrip("/") == "https://api-logs.datawake.cloud"
    assert manager.get("clickhouse.username") == "logger"
    assert manager.get("clickhouse.service_name") == "spark-into-postgres"


def test_clickhouse_verify_ssl_bool_e_certificado(monkeypatch, tmp_path):
    """verify_ssl é dual: 'false' vira bool; nome de arquivo fica str (cert)."""
    monkeypatch.setenv("CLICKHOUSE_VERIFY_SSL", "false")
    assert ConfigManager(config_base_path=str(tmp_path)).get("clickhouse.verify_ssl") is False

    monkeypatch.setenv("CLICKHOUSE_VERIFY_SSL", "wildcard_datadriven_cloud.crt")
    assert ConfigManager(config_base_path=str(tmp_path)).get("clickhouse.verify_ssl") == "wildcard_datadriven_cloud.crt"


def test_spark_s3_retorna_no_aninhado(monkeypatch, tmp_path):
    monkeypatch.setenv("SPARK_S3_ENDPOINT", "https://minio:9000")
    monkeypatch.setenv("SPARK_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("SPARK_S3_SECRET_KEY", "sk")

    manager = ConfigManager(config_base_path=str(tmp_path))
    s3 = manager.get("spark.s3", {})

    assert s3.get("endpoint") == "https://minio:9000"
    assert s3.get("access_key") == "ak"
    assert dict(s3.items())["secret_key"] == "sk"


def test_spark_conf_ausente_retorna_default_iteravel(monkeypatch, tmp_path):
    manager = ConfigManager(config_base_path=str(tmp_path))
    assert list(manager.get("spark.conf", {}).items()) == []


def test_env_var_ausente_retorna_default(monkeypatch, tmp_path):
    manager = ConfigManager(config_base_path=str(tmp_path))
    assert manager.get("trino.host", "padrao") == "padrao"
    assert manager.get("postgres") is None


def test_localiza_resources_automaticamente(monkeypatch):
    """Sem config_base_path explícito, sobe a árvore até achar 'resources/'."""
    manager = ConfigManager()
    assert manager.config_base_path.endswith("resources")
