import pytest


@pytest.fixture(scope="session")
def postgres_container():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def config_postgres(postgres_container):
    return {
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(5432)),
        "database": postgres_container.dbname,
        "user": postgres_container.username,
        "password": postgres_container.password,
        "schema": "public",
    }


class FakeConfigApplication:
    """ConfigManager mínimo: get() por chave pontilhada, como o real."""

    def __init__(self, data):
        self._data = data

    def get(self, key, default=None):
        return self._data.get(key, default)


@pytest.fixture
def make_repository(spark, config_postgres, tmp_path):
    """Constrói RepositoryGoldDatamart com a query .sql em diretório temporário."""

    def _make(table_name, chave_pk, query_sql="SELECT 1"):
        from repo.repository import RepositoryGoldDatamart
        from utils.pipeline_config import PipelineConfig

        (tmp_path / f"{table_name}.sql").write_text(query_sql, encoding="utf-8")
        config_application = FakeConfigApplication({
            "postgres": config_postgres,
            "minio.base_path": "s3a://bucket-teste",
            "trino.schema_folder_silver": "silver",
            "trino.schema_folder_gold": "gold",
        })
        params = PipelineConfig(
            pipeline_type="gold_datamart",
            config_name="teste",
            table_name=table_name,
            topic=f"teste_{table_name}",
            chave_pk=chave_pk,
        )
        return RepositoryGoldDatamart(spark, config_application, params, str(tmp_path))

    return _make


@pytest.fixture
def pg_query(config_postgres):
    """Executa SQL no Postgres efêmero; retorna linhas para SELECT, None para DDL/DML."""

    def _run(sql):
        import psycopg2

        connection = psycopg2.connect(
            host=config_postgres["host"],
            port=config_postgres["port"],
            dbname=config_postgres["database"],
            user=config_postgres["user"],
            password=config_postgres["password"],
        )
        try:
            with connection, connection.cursor() as cursor:
                cursor.execute(sql)
                if cursor.description:
                    return cursor.fetchall()
                return None
        finally:
            connection.close()

    return _run
