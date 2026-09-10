import os

import pytest


def _postgres_do_ambiente():
    """Aponta para um Postgres já de pé quando DATAMART_PG_HOST está no ambiente.

    Sem isso a suíte exige testcontainers, que precisa de um daemon Docker — indisponível
    dentro do cluster, onde o único Postgres real vive.

    O banco apontado NÃO é descartado no fim: os testes assumem estado limpo, então use um
    DATAMART_PG_SCHEMA novo a cada execução.
    """
    host = os.environ.get("DATAMART_PG_HOST")
    if not host:
        return None
    return {
        "host": host,
        "port": int(os.environ.get("DATAMART_PG_PORT", "5432")),
        "database": os.environ["DATAMART_PG_DATABASE"],
        "user": os.environ["DATAMART_PG_USER"],
        "password": os.environ["DATAMART_PG_PASSWORD"],
        "schema": os.environ.get("DATAMART_PG_SCHEMA", "public"),
    }


@pytest.fixture(scope="session")
def postgres_container():
    if _postgres_do_ambiente():
        yield None
        return

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def config_postgres(postgres_container):
    do_ambiente = _postgres_do_ambiente()
    if do_ambiente:
        _criar_schema(do_ambiente)
        return do_ambiente

    return {
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(5432)),
        "database": postgres_container.dbname,
        "user": postgres_container.username,
        "password": postgres_container.password,
        "schema": "public",
    }


def _criar_schema(config):
    import psycopg2

    conexao = psycopg2.connect(
        host=config["host"], port=config["port"], dbname=config["database"],
        user=config["user"], password=config["password"])
    try:
        with conexao, conexao.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{config["schema"]}"')
    finally:
        conexao.close()


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
            pipeline="gold_datamart",
            tenant_name="teste",
            filial_name="teste",
            table_name=table_name,
            primary_key=chave_pk,
        )
        return RepositoryGoldDatamart(spark, config_application, params, str(tmp_path))

    return _make


@pytest.fixture
def pg_schema(config_postgres):
    """Schema onde o repositório grava — nem sempre `public` quando o Postgres é o do cluster."""
    return config_postgres["schema"]


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
