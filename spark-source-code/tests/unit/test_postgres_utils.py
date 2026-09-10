import pytest

from utils.postgres_utils import GeneratePostgresQueryUtils


@pytest.fixture
def df(make_df, make_field):
    return make_df(
        make_field("hk_business_id", "StringType"),
        make_field("user", "StringType"),  # palavra reservada precisa continuar citada
        make_field("quantidade", "DecimalType", precision=12, scale=4),
        make_field("load_dts", "TimestampType"),
    )


@pytest.mark.parametrize("type_name,expected", [
    ("StringType", "TEXT"),
    ("IntegerType", "INTEGER"),
    ("ShortType", "SMALLINT"),
    ("LongType", "BIGINT"),
    ("FloatType", "REAL"),
    ("DoubleType", "DOUBLE PRECISION"),
    ("BooleanType", "BOOLEAN"),
    ("BinaryType", "BYTEA"),
    ("TimestampType", "TIMESTAMP"),
    ("TimestampNTZType", "TIMESTAMP"),
    ("DateType", "DATE"),
    ("MapType", "TEXT"),
    ("ArrayType", "TEXT"),
])
def test_get_type_sql_mapeia_tipos(make_field, type_name, expected):
    assert GeneratePostgresQueryUtils.get_type_sql(make_field("c", type_name)) == expected


def test_get_type_sql_decimal_preserva_precisao_e_escala(make_field):
    field = make_field("valor", "DecimalType", precision=12, scale=4)
    assert GeneratePostgresQueryUtils.get_type_sql(field) == "NUMERIC(12,4)"


def test_create_table_staging_unlogged_sem_unique(df):
    sql = GeneratePostgresQueryUtils.generate_sql_create_table(
        df, '"public"."stg_fact"', unlogged=True)

    assert sql.startswith("CREATE UNLOGGED TABLE IF NOT EXISTS")
    assert '"public"."stg_fact"' in sql
    assert "UNIQUE" not in sql
    assert '"hk_business_id" TEXT' in sql
    assert '"user" TEXT' in sql
    assert '"quantidade" NUMERIC(12,4)' in sql
    assert '"load_dts" TIMESTAMP' in sql


def test_create_table_alvo_com_unique_sem_unlogged(df):
    sql = GeneratePostgresQueryUtils.generate_sql_create_table(
        df, '"public"."fact"', unique_key="hk_business_id")

    assert sql.startswith("CREATE TABLE IF NOT EXISTS")
    assert "UNLOGGED" not in sql
    assert 'UNIQUE ("hk_business_id")' in sql


def test_merge_usa_lista_explicita_de_colunas(df):
    sql = GeneratePostgresQueryUtils.generate_sql_merge(
        df, '"public"."fact"', '"public"."stg_fact"', "hk_business_id")

    colunas = '"hk_business_id", "user", "quantidade", "load_dts"'
    assert f'INSERT INTO "public"."fact" ({colunas})' in sql
    assert f'SELECT {colunas} FROM "public"."stg_fact"' in sql
    assert "*" not in sql


def test_merge_conflita_na_chave_e_atualiza_as_demais(df):
    sql = GeneratePostgresQueryUtils.generate_sql_merge(
        df, '"public"."fact"', '"public"."stg_fact"', "hk_business_id")

    assert 'ON CONFLICT ("hk_business_id") DO UPDATE SET' in sql
    assert '"user" = EXCLUDED."user"' in sql
    assert '"quantidade" = EXCLUDED."quantidade"' in sql
    assert '"load_dts" = EXCLUDED."load_dts"' in sql
    assert '"hk_business_id" = EXCLUDED' not in sql


def test_get_qualified_name_cita_schema_e_tabela():
    assert GeneratePostgresQueryUtils.get_qualified_name("public", "fact") == '"public"."fact"'


def test_get_staging_table_name_prefixa_stg():
    assert GeneratePostgresQueryUtils.get_staging_table_name("fact") == "stg_fact"


def test_get_drop_table_usa_if_exists():
    sql = GeneratePostgresQueryUtils.get_drop_table('"public"."stg_fact"')
    assert sql == 'DROP TABLE IF EXISTS "public"."stg_fact";'
