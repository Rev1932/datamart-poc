import pytest

from utils.sql_server_utils import GenerateSqlServerQueryUtils


@pytest.mark.parametrize("type_name,expected", [
    ("StringType", "NVARCHAR(max)"),
    ("IntegerType", "INT"),
    ("LongType", "BIGINT"),
    ("FloatType", "FLOAT"),
    ("DoubleType", "FLOAT"),
    ("BooleanType", "BIT"),
    ("BinaryType", "VARBINARY(MAX)"),
    ("TimestampType", "DATETIME"),
    ("DateType", "DATE"),
    ("DecimalType", "DECIMAL(18,4)"),
    ("MapType", "VARCHAR(8000)"),
])
def test_get_type_sql_mapeia_tipos(type_name, expected):
    assert GenerateSqlServerQueryUtils.get_type_sql(type_name) == expected


def test_generate_sql_create_temp_table(make_df, make_field):
    df = make_df(
        make_field("id", "IntegerType"),
        make_field("nome", "StringType"),
    )
    sql = GenerateSqlServerQueryUtils.generate_sql_create_temp_table(df, "##fact")
    assert sql == "CREATE TABLE ##fact ( id INT, nome NVARCHAR(max) );"


def test_get_temp_table_name_prefixa_global():
    assert GenerateSqlServerQueryUtils.get_temp_table_name("fact") == "##fact"


def test_get_global_merge_proc():
    sql = GenerateSqlServerQueryUtils.get_global_merge_proc("##fact", "fact", "id")
    assert sql == "EXECUTE dbo.sp_Merge_Temp_Global ##fact,dbo,fact,id"


def test_get_drop_temp_table():
    assert GenerateSqlServerQueryUtils.get_drop_temp_table("##fact") == "drop table if exists ##fact"
