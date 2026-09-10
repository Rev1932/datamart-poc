import importlib.util
import logging
import sys
import types

import pytest

# initialize_logger() (chamado no import da pipeline_factory e no __init__ dos
# repositórios) constrói um handler de ClickHouse a partir de config real; com um
# handler pré-existente no "AppLogger" ele retorna cedo, mantendo a suíte offline
# e sem dependência de config externa.
logging.getLogger("AppLogger").addHandler(logging.NullHandler())


class Stub:
    """Substituto permissivo para símbolos de bibliotecas não instaladas no ambiente de teste."""

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return Stub()

    def __getattr__(self, name):
        return Stub()


def _stub_module(name):
    module = types.ModuleType(name)
    module.__getattr__ = lambda attr: Stub()
    sys.modules[name] = module
    if "." in name:
        parent, child = name.rsplit(".", 1)
        setattr(sys.modules[parent], child, module)


# Bibliotecas importadas no topo dos módulos-alvo mas irrelevantes para os testes
# (nenhuma asserção é feita sobre esses stubs). Pais antes dos filhos.
_STUBBED_LIBS = [
    "psycopg2",
    "pyodbc",
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.sql.avro",
    "pyspark.sql.avro.functions",
    "pyspark.sql.window",
    "delta",
    "delta.tables",
    "delta.exceptions",
    "confluent_kafka",
    "confluent_kafka.schema_registry",
    "minio",
    "minio.commonconfig",
    "minio.deleteobjects",
    "trino",
    "trino.dbapi",
    "trino.auth",
]


def _is_missing(name):
    try:
        return importlib.util.find_spec(name) is None
    except Exception:
        return True


# Stub apenas do que realmente falta no ambiente: bibliotecas instaladas (ex.:
# pyspark, psycopg2) são usadas reais — um stub delas envenenaria a suíte de
# integração, que roda no mesmo processo. Submódulos de uma raiz stubada são
# stubados direto: find_spec não consegue inspecionar um pai fake.
_stubbed_roots = set()
for _name in _STUBBED_LIBS:
    if _name in sys.modules:
        continue
    if _name.split(".")[0] in _stubbed_roots or _is_missing(_name):
        _stub_module(_name)
        _stubbed_roots.add(_name.split(".")[0])


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .appName("spark_into_postgres-tests")
        .master("local[2]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.7")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
