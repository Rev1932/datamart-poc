import re
from pathlib import Path

import pytest

from utils.queryutils import QueryUtils

pytestmark = pytest.mark.integration

QUERIES_DIR = Path(__file__).resolve().parents[2] / "resources" / "queries"

_REPLACEMENTS = {
    "minio.base_path": "s3a://bucket-teste",
    "trino.schema_folder_silver": "silver",
    "trino.schema_folder_gold": "gold",
}

_PLACEHOLDER_ORFAO = re.compile(r"MINIO_BASE_PATH|S3_PATH_[A-Z_]+|JANELA_[A-Z]+")


class FakeConfig:
    def get(self, key, default=None):
        return _REPLACEMENTS.get(key, default)


class FakeRuntime:
    """Janela fixa: a query precisa parsear, o valor em si nao importa aqui."""

    _VALORES = {"janela_inicio": "2026-01-01 00:00:00", "janela_fim": "2026-02-01 00:00:00"}

    def get(self, key, default=None):
        return self._VALORES.get(key, default)


def _parse(spark, query):
    """Valida a sintaxe com o parser real do Spark, sem executar a query."""
    spark._jsparkSession.sessionState().sqlParser().parsePlan(query)


def _sql_files():
    return sorted(path.stem for path in QUERIES_DIR.glob("*.sql"))


@pytest.mark.parametrize("table_name", _sql_files() or [None])
def test_query_do_projeto_parseia_no_spark(spark, table_name):
    if table_name is None:
        pytest.skip("resources/queries ainda sem .sql — migração das queries pendente")

    query = QueryUtils.build_query(str(QUERIES_DIR), FakeConfig(), table_name, FakeRuntime())

    orfao = _PLACEHOLDER_ORFAO.search(query)
    assert orfao is None, f"{table_name}.sql: placeholder não substituído: {orfao.group()}"

    try:
        _parse(spark, query)
    except Exception as exc:
        pytest.fail(f"{table_name}.sql não parseia: {exc}")


# Validação da própria máquina de verificação, com .sql sintéticos — cobre o
# esqueleto enquanto as queries reais não são migradas.

def test_maquina_aceita_sql_valido(spark, tmp_path):
    (tmp_path / "ok.sql").write_text(
        "SELECT a, b FROM delta.`MINIO_BASE_PATH/S3_PATH_SILVER/tabela` WHERE a > 1",
        encoding="utf-8",
    )
    query = QueryUtils.build_query(str(tmp_path), FakeConfig(), "ok")
    _parse(spark, query)


def test_maquina_rejeita_sql_invalido(spark, tmp_path):
    # Parêntese desbalanceado: a classe de erro típica de uma migração manual.
    # (Obs.: "SELECT FROM WHERE" parseia — fora do modo ANSI, keywords do Spark
    # são identificadores válidos.)
    (tmp_path / "quebrado.sql").write_text(
        "SELECT a FROM tabela WHERE (a = 1", encoding="utf-8")
    query = QueryUtils.build_query(str(tmp_path), FakeConfig(), "quebrado")

    with pytest.raises(Exception):
        _parse(spark, query)


def test_maquina_detecta_placeholder_orfao():
    assert _PLACEHOLDER_ORFAO.search("delta.`s3a://bucket/S3_PATH_SILVR/tabela`")
