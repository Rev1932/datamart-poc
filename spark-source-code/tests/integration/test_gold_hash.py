import hashlib

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def repository_com_view(spark, make_repository):
    """Repositório gold_datamart lendo de uma temp view (sem Delta/MinIO)."""

    def _make(table_name, chave_pk, rows):
        df = spark.createDataFrame(rows, "a string, b string")
        df.createOrReplaceTempView(f"src_{table_name}")
        return make_repository(table_name, chave_pk, query_sql=f"SELECT * FROM src_{table_name}")

    return _make


def _sha256(texto):
    return hashlib.sha256(texto.encode()).hexdigest()


def _hashes(repository):
    return [row["hk_business_id"] for row in repository.read().select("hk_business_id").collect()]


def test_hash_e_deterministico_e_igual_ao_sha256_das_chaves(repository_com_view):
    repository = repository_com_view("t_det", ["a", "b"], [("x", "y")])

    primeira = _hashes(repository)
    segunda = _hashes(repository)

    assert primeira == segunda
    assert primeira == [_sha256("x_y")]


def test_ordem_das_chaves_altera_o_hash(repository_com_view, make_repository):
    repository_ab = repository_com_view("t_ordem", ["a", "b"], [("x", "y")])
    repository_ba = make_repository("t_ordem_inv", ["b", "a"], query_sql="SELECT * FROM src_t_ordem")

    assert _hashes(repository_ab) != _hashes(repository_ba)
    assert _hashes(repository_ba) == [_sha256("y_x")]


def test_null_nas_chaves_colide_com_chave_composta_equivalente(repository_com_view):
    # concat_ws ignora null: ("a_b", null) e ("a", "b") geram o MESMO hash.
    # Comportamento herdado do pipeline gold Delta, documentado aqui como contrato:
    # chaves com null são risco real de colisão.
    repository = repository_com_view("t_null", ["a", "b"], [("a_b", None), ("a", "b")])

    hashes = _hashes(repository)
    assert hashes[0] == hashes[1] == _sha256("a_b")


def test_string_vazia_nas_chaves_nao_colide_com_null(repository_com_view):
    # String vazia mantém o separador ("x_"), diferente de null (vira só "x").
    repository = repository_com_view("t_vazio", ["a", "b"], [("x", ""), ("x", None)])

    hashes = _hashes(repository)
    assert hashes[0] == _sha256("x_")
    assert hashes[1] == _sha256("x")
    assert hashes[0] != hashes[1]


def test_transform_mantem_uma_linha_por_hash(repository_com_view):
    repository = repository_com_view(
        "t_dedup", ["a", "b"], [("x", "y"), ("x", "y"), ("z", "w")])

    data = repository.read()
    deduplicado = repository.transform(data)

    assert data.count() == 3
    assert deduplicado.count() == 2
