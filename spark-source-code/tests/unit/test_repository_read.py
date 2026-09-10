"""Ordem de validação na leitura de parquet (`RepositoryBronzeToSilver`).

O defeito que estes testes ancoram: `read()` checava a tabela Delta de DESTINO e carregava o
schema dela ANTES de olhar para a origem. A pergunta "existe arquivo para processar?" era
respondida pelo efeito colateral de `spark.read.parquet()` estourar
`AnalysisException: Unable to infer schema for Parquet` num `except Exception` abrangente.

Consequência: qualquer falha (S3 fora, credencial inválida, parquet corrompido) virava
`None`, e `None` é lido por `process_data()` como "nada a processar" — tabela reportada como
sucesso sem ter feito nada.
"""
import types

import pytest

from repo.repository import RepositoryBronzeToSilver


class FakeMinio:
    """Duplo do MinioAdmin: só a listagem importa aqui."""

    def __init__(self, object_names=(), erro=None):
        self.object_names = list(object_names)
        self.erro = erro
        self.chamadas = []

    def list_path_files(self, path):
        self.chamadas.append(path)
        if self.erro:
            raise self.erro
        return [types.SimpleNamespace(object_name=n) for n in self.object_names]


class FakeReader:
    def __init__(self, registro):
        self._registro = registro

    def parquet(self, *paths):
        self._registro.append(("parquet", paths))
        return types.SimpleNamespace(columns=["id"], select=lambda *a: "DATAFRAME")

    def format(self, fmt):
        self._registro.append(("format", fmt))
        return self

    def load(self, path):
        self._registro.append(("load", path))
        return types.SimpleNamespace(schema=[])


class FakeSpark:
    def __init__(self):
        self.registro = []
        self.read = FakeReader(self.registro)


@pytest.fixture
def repositorio(monkeypatch):
    """Constrói o repositório sem tocar em MinIO nem Spark reais."""
    def _build(object_names=(), erro_listagem=None, destino_e_delta=False):
        fake_minio = FakeMinio(object_names, erro_listagem)
        monkeypatch.setattr(
            "repo.repository.MinioAdmin",
            types.SimpleNamespace(shared=lambda *a, **k: fake_minio),
        )

        chamadas_is_delta = []

        def _is_delta(spark, path):
            chamadas_is_delta.append(path)
            return destino_e_delta

        monkeypatch.setattr(
            "repo.repository.DeltaTable",
            types.SimpleNamespace(isDeltaTable=_is_delta),
        )

        spark = FakeSpark()
        repo = RepositoryBronzeToSilver(
            spark,
            {"minio.base_path": "s3a://bucket", "spark.s3": {}},
            {"filial_name": "limeira", "table_name": "dw_a"},
        )
        return repo, spark, fake_minio, chamadas_is_delta

    return _build


# --------------------------------------------------------------------------------------
# A ordem — o cerne da correção
# --------------------------------------------------------------------------------------

def test_origem_vazia_devolve_none_sem_tocar_no_destino(repositorio):
    """
    A prova de que a ordem está correta: com a origem vazia, o método NÃO pode ter checado a
    tabela Delta de destino nem lido o schema dela. Antes, essas duas operações vinham
    primeiro e o "vazio" só era descoberto depois.
    """
    repo, spark, fake_minio, chamadas_is_delta = repositorio(object_names=[])

    assert repo.read() is None
    # a listagem aconteceu...
    assert fake_minio.chamadas == ["s3a://bucket/data-bee_replication/data-bee_limeira/dw_a"]
    # ...e nada mais aconteceu
    assert chamadas_is_delta == []
    assert spark.registro == []


def test_ignora_arquivos_que_nao_sao_parquet(repositorio):
    """A subpasta `processed/` aparece na listagem; só `.parquet` conta como pendente."""
    repo, _, _, chamadas_is_delta = repositorio(
        object_names=["caminho/processed/", "caminho/x.parquet.bkp"]
    )

    assert repo.read() is None
    assert chamadas_is_delta == []


# --------------------------------------------------------------------------------------
# Falha real x origem vazia
# --------------------------------------------------------------------------------------

def test_falha_de_listagem_propaga_em_vez_de_virar_origem_vazia(repositorio):
    """
    A regressão mais perigosa que esta correção impede: MinIO fora do ar devolvendo `[]`
    seria indistinguível de pasta vazia, e a tabela ficaria VERDE com o dado parado na origem.
    """
    repo, _, _, _ = repositorio(erro_listagem=ConnectionError("minio indisponivel"))

    with pytest.raises(ConnectionError, match="minio indisponivel"):
        repo.read()


def test_get_origin_list_file_names_relanca(repositorio):
    """Afeta também get_files_grouped_by_schema e process_one_file_per_execution."""
    repo, _, _, _ = repositorio(erro_listagem=OSError("timeout"))

    with pytest.raises(OSError, match="timeout"):
        repo.get_origin_list_file_names()


# --------------------------------------------------------------------------------------
# Leitura: lista explícita, nunca o diretório
# --------------------------------------------------------------------------------------

def test_le_a_lista_explicita_de_arquivos_e_nao_o_diretorio(repositorio):
    """
    Ler o diretório deixaria o Spark descer em `processed/` e reler os `.bkp` já arquivados,
    além de divergir da listagem usada na checagem de vazio.
    """
    repo, spark, _, _ = repositorio(object_names=["pasta/a.parquet", "pasta/b.parquet"])

    repo.read()

    leituras = [args for op, args in spark.registro if op == "parquet"]
    assert leituras == [("s3a://bucket/pasta/a.parquet", "s3a://bucket/pasta/b.parquet")]
    # o input_path nunca é passado ao Spark
    assert all(repo.input_path not in p for args in leituras for p in args)


def test_alinha_schema_quando_o_destino_delta_existe(repositorio):
    """Com destino existente, a leitura passa pelo alinhamento — comportamento preservado."""
    repo, spark, _, chamadas_is_delta = repositorio(
        object_names=["pasta/a.parquet"], destino_e_delta=True
    )

    assert repo.read() == "DATAFRAME"
    assert chamadas_is_delta == [repo.output_path]
    assert ("load", repo.output_path) in spark.registro
