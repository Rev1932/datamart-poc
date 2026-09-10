"""Validação de `--source_tenants` (pipeline `silver_super_tenant`).

Por que validar no PARSE e não na leitura: um `base_path` com typo chegaria ao repositório como
`DeltaTable.isDeltaTable() == False` — indistinguível de "esse cliente não tem essa tabela". O
repositório PULA origens ausentes de propósito (nem todo cliente tem toda tabela), então o typo
sumiria em silêncio e a consolidação rodaria com menos clientes do que o contrato prevê.
"""
import pytest

from utils.pipeline_config import normalize_source_tenants


UMA_ORIGEM = [{"tenant": "belafatia", "base_path": "s3a://datawake-belafatia"}]


# --------------------------------------------------------------------------------------
# Ausência é estado válido — os outros pipelines não passam o argumento
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize("ausente", [None, "", []])
def test_source_tenants_ausente_vira_lista_vazia(ausente):
    """Os demais pipelines não passam o argumento; a config precisa nascer válida mesmo assim."""
    assert normalize_source_tenants(ausente) == []


def test_lista_json_vazia_falha_em_vez_de_virar_ausencia():
    """
    `--source_tenants '[]'` passa a guarda do argparse (o argumento ESTÁ presente) mas não tem
    origem alguma. Tratá-lo como ausência faria a consolidação rodar sem ler nada e terminar
    verde — o desfecho que este pipeline inteiro é desenhado para impedir.
    """
    with pytest.raises(ValueError, match="lista nao vazia"):
        normalize_source_tenants("[]")


def test_aceita_lista_ja_desserializada():
    """A DAG pode montar a lista em Python; o argparse entrega string. Os dois caminhos valem."""
    assert normalize_source_tenants(UMA_ORIGEM) == UMA_ORIGEM


def test_aceita_json_do_argparse():
    entrada = '[{"tenant": "belafatia", "base_path": "s3a://datawake-belafatia"}]'
    assert normalize_source_tenants(entrada) == UMA_ORIGEM


# --------------------------------------------------------------------------------------
# Malformação — falha nomeando o item
# --------------------------------------------------------------------------------------

def test_json_invalido_falha_nomeando_o_argumento():
    with pytest.raises(ValueError, match="--source_tenants"):
        normalize_source_tenants('[{"tenant": "belafatia",]')


def test_json_que_nao_e_lista_falha():
    with pytest.raises(ValueError, match="lista nao vazia"):
        normalize_source_tenants('{"tenant": "belafatia"}')


def test_item_que_nao_e_objeto_falha_mostrando_o_item():
    with pytest.raises(ValueError, match="belafatia"):
        normalize_source_tenants(["belafatia"])


def test_item_sem_tenant_falha():
    with pytest.raises(ValueError, match="sem 'tenant'"):
        normalize_source_tenants([{"base_path": "s3a://datawake-belafatia"}])


def test_item_sem_base_path_falha_nomeando_o_tenant():
    with pytest.raises(ValueError, match="belafatia"):
        normalize_source_tenants([{"tenant": "belafatia"}])


def test_base_path_com_esquema_s3_e_recusado():
    """
    `s3://` não é lido pelo conector configurado na sessão (só `s3a://`). Sem esta guarda o erro
    apareceria como `No FileSystem for scheme: s3` no meio do job, longe da causa.
    """
    with pytest.raises(ValueError, match="s3a://"):
        normalize_source_tenants([{"tenant": "belafatia", "base_path": "s3://datawake-belafatia"}])


# --------------------------------------------------------------------------------------
# Normalização
# --------------------------------------------------------------------------------------

def test_barra_final_do_base_path_e_removida():
    """Sem isso o path de origem vira `s3a://bucket//pasta/tabela` e o Delta não acha o log."""
    normalizado = normalize_source_tenants(
        [{"tenant": "belafatia", "base_path": "s3a://datawake-belafatia/"}]
    )
    assert normalizado[0]["base_path"] == "s3a://datawake-belafatia"


def test_espacos_em_volta_sao_removidos():
    normalizado = normalize_source_tenants(
        [{"tenant": "  belafatia ", "base_path": " s3a://datawake-belafatia "}]
    )
    assert normalizado == UMA_ORIGEM


# --------------------------------------------------------------------------------------
# Duplicatas — as duas formas de perder dado em silêncio
# --------------------------------------------------------------------------------------

def test_tenant_duplicado_falha():
    """Duas origens homônimas colidiriam na mesma partição `source`; uma sobrescreveria a outra."""
    with pytest.raises(ValueError, match="duplicado"):
        normalize_source_tenants([
            {"tenant": "belafatia", "base_path": "s3a://datawake-belafatia"},
            {"tenant": "belafatia", "base_path": "s3a://datawake-belafatia-2"},
        ])


def test_base_path_duplicado_falha():
    """
    Dois tenants apontando para o MESMO bucket geram dois `hk_business_id` diferentes para a
    mesma linha física — duplicação no destino que não é deduplicável depois, porque os hashes
    divergem.
    """
    with pytest.raises(ValueError, match="repetido"):
        normalize_source_tenants([
            {"tenant": "belafatia", "base_path": "s3a://datawake-belafatia"},
            {"tenant": "belafatia_matriz", "base_path": "s3a://datawake-belafatia"},
        ])
