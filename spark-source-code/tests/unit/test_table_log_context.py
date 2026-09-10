"""Contexto de tabela por thread no logger.

No modo multi-tabela várias threads escrevem no MESMO logger singleton (`AppLogger`).
Sem este contexto, os logs das N tabelas se intercalam no stdout do driver sem forma de
saber a qual tabela cada linha pertence.
"""
import logging
import threading

import pytest

from utils.handler_logger import (
    TableContextFilter,
    get_current_table,
    set_current_table,
)


@pytest.fixture(autouse=True)
def limpa_contexto():
    set_current_table(None)
    yield
    set_current_table(None)


def _record():
    return logging.LogRecord(
        name="AppLogger", level=logging.INFO, pathname=__file__, lineno=1,
        msg="mensagem", args=(), exc_info=None,
    )


def test_injeta_a_tabela_da_thread_atual():
    set_current_table("dw_a")
    record = _record()

    TableContextFilter().filter(record)

    assert record.tabela == "dw_a"
    assert record.tabela_log == "[dw_a] "


def test_sem_contexto_nao_polui_a_mensagem():
    """No modo single-table não há contexto: o formatter não pode imprimir 'None'."""
    record = _record()

    TableContextFilter().filter(record)

    assert record.tabela is None
    assert record.tabela_log == ""


def test_nao_sobrescreve_contexto_ja_presente_no_record():
    """Um LoggerAdapter com `extra={'tabela': ...}` é mais específico e deve vencer."""
    set_current_table("dw_thread")
    record = _record()
    record.tabela = "dw_adapter"

    TableContextFilter().filter(record)

    assert record.tabela == "dw_adapter"


def test_contexto_e_isolado_entre_threads():
    """A propriedade que torna o prefixo confiável com max_workers > 1."""
    vistos = {}
    liberado = threading.Event()

    def worker(nome):
        set_current_table(nome)
        liberado.wait(timeout=2)
        vistos[nome] = get_current_table()

    threads = [threading.Thread(target=worker, args=(f"dw_{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    liberado.set()
    for t in threads:
        t.join(timeout=5)

    assert vistos == {f"dw_{i}": f"dw_{i}" for i in range(4)}
    # a thread principal nunca foi contaminada
    assert get_current_table() is None


def test_formatter_usa_o_prefixo():
    """Integração do filtro com o formatter registrado em initialize_logger()."""
    set_current_table("dw_a")
    record = _record()
    TableContextFilter().filter(record)

    formatter = logging.Formatter('%(levelname)s - %(tabela_log)s%(message)s')

    assert formatter.format(record) == "INFO - [dw_a] mensagem"
