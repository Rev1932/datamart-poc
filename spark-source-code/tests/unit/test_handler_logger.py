import logging

import pytest

from utils.config_manager import ConfigManager
from utils.handler_logger import ClickHouseAsyncHandler, initialize_logger


@pytest.fixture
def app_logger_limpo():
    """
    Isola o logger "AppLogger": o conftest raiz adiciona um NullHandler que faria
    initialize_logger() retornar cedo. Limpa os handlers antes e restaura depois.
    """
    logger = logging.getLogger("AppLogger")
    handlers_originais = logger.handlers[:]
    logger.handlers.clear()
    try:
        yield logger
    finally:
        for h in logger.handlers:
            h.close()
        logger.handlers.clear()
        logger.handlers.extend(handlers_originais)


@pytest.fixture(autouse=True)
def limpa_clickhouse_env(monkeypatch):
    for var in ("CLICKHOUSE_URL", "CLICKHOUSE_USERNAME", "CLICKHOUSE_PASSWORD",
                "CLICKHOUSE_SERVICE_NAME", "CLICKHOUSE_VERIFY_SSL"):
        monkeypatch.delenv(var, raising=False)


def _tipos_de_handler(logger):
    return {type(h).__name__ for h in logger.handlers}


def _init_do_zero(logger, cm):
    """
    Garante handlers vazios no instante da chamada. O plugin de logging do pytest
    injeta um LogCaptureHandler no "AppLogger" na fase de call (após o setup da
    fixture), o que faria initialize_logger() retornar cedo pelo guard de handlers.
    """
    logger.handlers.clear()
    return initialize_logger(cm)


def test_sem_url_degrada_para_console_sem_crash(app_logger_limpo, tmp_path):
    cm = ConfigManager(config_base_path=str(tmp_path))
    logger = _init_do_zero(app_logger_limpo, cm)
    tipos = _tipos_de_handler(logger)
    assert "StreamHandler" in tipos
    assert "ClickHouseAsyncHandler" not in tipos


def test_url_vazia_tambem_degrada(app_logger_limpo, tmp_path, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", "")
    cm = ConfigManager(config_base_path=str(tmp_path))
    logger = _init_do_zero(app_logger_limpo, cm)
    assert "ClickHouseAsyncHandler" not in _tipos_de_handler(logger)


def test_com_url_monta_handler_clickhouse(app_logger_limpo, tmp_path, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", "https://ch.datawake.cloud")
    cm = ConfigManager(config_base_path=str(tmp_path))
    logger = _init_do_zero(app_logger_limpo, cm)
    assert "ClickHouseAsyncHandler" in _tipos_de_handler(logger)


def test_handler_direto_sem_url_erra_claro(tmp_path):
    """Uso indevido fora do factory falha explícito, não com AttributeError."""
    cm = ConfigManager(config_base_path=str(tmp_path))
    with pytest.raises(ValueError, match="clickhouse.url"):
        ClickHouseAsyncHandler(cm)
