import logging
import json
import requests
import datetime
import threading
import time
import random
import uuid
import socket
import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from concurrent.futures import ThreadPoolExecutor
from logging import LoggerAdapter

# --------------------------------------------------------------------------------------------
# Contexto de tabela por THREAD.
#
# No modo multi-tabela varias threads processam tabelas diferentes escrevendo no MESMO logger
# singleton (`AppLogger`). Sem isso, os logs das N tabelas se intercalam no stdout do driver
# sem nenhuma forma de saber a qual tabela cada linha pertence.
#
# A escolha por threading.local + Filter, em vez de passar um LoggerAdapter pela cadeia de
# chamadas, e deliberada: todas as classes (repositories, transformers, pipelines) ja chamam
# `initialize_logger()` e recebem o mesmo singleton. O filtro injeta o contexto sem exigir
# alteracao em nenhuma delas.
# --------------------------------------------------------------------------------------------
_contexto_thread = threading.local()


def set_current_table(nome: str | None) -> None:
    """Marca a tabela que a thread atual esta processando (ou None para limpar)."""
    _contexto_thread.tabela = nome


def get_current_table() -> str | None:
    return getattr(_contexto_thread, "tabela", None)


class TableContextFilter(logging.Filter):
    """Injeta `record.tabela` a partir do contexto da thread.

    Nao sobrescreve `tabela` quando o record ja traz o campo (ex.: vindo de um LoggerAdapter),
    para nao atropelar um contexto mais especifico.
    """

    def filter(self, record):
        if not getattr(record, "tabela", None):
            record.tabela = get_current_table()
        # Campo separado para o console: `tabela` vai crua para o ClickHouse (None quando nao
        # ha contexto), enquanto `tabela_log` e o prefixo formatado — assim o formatter nao
        # imprime "None" nas mensagens do modo single-table.
        record.tabela_log = f"[{record.tabela}] " if record.tabela else ""
        return True

class TLSAdapter(HTTPAdapter):
    """
    Adaptador HTTP para forçar o uso de TLS 1.3.
    """
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.maximum_version = ssl.TLSVersion.TLSv1_3
        kwargs['ssl_context'] = context
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        context = create_urllib3_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.maximum_version = ssl.TLSVersion.TLSv1_3
        kwargs['ssl_context'] = context
        return super(TLSAdapter, self).proxy_manager_for(*args, **kwargs)

class ClickHouseAsyncHandler(logging.Handler):
    def __init__(self, config, service_name="python-app"):
        super().__init__()
        import os
        # Carrega configurações do ConfigManager
        clickhouse_url = config.get("clickhouse.url")
        if not clickhouse_url:
            raise ValueError("clickhouse.url é obrigatória para o handler ClickHouse")
        self.base_url = clickhouse_url.rstrip('/')
        self.auth = (
            config.get("clickhouse.username"),
            config.get("clickhouse.password")
        )
        self.service_name = config.get("clickhouse.service_name")
        self.executor = ThreadPoolExecutor(max_workers=2)
        
        # Configuração SSL
        verify_ssl = config.get("clickhouse.verify_ssl", True)
        if isinstance(verify_ssl, str) and not os.path.isabs(verify_ssl):
            # Tenta localizar o certificado na pasta resources se o caminho não for absoluto
            resources_path = getattr(config, 'config_base_path', None)
            if resources_path:
                cert_path = os.path.join(resources_path, verify_ssl)
                if os.path.exists(cert_path):
                    verify_ssl = cert_path
        self.verify = verify_ssl

        # Configura a sessão com o TLS 1.3 Adapter
        self.session = requests.Session()
        self.session.mount("https://", TLSAdapter())

    def emit(self, record):
        try:
            payload = self.format_record(record)
            self.executor.submit(self._send_log, payload)
        except Exception:
            self.handleError(record)

    def format_record(self, record): 
        dt = datetime.datetime.fromtimestamp(record.created)
        # Validar se precisa corrigir o utc na sua aplicacao
        #dt = datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc)
        
        log_id = int(time.time() * 1000000) + random.randint(0, 999)
        return {
            "id": log_id,
            "id_session": getattr(record, "id_session", None),
            "data_hora": dt.strftime('%Y-%m-%d %H:%M:%S'),
            "sistema": self.service_name,
            "rotina": getattr(record, "rotina", "Geral"),
            "usuario": getattr(record, "user_id", "logger_unipac"),
            "tipo": record.levelname,
            "mensagem": record.getMessage(),
            "cliente": getattr(record, "cliente", None),
            "ip_user": getattr(record, "ip_user", self.obter_ip_local()),
            "tabela": getattr(record, "tabela", None),
            "topico": getattr(record, "topico", None),
            "op": getattr(record, "movimento", None),
            "unidade_producao": getattr(record, "unidade_producao", None),
            "uid": getattr(record, "uid", None)
        }

    def _send_log(self, json_data):
        try:
            data_str = json.dumps(json_data)
            params = {'query': 'INSERT INTO datawake_logs.logs FORMAT JSONEachRow'}
            response = self.session.post(
                self.base_url,
                params=params, 
                auth=self.auth,
                data=data_str,
                timeout=5,
                verify=self.verify
            )
            if response.status_code != 200:
                print(f"Erro ClickHouse ({response.status_code}): {response.text}")
        except Exception as e:
            print(f"Erro Conexão Log: {e}")

    def obter_ip_local(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip_local = s.getsockname()[0]
            s.close()
            return ip_local
        except Exception:
            return "Não foi possível obter o IP"

    def close(self):
        self.executor.shutdown(wait=True)
        self.session.close()
        super().close()

class SessionUidFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.uid = str(uuid.uuid4())

    def filter(self, record):
        record.uid = self.uid
        return True

def initialize_logger(config_manager=None):
    logger_base = logging.getLogger("AppLogger")

    if logger_base.handlers:
        return logger_base

    if config_manager is None:
        # Tenta criar um ConfigManager padrão se não for fornecido
        from utils.config_manager import ConfigManager
        config_manager = ConfigManager()

    logger_base.setLevel(logging.INFO)
    logger_base.propagate = False

    # Filtros ANTES dos handlers: no modo multi-tabela varias threads escrevem neste mesmo
    # logger, e sem o prefixo de tabela o stdout do driver fica ilegivel.
    logger_base.addFilter(TableContextFilter())

    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(tabela_log)s%(message)s'
    ))
    logger_base.addHandler(console_handler)

    # Sink ClickHouse é opcional: sem clickhouse.url (ex.: CLICKHOUSE_URL ausente no
    # manifesto), degrada para console em vez de derrubar o job efêmero na importação.
    if config_manager.get("clickhouse.url"):
        logger_base.addHandler(ClickHouseAsyncHandler(config_manager))
    else:
        logger_base.warning(
            "clickhouse.url não configurada (CLICKHOUSE_URL ausente): "
            "logging remoto ClickHouse desativado; seguindo apenas com console."
        )

    session_filter = SessionUidFilter()
    logger_base.addFilter(session_filter)
    
    return logger_base


def apply_and_trace_context(logger_base, config, spark = None) -> LoggerAdapter:
    """
    Aplica o contexto ao logger e retorna um LoggerAdapter.
    """
    if spark is None:
        applicationId = None 
    else: 
        applicationId = spark.sparkContext.applicationId    

    # No modo multi-tabela a config base nao tem `table_name` — quem identifica a tabela de
    # cada linha e o TableContextFilter, por thread. Aqui a rotina fica no nivel da filial.
    rotina = (
        f"{config.filial_name}_{config.table_name}"
        if config.table_name else str(config.filial_name)
    )

    contexto_inicial = {
        "rotina": rotina,
        "cliente": config.tenant_name,
        "tabela": config.table_name,
        "id_session": applicationId
    }
    return LoggerAdapter(logger_base, contexto_inicial)

