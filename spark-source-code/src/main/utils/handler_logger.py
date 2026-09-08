import logging
import json
import requests
import datetime
import time
import random
import uuid
import socket
import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from concurrent.futures import ThreadPoolExecutor
from logging import LoggerAdapter

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
        self.base_url = config.get("clickhouse.url").rstrip('/')
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

    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger_base.addHandler(console_handler)

    # Envio de logs para o ClickHouse é opcional. Desabilite com
    # clickhouse.logs_enabled = false (default: habilitado, preserva o comportamento atual).
    if str(config_manager.get("clickhouse.logs_enabled", True)).lower() == "true":
        ch_handler = ClickHouseAsyncHandler(config_manager)
        logger_base.addHandler(ch_handler)

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

    contexto_inicial = {
        "rotina": config.topic,
        "cliente": config.config_name,
        "tabela": config.table_name,
        "id_session": applicationId
    }
    return LoggerAdapter(logger_base, contexto_inicial)

