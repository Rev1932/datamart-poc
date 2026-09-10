import logging
import os
from pyhocon import ConfigFactory

logger = logging.getLogger(__name__)


def _coerce_bool(value: str) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes")


def _coerce_verify_ssl(value: str):
    """
    `clickhouse.verify_ssl` aceita bool OU nome de certificado.

    "true"/"false" viram bool (liga/desliga a verificação); qualquer outro valor
    (ex.: "wildcard_datadriven_cloud.crt") fica como str para o handler resolver
    contra `resources/` (ver handler_logger.ClickHouseAsyncHandler).
    """
    normalized = str(value).strip().lower()
    if normalized in ("true", "1", "yes"):
        return True
    if normalized in ("false", "0", "no"):
        return False
    return value.strip()


# Fonte única da verdade do contrato de configuração no Kubernetes.
# Cada entrada mapeia uma variável de ambiente (injetada pelo manifesto via
# ConfigMap/Secret) para o caminho pontilhado consumido pelo código e a coerção
# de tipo. Env vars chegam sempre como str: `port` precisa virar int e
# `verify_ssl` precisa virar bool (a string "false" seria truthy e desligaria a
# verificação TLS do Trino por engano).
# (ENV_VAR, "caminho.pontilhado", coercao)
_ENV_SCHEMA = [ #TODO mapear o que vai estar por padrão no Spark e o que vai estar no ConfigMap
    ("MINIO_BASE_PATH",            "minio.base_path",             str),
    ("SPARK_S3_ENDPOINT",          "spark.s3.endpoint",           str),
    ("SPARK_S3_ACCESS_KEY",        "spark.s3.access_key",         str),
    ("SPARK_S3_SECRET_KEY",        "spark.s3.secret_key",         str),
    ("POSTGRES_HOST",              "postgres.host",               str),
    ("POSTGRES_PORT",              "postgres.port",               int),
    ("POSTGRES_DATABASE",          "postgres.database",           str),
    ("POSTGRES_USER",              "postgres.user",               str),
    ("POSTGRES_PASSWORD",          "postgres.password",           str),
    ("POSTGRES_SCHEMA",            "postgres.schema",             str),
    ("TRINO_TYPE_AUTH",            "trino.type_auth",             str),
    ("TRINO_HOST",                 "trino.host",                  str),
    ("TRINO_PORT",                 "trino.port",                  int),
    ("TRINO_CATALOG",              "trino.catalog",               str),
    ("TRINO_SCHEMA",               "trino.schema",                str),
    ("TRINO_SCHEMA_FOLDER",        "trino.schema_folder",         str),
    ("TRINO_SCHEMA_FOLDER_SILVER", "trino.schema_folder_silver",  str),
    ("TRINO_SCHEMA_FOLDER_GOLD",   "trino.schema_folder_gold",    str),
    ("TRINO_VERIFY_SSL",           "trino.verify_ssl",            _coerce_bool),
    ("TRINO_USER",                 "trino.user",                  str),
    ("TRINO_PASSWORD",             "trino.password",              str),
    ("TRINO_BASE_URL",             "trino.base_url",              str),
    ("TRINO_CLIENT_ID",            "trino.client_id",             str),
    ("TRINO_CLIENT_SECRET",        "trino.client_secret",         str),
    ("CLICKHOUSE_URL",             "clickhouse.url",              str),
    ("CLICKHOUSE_USERNAME",        "clickhouse.username",         str),
    ("CLICKHOUSE_PASSWORD",        "clickhouse.password",         str),
    ("CLICKHOUSE_SERVICE_NAME",    "clickhouse.service_name",     str),
    ("CLICKHOUSE_VERIFY_SSL",      "clickhouse.verify_ssl",       _coerce_verify_ssl),
    # Prefixo proprio: as CLICKHOUSE_* acima pertencem ao handler_logger de auditoria, que
    # em producao aponta para api-logs. Reusa-las faria o log escrever no datamart.
    ("DATAMART_CH_URL",            "datamart_clickhouse.url",      str),
    ("DATAMART_CH_USER",           "datamart_clickhouse.user",     str),
    ("DATAMART_CH_PASSWORD",       "datamart_clickhouse.password", str),
    ("DATAMART_CH_DATABASE",       "datamart_clickhouse.database", str),
    ("DATAMART_CH_CATALOG",        "datamart_clickhouse.catalog",  str),
]


class ConfigManager:
    """
    Monta a configuração da aplicação a partir das variáveis de ambiente.

    No Kubernetes todos os valores e segredos são injetados como env vars pelo
    manifesto (ConfigMap + Secret via envFrom). Esta classe lê `os.environ`,
    guiada por `_ENV_SCHEMA`, e remonta a estrutura aninhada esperada pelos
    consumidores. O objeto interno continua sendo um `ConfigTree` (pyhocon), que
    entrega o mesmo contrato de acesso usado no código: `get("a.b")` pontilhado,
    subscrição `["chave"]`, `.items()` e `get(chave, default)` nos nós aninhados.

    Nenhum arquivo `.conf` é lido em runtime — apenas os `.sql` de
    `resources/queries/`, localizados por `config_base_path`.
    """

    def __init__(self, config_base_path: str = None, env: str = None):
        """
        Args:
            config_base_path (str): Caminho da pasta 'resources'. Se None, é
                localizada automaticamente subindo a árvore de diretórios (ainda
                necessária para achar 'queries/').
            env (str): Rótulo de ambiente, apenas informativo/log. Default: env
                var ENV ou 'local'.
        """
        self.env = (env or os.getenv("ENV", "local")).lower()

        # Localiza a pasta resources automaticamente se não for informada.
        if config_base_path is None:
            current_path = os.path.abspath(__file__)
            while True:
                parent_path = os.path.dirname(current_path)
                if parent_path == current_path:  # Chegou na raiz do sistema
                    break

                potential_resources = os.path.join(parent_path, "resources")
                if os.path.isdir(potential_resources):
                    config_base_path = potential_resources
                    break
                current_path = parent_path

            if config_base_path is None:
                raise Exception("Não foi possível localizar a pasta 'resources' dinamicamente.")

        self.config_base_path = config_base_path.rstrip('/')
        self.config = None
        self._load_from_env()

    @staticmethod
    def _set_nested(target: dict, dotted_key: str, value):
        parts = dotted_key.split(".")
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = value

    def _load_from_env(self):
        """
        Constrói o ConfigTree a partir de os.environ conforme `_ENV_SCHEMA`.

        Variável ausente é omitida (não vira chave com None): reproduz a
        semântica do `${?VAR}` opcional do HOCON e mantém `get(chave, default)`
        retornando o default. Isso é intencional — cada pipeline consome um
        subconjunto diferente das chaves.

        `spark.conf` não é populado aqui: no K8s os `sparkConf` vêm do manifesto
        do Spark Operator / spark-defaults.conf da plataforma. Os consumidores já
        tratam `get("spark.conf", {})` ausente iterando um dict vazio.
        """
        nested = {}
        for env_name, dotted_key, coerce in _ENV_SCHEMA:
            raw = os.environ.get(env_name)
            if raw is None:
                continue
            self._set_nested(nested, dotted_key, coerce(raw))

        self.config = ConfigFactory.from_dict(nested)

    def get(self, key: str, default=None):
        """
        Recupera um valor de configuração por chave (aceita caminho pontilhado).
        """
        try:
            return self.config.get(key, default)
        except Exception:
            logger.error(f"Chave de configuração '{key}' não encontrada, retornando valor padrão: {default}")
            return default

    def get_config_tree(self):
        """
        Retorna toda a configuração como um objeto ConfigTree.
        """
        return self.config
