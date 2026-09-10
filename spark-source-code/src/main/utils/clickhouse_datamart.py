import uuid

import requests

from utils.handler_logger import initialize_logger


class ClickHouseDatamartClient:
    """Cliente HTTP para o DDL e a troca de particao que o connector Spark nao expressa.

    `ALTER TABLE ... REPLACE PARTITION` e parseado pelo Spark, e a API V2 so aceita
    ADD/DROP/RENAME COLUMN e SET TBLPROPERTIES — nao ha SQL arbitrario na API publica.
    """

    def __init__(self, config):
        self.logger = initialize_logger()
        self.url = config["url"]
        self.database = config["database"]
        self.sessao = requests.Session()
        self.sessao.auth = (config["user"], config["password"])

    def executar(self, sql: str) -> str:
        resposta = self.sessao.post(self.url, params={"database": self.database}, data=sql.encode("utf-8"))
        if resposta.status_code != 200:
            raise RuntimeError(f"ClickHouse recusou o comando: {resposta.text.strip()[:500]}")
        return resposta.text.strip()

    def escalar(self, sql: str) -> str:
        return self.executar(sql)

    def nome_staging(self, tabela: str) -> str:
        """Sufixo aleatorio: duas execucoes concorrentes nao podem compartilhar a staging."""
        return f"{tabela}_stg_{uuid.uuid4().hex[:8]}"

    def criar_staging(self, tabela: str, staging: str) -> None:
        # CREATE TABLE AS copia engine, ORDER BY, PARTITION BY e politica de armazenamento —
        # os tres requisitos que o REPLACE PARTITION confere antes de aceitar a troca.
        self.executar(f"CREATE TABLE {self.database}.{staging} AS {self.database}.{tabela}")

    def contar(self, tabela: str) -> int:
        return int(self.escalar(f"SELECT count() FROM {self.database}.{tabela}") or 0)

    def trocar_particoes(self, tabela: str, staging: str, particoes) -> None:
        for particao in particoes:
            self.logger.info(f"REPLACE PARTITION '{particao}' em {self.database}.{tabela}")
            self.executar(
                f"ALTER TABLE {self.database}.{tabela} "
                f"REPLACE PARTITION '{particao}' FROM {self.database}.{staging}"
            )

    def descartar(self, tabela: str) -> None:
        self.executar(f"DROP TABLE IF EXISTS {self.database}.{tabela}")
