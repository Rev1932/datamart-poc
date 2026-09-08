import requests
from typing import Optional
import concurrent.futures
from requests.auth import HTTPBasicAuth

class GerenciamentoIngestoesSender:
    def __init__(self, url: str, user: str, password: str):
        self.url = url
        self.auth = HTTPBasicAuth(user, password)
        self.headers = {"Content-Type": "application/json"}
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def _enviar(self, itens: list, identificador: Optional[str]):
        payload = []

        for item in itens:
            item_payload = dict(item)

            if identificador:
                item_payload["identificador"] = identificador

            payload.append(item_payload)

        print("PAYLOAD ENVIADO:")
        print(payload)

        try:
            response = requests.post(
                f"{self.url}/gerenciamento_ingestoes",
                json=payload,
                headers=self.headers,
                timeout=60,
                verify=False,
                auth=self.auth
            )

            print("STATUS:", response.status_code)
            print("RESPOSTA:", response.text)

            response.raise_for_status()

            print("Ingestao enviada.")

        except requests.exceptions.RequestException as e:
            print(f"Falha ao enviar ingestao: {e}")

    def enviar_async(
        self,
        itens: list,
        identificador: str,
        tenant: str,
        filial: str,
        db_name: str,
        table_name: str,
        camada: str,
        last_run=None,
        last_process_run=None,
        last_processed_id=None,
        last_origin_id=None,
        file_path=None,
        process_owner=None,
        desc_error=None,
        last_error_date=None,
        count=None
    ):
        if not tenant or not filial or not db_name or not table_name or not camada:
            print("Campos obrigatorios ausentes: tenant, filial, db_name, table_name, camada.")
            return

        itens_preenchidos = []

        for item in itens:
            item_preenchido = dict(item)

            item_preenchido["tenant"] = tenant
            item_preenchido["filial"] = filial
            item_preenchido["db_name"] = db_name
            item_preenchido["table_name"] = table_name
            item_preenchido["camada"] = camada

            campos_opcionais = {
                "last_run": last_run,
                "last_process_run": last_process_run,
                "last_processed_id": last_processed_id,
                "last_origin_id": last_origin_id,
                "file_path": file_path,
                "process_owner": process_owner,
                "desc_error": desc_error,
                "last_error_date": last_error_date,
                "count": count,
            }

            for campo, valor in campos_opcionais.items():
                if valor is not None:
                    item_preenchido[campo] = valor

            itens_preenchidos.append(item_preenchido)

        # Para teste, deixa síncrono para enxergar o erro real
        self._enviar(itens_preenchidos, identificador)
        


###if __name__ == '__main__':
###    url_api = 'https://api-logs-2.datawake.cloud:9401'
###
###    user = 'datawakeroot'
###    password = 'tFnCc2*bEH885K@BxV86'
###
###    ingestao_sender = GerenciamentoIngestoesSender(url_api, user, password)
###
###    ingestao_sender.enviar_async(
###        itens=[
###            {
###                "last_run": "2026-02-10 10:00:00",
###                "last_process_run": "2026-02-10 10:05:00",
###                "last_processed_id": 88,
###                "last_origin_id": None,
###                "file_path": "s3a://bucket/caminho/arquivo.parquet",
###                "process_owner": "Databee",
###                "desc_error": None,
###                "last_error_date": None,
###                "count": 56
###            }
###        ],
###        identificador="databee",
###        tenant="Datawake",
###        filial="Sabino teste",
###        db_name="rpa",
###        table_name="dw_cadeia_ajuda",
###        camada="Data-bee_"
###    )