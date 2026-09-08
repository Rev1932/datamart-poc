import warnings
import requests
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from trino.auth import JWTAuthentication
from urllib3.exceptions import InsecureRequestWarning
import sys
import logging

# Desabilitar especificamente o InsecureRequestWarning do urllib3
warnings.simplefilter('ignore', InsecureRequestWarning)

class TrinoConnection:
    def __init__(self, config_manager): #Como o Trino ainda não tem senha o param aqui não está sendo contemplado
        """
        Inicializa a conexão com o Trino.
        host = endereço do Trino (sem necessidadefor valido ele define o http ou https automaticamente)
        port = porta aberta para acesso
        user = admin
        catalog = datalake_delta
        schema = variavel conforme a camada
        """
        self.HTTPS_CODE="https"
        self.config_manager = config_manager
        try:
            
            self.type_auth = self.config_manager.get("trino.type_auth")

            self.host = self.config_manager.get("trino.host")
            self.port = self.config_manager.get("trino.port")

            self.catalog = self.config_manager.get("trino.catalog")
            self.schema = self.config_manager.get("trino.schema")
            self.schema_folder_gold = self.config_manager.get("trino.schema_folder_gold")

            self.schema_folder = self.config_manager.get("trino.schema_folder")
            self.verify = self.config_manager.get("trino.verify_ssl")

            if self.type_auth == "basic":
                self.user = self.config_manager.get("trino.user")
                self.password = self.config_manager.get("trino.password")
            elif self.type_auth == "oauth2":
                self.base_url = self.config_manager.get("trino.base_url")
                self.client_id = self.config_manager.get("trino.client_id")
                self.client_secret = self.config_manager.get("trino.client_secret")
            elif self.type_auth == "jwt":
                self.user = self.config_manager.get("trino.user")
                self.jwt_token = self.config_manager.get("trino.jwt_token")
            else:
                raise ValueError("Tipo de autenticação inválido. Use 'basic' ou 'oauth2'.")    


            self.conn = self._connect(self.type_auth)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Falha na conexão do Trino {e}")
            raise Exception(e)

    def _connect(self, type_auth):
        if type_auth == "basic":
            return connect(
                host=self.host,
                port=self.port,
                user=self.user,
                auth=BasicAuthentication(self.user, self.password),
                http_scheme=self.HTTPS_CODE,
                catalog=self.catalog,
                verify=self.verify,
                schema=self.schema
            )
        elif type_auth == "oauth2":
            token = self._get_token_oauth2(
                self.base_url,
                self.client_id,
                self.client_secret
            )
            return connect(
                host=self.host,
                port=self.port,
                catalog=self.catalog,
                schema=self.schema,
                http_scheme=self.HTTPS_CODE,
                http_headers={'Authorization': f'Bearer {token}'},
                verify=self.verify,
            )
        elif type_auth == "jwt":
            return connect(
                host=self.host,
                port=self.port,
                user=self.user,
                auth=JWTAuthentication(self.jwt_token),
                http_scheme=self.HTTPS_CODE,
                catalog=self.catalog,
                verify=self.verify,
                schema=self.schema
            )
        else:
            raise ValueError("Tipo de autenticação inválido. Use 'basic' ou 'oauth2'.")

    
    def _get_token_oauth2(self, base_url, client_id, client_secret):
        """
        Obtém um token de acesso OAuth 2.0 do Keycloak usando o fluxo 'client credentials grant'.
        Recomendado para comunicação máquina-a-máquina.
        """
        token_url = f"{base_url}/protocol/openid-connect/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "openid" # Escopo mínimo para client credentials
        }

        print(f"Tentando obter token de: {token_url}")
        try:
            response = requests.post(token_url, headers=headers, data=data, verify=False)
            response.raise_for_status() # Lança exceção para status de erro (4xx ou 5xx)
            token_data = response.json()
            access_token = token_data.get("access_token")
            if not access_token:
                raise ValueError("Token de acesso não encontrado na resposta do Keycloak.")
            print("Token de acesso obtido com sucesso.")
            return access_token
        except requests.exceptions.RequestException as e:
            print(f"Erro ao obter token do Keycloak: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Resposta do Keycloak: {e.response.text}")
            return None        

    def execute_query(self, command, fetch_results=True):
        """
        Executa um comando SQL no Trino.

        command: Comando SQL
        fetch_results: Retorna resultados
        """
        try:
            print(f"Executando comando: {command}")
            self.cursor.execute(command)
            if fetch_results:
                return self.cursor.fetchall()
        except Exception as e:
            logging.error(e)
            sys.exit(1)

    def is_schema(self, schema, catalog=None):
        catalog = catalog or self.catalog
        df = self.execute_query(f'SHOW SCHEMAS FROM "{catalog}" LIKE \'{schema}\'')
        return True if df and len(df) > 0 else False

    def is_table(self, table_name, schema, catalog=None):
        catalog = catalog or self.catalog
        df = self.execute_query(f'SHOW TABLES FROM "{catalog}"."{schema}" LIKE \'{table_name}\'')
        return True if df and len(df) > 0 else False    

    def create_not_exists_schema(self, schema, catalog=None):
        catalog = catalog or self.catalog
        if not self.is_schema(schema, catalog):
            query_name = f'create schema if not exists "{catalog}"."{schema}" with (location = \'{self.config_manager.get("minio.base_path")}/{self.schema_folder}/\')'
            self.execute_query(query_name)
            print(f"Schema {self.schema} criado")

    def create_table(self, table_name, pipeline: str = "silver"):
        if pipeline == "gold":
            minio_path = f"{self.config_manager.get('minio.base_path')}/{self.schema_folder_gold}/{table_name}"
            schema = self.schema
            catalog = self.catalog
        else:
            minio_path = f"{self.config_manager.get("minio.base_path")}/{self.schema_folder}/{table_name}"
            schema = self.schema
            catalog = self.catalog
        try:
            self.create_not_exists_schema(schema, catalog)
            if not self.is_table(table_name, schema, catalog):
                print("Tabela não existe")
                try:
                    registry_table = f"""
                    CALL "{catalog}".system.register_table(
                        schema_name => '{schema}',
                        table_name => '{table_name}',
                        table_location => '{minio_path}'
                    )
                    """
                    self.execute_query(registry_table, fetch_results=True)
                    print(f"Tabela {table_name} criada")
                except Exception as e:
                    if "No transaction log found in location" in str(e):
                        msg = f"[Error] - path={minio_path} não existe para poder criar tabela={table_name}"
                        print(msg)
                    else:
                        raise Exception(e)         
        except Exception as e:
            print(f"Erro ao criar tabela {table_name}: {e}")
            raise Exception(e)

