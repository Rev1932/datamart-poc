from minio import Minio
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject
from urllib.parse import urlparse
import sys

class MinioAdmin:
    def __init__(self, minio_config:dict , secure=True):
        """
        endpoint: 'localhost:9000' ou 'minio:9000' (sem http://)
        secure: False para HTTP, True para HTTPS
        """
        self.minio_endpoint = minio_config.get("endpoint")
        self.minio_access_key = minio_config.get("access_key")
        self.minio_secret_key = minio_config.get("secret_key")

        parsed = urlparse(self.minio_endpoint)
        self.minio_endpoint = parsed.netloc

        self.client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=secure
        )

    @staticmethod
    def parse_s3_url(s3_url):
        """
        Transforma 's3a://bucket/pasta/arquivo.parquet' 
        em bucket='bucket' e object_name='pasta/arquivo.parquet'
        """
        parsed = urlparse(s3_url)
        bucket_name = parsed.netloc
        # Remove a barra inicial '/' que o urlparse deixa no path
        object_name = parsed.path.lstrip('/')
        return bucket_name, object_name
    
    def list_path_files(self, path_name):
        bucket_name, path = self.parse_s3_url(path_name)
        path = f"{path}/"
        object_list = self.client.list_objects(
            bucket_name, prefix=path, recursive=False
        )
        return object_list


    def cleaning_processed(self, processed_tables):
        for file_url in processed_tables:
            try:
                # 1. Parse do caminho (igual ao anterior)
                bucket_name, old_key = self.parse_s3_url(file_url)
                
                # 2. Lógica de String para definir novo caminho (igual ao anterior)
                path_parts = old_key.split('/')
                file_name = path_parts[-1]
                folder_path = "/".join(path_parts[:-1])
                
                new_key = f"{folder_path}/processed/{file_name}.bkp"
                
                print(f"Movendo arquivos do Minio : {old_key} -> {new_key}")

                self.client.copy_object(
                    bucket_name,    # Bucket de destino
                    new_key,        # Nome do objeto de destino
                    CopySource(bucket_name, old_key) # Fonte
                )

                self.client.remove_object(
                    bucket_name, 
                    old_key
                )

            except Exception as e:
                print(f"Erro ao processar {file_url}: {e}")
        
    def restore_backups(self, backup_tables_path): #TODO aqui tem que ser apenas o path da tabela que precisa ser reprocessada, para rodar uma busca recursiva pra pegar todos os arquivos com bkp
        """
        Reverte o processo da função 'cleaning_processed'
        detecta todos os arquivos .bkp, retira o sulfixo, e move 1 diretorio acima para processar novamente.
        """
        backup_tables_list = self.list_path_files(backup_tables_path)
        bucket_name, old_key = self.parse_s3_url(backup_tables_path)
        for obj in backup_tables_list:
            file_name = obj.object_name
            try:                
                #Lógica de String para reverter o caminho
                path_parts = file_name.split('/')
                
                #Pega o nome do arquivo atual
                current_file_name = path_parts[-1]
                
                # Remove o .bkp do final, se existir
                if current_file_name.endswith('.bkp'):
                    new_file_name = current_file_name[:-4]
                else:
                    new_file_name = current_file_name # Caso não tenha .bkp, mantér

                parent_folder_parts = path_parts[:-2]
                
                if parent_folder_parts:
                    new_folder_path = ""
                    new_folder_path = "/".join(parent_folder_parts)
                    new_key = f"{new_folder_path}/{new_file_name}"
                else:
                    # Caso o arquivo estivesse na raiz de uma pasta processada na raiz do bucket
                    new_key = new_file_name
                old_path = f"{old_key}/{new_file_name}.bkp"
                print(f"Movendo arquivos do Minio: {old_path} -> {new_key}")

                self.client.copy_object(
                    bucket_name,
                    new_key,
                    CopySource(bucket_name, old_path)
                )

                self.client.remove_object(
                    bucket_name, 
                    old_path
                )
            except Exception as e:
                print(f"Erro ao restaurar {file_name}: {e}")
                raise
    
    def cleaning_temp_oee(self):
        bucket_name = "datawake-unipac"
        temp_oee_list = [
            "service_data_data-bee/dim_turnos_expandidos",
            "service_data_data-bee/dim_hierarquia_completa",
            "service_data_data-bee/dim_op_unidade_periodo",
            "service_data_data-bee/dim_producao_agregada",
            "service_data_data-bee/dim_paradas_agregadas",
            "service_data_data-bee/dim_producao_durante_paradas",
            "service_data_data-bee/dim_quantidades_consolidadas",
            "service_data_data-bee/dim_colaborador_logados",
            "service_data_data-bee/dim_base_quebras_colaborador",
            "service_data_data-bee/dim_producao_colaborador",
            "service_data_data-bee/paradas_colaborador",
            "service_data_data-bee/dim_producao_durante_paradas_colaborador",
            "service_data_data-bee/dim_quantidades_consolidadas_colaborador",
        ]

        for path in temp_oee_list:
            try:
                objects_to_delete = self.client.list_objects(
                    bucket_name, prefix=path, recursive=True
                )
                delete_list = [DeleteObject(obj.object_name) for obj in objects_to_delete]
                if delete_list:
                    errors = self.client.remove_objects(bucket_name, delete_list)
                    error_list = list(errors)
                    if not error_list:
                        print(f"Arquivos temporários em '{path}' limpos com sucesso.")
                    else:
                        print(f"Ocorreram erros ao deletar itens em '{path}': {error_list}")
                else:
                    print(f"Nenhum arquivo encontrado no caminho '{path}'.")

            except Exception as e:
                print(f"Erro crítico ao processar o caminho {path}: {str(e)}")