from minio import Minio
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject
from urllib.parse import urlparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import threading

from utils.handler_logger import initialize_logger

# Cache de instancias por credencial. No modo multi-tabela cada repositorio instanciava o seu
# proprio MinioAdmin — com dezenas de tabelas por pod isso virava dezenas de pools HTTP
# abertos em paralelo, sem ganho nenhum (o client e thread-safe e ja faz pooling internamente).
_INSTANCIAS: dict = {}
_LOCK_INSTANCIAS = threading.Lock()


class MinioAdmin:
    def __init__(self, minio_config:dict , secure=True, max_move_workers: int = 8):
        """
        endpoint: 'localhost:9000' ou 'minio:9000' (sem http://)
        secure: False para HTTP, True para HTTPS
        max_move_workers: paralelismo das movimentacoes de objeto (copy+remove)
        """
        self.logger = initialize_logger()
        self.max_move_workers = max(1, int(max_move_workers))
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

    @classmethod
    def shared(cls, minio_config: dict, **kwargs):
        """Devolve a instancia compartilhada para estas credenciais, criando-a se preciso.

        Usar isto em vez de `MinioAdmin(...)` sempre que a chamada estiver num caminho
        executado uma vez por tabela.
        """
        chave = (minio_config.get("endpoint"), minio_config.get("access_key"))
        with _LOCK_INSTANCIAS:
            if chave not in _INSTANCIAS:
                _INSTANCIAS[chave] = cls(minio_config, **kwargs)
            return _INSTANCIAS[chave]

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


    def _mover_para_processed(self, file_url):
        """Move um objeto para `<pasta>/processed/<arquivo>.bkp` (copy + remove)."""
        bucket_name, old_key = self.parse_s3_url(file_url)

        path_parts = old_key.split('/')
        file_name = path_parts[-1]
        folder_path = "/".join(path_parts[:-1])

        new_key = f"{folder_path}/processed/{file_name}.bkp"

        self.logger.debug(f"Movendo arquivo do MinIO: {old_key} -> {new_key}")

        self.client.copy_object(
            bucket_name,    # Bucket de destino
            new_key,        # Nome do objeto de destino
            CopySource(bucket_name, old_key) # Fonte
        )

        self.client.remove_object(
            bucket_name,
            old_key
        )

    def cleaning_processed(self, processed_tables):
        """
        Move os arquivos ja processados para `processed/`, em paralelo.

        Sao chamadas de rede puras (copy + remove por objeto), sem trabalho de CPU: feitas em
        serie, uma ingestao com centenas de arquivos passava a maior parte do tempo do driver
        esperando round-trip do MinIO. O client do minio-py e thread-safe (urllib3.PoolManager
        interno), entao um pool pequeno resolve.
        """
        processed_tables = list(processed_tables)
        if not processed_tables:
            return

        falhas = []
        workers = min(self.max_move_workers, len(processed_tables))
        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix="minio-move") as pool:
            futuros = {
                pool.submit(self._mover_para_processed, url): url
                for url in processed_tables
            }
            for futuro in as_completed(futuros):
                url = futuros[futuro]
                try:
                    futuro.result()
                except Exception as e:
                    self.logger.error(f"Erro ao mover {url}: {e}")
                    falhas.append(url)

        if falhas:
            # Levanta em vez de so `print()` dentro do loop, como era antes: agora a falha vira
            # um registro de log estruturado (e chega ao ClickHouse) em vez de sumir no stdout.
            #
            # ATENCAO ao alcance real: `RepositoryBronzeToSilver.cleaning_processed` ainda
            # captura esta excecao e apenas loga como ERROR, entao a TABELA continua sendo
            # reportada como sucesso. Isso e deliberado — a escrita ja concluiu e o arquivo nao
            # movido so sera reprocessado na proxima rajada (o merge e idempotente por
            # hk_business_id, entao nao ha duplicacao de dado, so trabalho repetido). Para que a
            # tabela falhe de fato, o try/except de la precisa sair.
            raise RuntimeError(
                f"Falha ao mover {len(falhas)}/{len(processed_tables)} arquivo(s) "
                f"para processed/: {falhas[:5]}{' ...' if len(falhas) > 5 else ''}"
            )

        self.logger.debug(f"{len(processed_tables)} arquivo(s) movidos para processed/.")

    def restore_backups(self, backup_tables_path, from_date: datetime = None, to_date: datetime = None):
        """
        Reverte o processo da função 'cleaning_processed':
        detecta todos os arquivos .bkp, retira o sufixo, e move 1 diretório acima para processar novamente.

        Parâmetros de filtro por data (last_modified do objeto no MinIO):
            from_date: restaura apenas arquivos modificados a partir desta data (inclusive).
            to_date:   restaura apenas arquivos modificados até esta data (inclusive).
            Ambos aceitam datetime com ou sem timezone; sem timezone são tratados como UTC.
        """
        def _as_utc(dt: datetime) -> datetime:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt

        from_date_utc = _as_utc(from_date) if from_date else None
        to_date_utc   = _as_utc(to_date)   if to_date   else None

        backup_tables_list = self.list_path_files(backup_tables_path)
        bucket_name, old_key = self.parse_s3_url(backup_tables_path)

        for obj in backup_tables_list:
            file_name = obj.object_name

            # Filtro por last_modified
            last_modified = obj.last_modified
            if last_modified is not None:
                if from_date_utc and last_modified < from_date_utc:
                    print(f"Ignorando {file_name}: last_modified {last_modified} anterior a from_date {from_date_utc}")
                    continue
                if to_date_utc and last_modified > to_date_utc:
                    print(f"Ignorando {file_name}: last_modified {last_modified} posterior a to_date {to_date_utc}")
                    continue

            try:
                # Lógica de String para reverter o caminho
                path_parts = file_name.split('/')

                # Pega o nome do arquivo atual
                current_file_name = path_parts[-1]

                # Remove o .bkp do final, se existir
                if current_file_name.endswith('.bkp'):
                    new_file_name = current_file_name[:-4]
                else:
                    new_file_name = current_file_name  # Caso não tenha .bkp, mantém

                parent_folder_parts = path_parts[:-2]

                if parent_folder_parts:
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