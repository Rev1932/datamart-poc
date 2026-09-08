import logging
import os
from pyhocon import ConfigFactory, ConfigTree
#from utils.handler_logger import initialize_logger

#logger = initialize_logger()

class ConfigManager:
    """
    Gerencia configurações de aplicativos usando o formato HOCON.
    Carrega configurações de um arquivo especificado ou de variáveis ​​de ambiente,
    com suporte para arquivos de configuração específicos do ambiente.
    """

    def __init__(self, config_base_path: str = None, file_name: str = "application", env: str = None, unique_env_name: bool = False):
        """
        Inicializa o gerenciador de configurações.

        Args:
            config_base_path (str): Caminho base opcional. Se None, busca a pasta 'resources' na raiz.
            file_name (str): Nome base do arquivo de configuração (padrão: "application").
            env (str): Ambiente opcional. Se None, usa a variável de ambiente ENV ou 'local'.
            unique_env_name (bool): Se o nome do arquivo de configuração deve ignorar o sufixo do ambiente.
        """
        # Define o ambiente (prioridade: argumento > variável ENV > 'local')
        self.env = (env or os.getenv("ENV", "local")).lower()

        # Localiza a pasta resources automaticamente se não for informada
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
                # Fallback ou erro caso não encontre
                raise Exception("Não foi possível localizar a pasta 'resources' dinamicamente.")

        self.config_base_path = config_base_path.rstrip('/')
        self.file_name = file_name
        self.unique_env_name = unique_env_name
        self.config = None
        self._load_config()

    def _load_config(self):
        """
        Carrega a configuração do arquivo específico do ambiente especificado.
        Em seguida, sobrepõe as variáveis ​​de ambiente.
        """
        if not self.unique_env_name:
            file_name = f"{self.file_name}-{self.env}.conf"
        else:
            file_name = f"{self.file_name}.conf"    
        
        env_config_file = os.path.join(self.config_base_path, file_name)    
        # Comece com uma configuração vazia
        self.config = ConfigFactory.from_dict({})

        # Carregue primeiro a configuração específica do ambiente
        if os.path.exists(env_config_file):
            self.config = self.config.with_fallback(ConfigFactory.parse_file(env_config_file))
        else:
            #logger.error(f"Não foi encontrado arquivo de configuração, cria arquivo em resource/{file_name}")
            raise Exception(f"Não foi encontrado arquivo de configuração, cria arquivo em resource/{file_name}")
   
    def get(self, key: str, default=None):
        """
        Recupera um valor de configuração por chave.
        """
        try:
            return self.config.get(key, default)
        except Exception:
            #logger.error(f"Chave de configuração '{key}' não encontrada, retornando valor padrão: {default}")
            return default

    def get_config_tree(self):
        """
        Retorna toda a configuração como um objeto ConfigTree.
        """
        return self.config
