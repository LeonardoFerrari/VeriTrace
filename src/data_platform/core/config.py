import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging

class Config:
    """
    Carrega e fornece acesso a configuracao do projeto de um arquivo YAML
    Implementa o padrao Singleton para garantir uma unica instancia de configuracao
    """
    _instance = None
    _config: Dict[str, Any] = {}

    def __new__(cls, config_file: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            # Carrega config padrao se nenhum arquivo for fornecido
            if config_file:
                cls._instance.load_config(config_file)
            else:
                default_path = Path(__file__).parent.parent.parent.parent / "config" / "dev.yaml"
                if default_path.exists():
                    cls._instance.load_config(str(default_path))
                else:
                    logging.warning("Arquivo de configuracao padrao 'config/dev.yaml' nao encontrado")
        return cls._instance

    def load_config(self, config_file: str):
        """Carrega ou recarrega configuracao de um arquivo YAML"""
        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {config_file}")
        try:
            with open(config_path, 'r') as f:
                self._config = yaml.safe_load(f)
            logging.info(f"Configuracao carregada com sucesso de {config_file}")
        except yaml.YAMLError as e:
            logging.error(f"Erro ao analisar arquivo YAML {config_file}: {e}")
            raise

    def get(self, key: str, default: Any = None) -> Any:
        """
        Recupera um valor de configuracao usando notacao de ponto para chaves aninhadas
        Exemplo: config.get('paths.staging', '/tmp/staging')
        
        Args:
            key (str): A chave separada por ponto para o valor
            default (Any, optional): Valor retornado se a chave nao for encontrada. Padrao None
            
        Returns:
            Any: O valor da configuracao ou o padrao
        """
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value
