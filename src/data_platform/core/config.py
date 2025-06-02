# src/data_platform/core/config.py
"""
Configuration management for the Data Reliability Platform
"""
import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path

class Config:
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or os.getenv('CONFIG_FILE', 'config/development.yaml')
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Carrega o arquivo de configuração YAML/retorna a config padrão se não carregar"""
        if not self.config_file:
            return self._get_default_config()
        config_path = Path(self.config_file)
        
        if not config_path.exists():
            return self._get_default_config()
        
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Erro ao carregar o arquivo de configuração: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        return {
            'database': {
                'url': os.getenv('DATABASE_URL', ''),
                'pool_size': 10,
                'max_overflow': 20
            },
            'lakefs': {
                'endpoint': os.getenv('LAKEFS_ENDPOINT', 'http://localhost:8000'),
                'access_key': os.getenv('LAKEFS_ACCESS_KEY', ''),
                'secret_key': os.getenv('LAKEFS_SECRET_KEY', ''),
                'repository': os.getenv('LAKEFS_REPO', '')
            },
            'bigchaindb': {
                'endpoint': os.getenv('BIGCHAINDB_ENDPOINT', 'http://localhost:9984'),
                'app_id': os.getenv('BIGCHAINDB_APP_ID', ''),
                'app_key': os.getenv('BIGCHAINDB_APP_KEY', '')
            },
            'kafka': {
                'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                'group_id': ''
            },
            'atlas': {
                'endpoint': os.getenv('ATLAS_ENDPOINT', 'http://localhost:21000'),
                'username': os.getenv('ATLAS_USERNAME', ''),
                'password': os.getenv('ATLAS_PASSWORD', '')
            },
            'paths': {
                'data_sources': 'data_sources/',
                'staging': 'staging/',
                'processed': 'processed/',
                'logs': 'logs/'
            },
            'validation': {
                'isolation_forest': {
                    'contamination': 0.01,
                    'random_state': 42
                },
                'dbscan': {
                    'eps': 0.5,
                    'min_samples': 5
                }
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Configura chave por chave"""
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value