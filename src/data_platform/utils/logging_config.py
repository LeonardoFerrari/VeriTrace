"""
Configuração de log basica para o projeto
"""
import logging
import logging.config
import os
from pathlib import Path
from typing import Dict, Any


def setup_logging(logging_config: Dict[str, Any] = None):
    """
    Configura o logging do projeto com base na config de entrada
    
    Args:
        logging_config: Opcional. Dicionário com a configuração de logging
    """
    if logging_config is None:
        logging_config = get_default_logging_config()
    
    # Cria diretorio, já que o MVP vai rodar em máquina local, e não na nuvem
    logs_dir = Path(logging_config.get('handlers', {}).get('file', {}).get('filename', 'logs/platform.log')).parent
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    logging.config.dictConfig(logging_config)


def get_default_logging_config() -> Dict[str, Any]:
    """Get default logging configuration"""
    return {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
            'detailed': {
                'format': '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s'
            }
        },
        'handlers': {
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'level': 'DEBUG',
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'detailed',
                'filename': 'logs/platform.log',
                'maxBytes': 10485760,  # 10MB
                'backupCount': 5
            }
        },
        'loggers': {
            '': {  # root 
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False
            }
        }
    }