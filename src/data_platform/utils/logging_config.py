"""
Configuração de log basica para o projeto
"""
import logging
import sys
from typing import Dict, Any

def setup_logging(config: Dict[str, Any] = None):
    if config is None:
        config = {}
    
    level = config.get('level', 'INFO')
    format_str = config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_str,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )