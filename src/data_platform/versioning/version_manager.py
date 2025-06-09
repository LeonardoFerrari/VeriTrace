import logging
import datetime
import hashlib
from pathlib import Path
from typing import Dict, Any

from ..core.config import Config
from ..core.exceptions import VersioningError

class VersionManager:
    """
    Simula versionamento de dados tipo LakeFS pro MVP
    No real usaria o cliente da API do LakeFS
    """
    def __init__(self, config: Config):
        """Inicia o version manager simulado"""
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.repository = self.config.get('versioning.lakefs.repository', 'veritrace-repo')
        self.logger.info(f"VersionManager simulado iniciado pro repo {self.repository}")

    def commit_data(self, data_path: str, message: str, branch: str = 'main') -> Dict[str, Any]:
        """
        Simula commit de arquivo ou pasta no repo de dados
        Gera um commit id fake pra demo

        Args:
            data_path (str): Caminho do dado pra commitar
            message (str): Mensagem do commit
            branch (str): Branch pra commitar

        Returns:
            Dict[str, Any]: Detalhes do commit simulado
        """
        self.logger.info(f"Tentando simular commit pro caminho {data_path}")
        try:
            path = Path(data_path)
            if not path.exists():
                raise VersioningError(f"Caminho de dado nao existe {data_path}")

            # Gera commit id fake baseado no caminho mensagem e timestamp
            timestamp = datetime.datetime.now().isoformat()
            commit_content = f"{data_path}{message}{timestamp}".encode()
            commit_hash = hashlib.sha1(commit_content).hexdigest()
            commit_id = commit_hash[:12] # Hash curto pra ficar legivel

            self.logger.info(f"SIMULANDO commit LakeFS na branch {branch} pro repo {self.repository}")
            self.logger.info(f"Commit {commit_id} {message}")

            return {
                "commit_id": commit_id,
                "branch": branch,
                "message": message,
                "timestamp": timestamp,
                "repository": self.repository,
                "status": "✅ Completo Simulado"
            }
        except Exception as e:
            self.logger.error(f"Falha ao simular commit pra {data_path} {e}", exc_info=True)
            raise VersioningError(f"Falha ao simular commit pra {data_path} {e}")
