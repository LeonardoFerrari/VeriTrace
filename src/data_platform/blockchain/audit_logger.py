import logging
import json
import hashlib
from pathlib import Path
import datetime
from typing import Dict, Any, List

from ..core.config import Config
from ..core.exceptions import BlockchainError

class AuditLogger:
    """
    Simula o registro de uma transação de auditoria em blockchain.
    Para o MVP, adiciona a transação a um arquivo JSON local
    """
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.log_file = Path(self.config.get('paths.blockchain_log', 'blockchain/audit_log.json'))
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"AuditLogger simulado inicializado. Registrando em '{self.log_file}'")

    def log_transaction(self, operacao: str, hash_dados: str, metadados: Dict[str, Any]) -> Dict[str, Any]:
        """
        Simula a criação e registro de uma transação na auditoria

        Args:
            operacao (str): Descrição da operação auditada ex: 'ingestao', 'validacao'
            hash_dados (str): Hash SHA-256 do dado envolvido
            metadados (Dict[str, Any]): Metadados adicionais

        Returns:
            Dict[str, Any]: Registro da transação criada
        """
        self.logger.info(f"Tentando registrar transação de auditoria para operação: {operacao}")
        try:
            timestamp = datetime.datetime.now().isoformat()
            tx_content = f"{hash_dados}{timestamp}{operacao}".encode()
            transaction_id = hashlib.sha256(tx_content).hexdigest()

            transacao = {
                "transaction_id": transaction_id,
                "timestamp": timestamp,
                "operation": operacao,
                "asset": {
                    "data": {
                        "content_hash": hash_dados,
                        "hash_algorithm": "SHA-256",
                        "author": "VeriTracePlatform",
                        **metadados
                    }
                },
                "status": "✅ Concluída (Simulada)"
            }

            registros: List[Dict] = []
            if self.log_file.exists() and self.log_file.stat().st_size > 0:
                with open(self.log_file, 'r') as f:
                    try:
                        registros = json.load(f)
                    except json.JSONDecodeError:
                        self.logger.warning(f"Arquivo de log '{self.log_file}' corrompido. Iniciando novo log.")
            
            registros.append(transacao)
            with open(self.log_file, 'w') as f:
                json.dump(registros, f, indent=4)
                
            self.logger.info(f"Transação '{transaction_id}' registrada com sucesso em '{self.log_file}'")
            return transacao
            
        except Exception as e:
            self.logger.error(f"Falha ao simular transação blockchain: {e}", exc_info=True)
            raise BlockchainError(f"Falha ao registrar transação: {e}")
