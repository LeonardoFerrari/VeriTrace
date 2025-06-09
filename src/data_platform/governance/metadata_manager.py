import logging
import json
from pathlib import Path
import datetime
from typing import Dict, Any

from ..core.config import Config

class MetadataManager:
    """
    simula um catálogo de dados para o MVP
    gerencia metadados em um arquivo JSON local
    """
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.catalog_file = Path(self.config.get('paths.metadata_catalog', 'metadata/catalog.json'))
        self.catalog_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"MetadataManager simulado inicializado. Catálogo em '{self.catalog_file}'")

    def register_dataset(self, dataset_path: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Registra ou atualiza os metadados de um dataset no catálogo simulado

        Args:
            dataset_path (str): Identificador único do dataset (caminho).
            metadata (Dict[str, Any]): Dicionário de metadados do dataset.

        Returns:
            Dict[str, Any]: Metadados completos do dataset.
        """
        self.logger.info(f"Registrando metadados para o dataset: {dataset_path}")
        
        catalog: Dict[str, Any] = {}
        if self.catalog_file.exists() and self.catalog_file.stat().st_size > 0:
            with open(self.catalog_file, 'r') as f:
                try:
                    catalog = json.load(f)
                except json.JSONDecodeError:
                    self.logger.warning(f"Catálogo de metadados '{self.catalog_file}' corrompido. Novo catálogo será criado.")

        entry = catalog.get(dataset_path, {})
        entry.update(metadata)
        entry["last_updated"] = datetime.datetime.now().isoformat()
        entry["dataset_path"] = dataset_path
        
        catalog[dataset_path] = entry
        
        try:
            with open(self.catalog_file, 'w') as f:
                json.dump(catalog, f, indent=4)
            self.logger.info(f"Metadados de '{dataset_path}' registrados/atualizados com sucesso.")
            return entry
        except Exception as e:
            self.logger.error(f"Falha ao gravar no catálogo de metadados: {e}", exc_info=True)
            raise
