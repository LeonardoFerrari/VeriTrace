import pandas as pd
import json
from typing import Dict, Any
from .base import BaseIngestion
from ..core.exceptions import IngestionError

class JSONIngestion(BaseIngestion):
    """classe que faz a leitura de arquivos JSON"""
    
    def ingest(self, source_path: str, **kwargs) -> Dict[str, Any]:
        """
        faz a leitura do arquivo JSON
        
        Args:
            source_path: caminho do arquivo JSON
            **kwargs: parâmetros adicionais do pandas read_json
            
        Returns:
            dicionário com os resultados da leitura
        """
        try:
            self.logger.debug(f"iniciando validação do arquivo: {source_path}")
            if not self._validate_source(source_path):
                self.logger.error(f"arquivo não encontrado: {source_path}")
                raise IngestionError(f"arquivo não encontrado: {source_path}")
            
            self.logger.info(f"iniciando ingestão do arquivo JSON: {source_path}")
            
            # tenta ler primeiro como dataframe do pandas
            try:
                df = pd.read_json(source_path, **kwargs)
            except:
                # se falhar, tenta carregar como json puro e converter
                with open(source_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                elif isinstance(data, dict):
                    df = pd.DataFrame([data])
                else:
                    raise IngestionError("estrutura JSON não suportada")
            
            result = {
                'dataframe': df,
                'rows': len(df),
                'columns': len(df.columns),
                'source_path': source_path,
                'source_type': 'json',
                'column_names': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
            }
            
            self.logger.info(f"JSON ingestion completed: {len(df)} rows, {len(df.columns)} columns")
            return result
            
        except Exception as e:
            self.logger.error(f"JSON ingestion failed: {str(e)}")
            raise IngestionError(f"Failed to ingest JSON: {str(e)}")