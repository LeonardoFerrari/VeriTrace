import pandas as pd
from typing import Dict, Any
from .base import BaseIngestion
from ..core.exceptions import IngestionError

class ExcelIngestion(BaseIngestion):
    """Classe responsável pela ingestão via excel"""
    
    def ingest(self, source_path: str, **kwargs) -> Dict[str, Any]:
        """
        Ingestão de dados a partir de um arquivo Excel
        
        Args:
            source_path: Caminho do arquivo Excel a ser lido
            **kwargs: Parâmetros adicionais para a leitura do Excel, como sheet_name, header, etc.
            
        Returns:
            Dicionário com o df lido
        """
        try:
            if not self._validate_source(source_path):
                raise IngestionError(f"Arquivo não encontrado: {source_path}")
            
            self.logger.info(f"Ingestão realizada para o path: {source_path}")
            
            excel_params = {
                'sheet_name': 0,  # Primeira aba por padrão
                **kwargs
            }
            
            df = pd.read_excel(source_path, **excel_params)
            
            result = {
                'dataframe': df,
                'rows': len(df),
                'columns': len(df.columns),
                'source_path': source_path,
                'source_type': 'excel',
                'column_names': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
            }
            
            self.logger.info(f"Excel ingestion completed: {len(df)} rows, {len(df.columns)} columns")
            return result
            
        except Exception as e:
            self.logger.error(f"Excel ingestion failed: {str(e)}")
            raise IngestionError(f"Failed to ingest Excel: {str(e)}")