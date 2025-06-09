import logging
import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, Optional
from ..core.exceptions import IngestionError


class JSONIngestion:
    """Ingestão json com validação e tratamento de erros"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.staging_path = Path(config.get('paths.staging', 'staging/'))
        self.staging_path.mkdir(parents=True, exist_ok=True)
    
    def ingest(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """
        Ingestao e conversão de arquivo JSON para df
        
        Args:
            file_path: Caminho do arquivo JSON
            **kwargs: Parâmetros adicionais para leitura do JSON
            
        Returns:
            Dicionário com resultados da ingestão
        """
        try:
            self.logger.info(f"Starting JSON ingestion: {file_path}")
            
            if not Path(file_path).exists():
                raise IngestionError(f"JSON file not found: {file_path}")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
            except json.JSONDecodeError as e:
                raise IngestionError(f"Invalid JSON format: {str(e)}")
            except Exception as e:
                raise IngestionError(f"Failed to read JSON file: {str(e)}")
            
            # convert json pra df
            try:
                if isinstance(json_data, list):
                    df = pd.DataFrame(json_data)
                elif isinstance(json_data, dict):
                    if 'data' in json_data:
                        df = pd.DataFrame(json_data['data'])
                    else:
                        df = pd.DataFrame([json_data])
                else:
                    raise IngestionError("Unsupported JSON structure")
            except Exception as e:
                raise IngestionError(f"Failed to convert JSON to DataFrame: {str(e)}")
            
            if df.empty:
                raise IngestionError("JSON file resulted in empty DataFrame")
            
            input_name = Path(file_path).stem
            output_path = self.staging_path / f"{input_name}.parquet"
            
            # Em parquet pra melhor performance
            df.to_parquet(output_path, index=False)
            
            stats = self._calculate_stats(df)
            
            result = {
                'source_file': file_path,
                'output_file': str(output_path),
                'dataframe': df,
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': list(df.columns),
                'data_types': df.dtypes.to_dict(),
                'statistics': stats,
                'file_size_mb': Path(file_path).stat().st_size / (1024 * 1024)
            }
            
            self.logger.info(f"JSON ingestion completed: {len(df)} rows, {len(df.columns)} columns")
            return result
            
        except Exception as e:
            self.logger.error(f"JSON ingestion failed: {str(e)}")
            raise IngestionError(f"JSON ingestion failed: {str(e)}")
    
    def _calculate_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calcula estatísticas básicas do dataframe"""
        numeric_cols = df.select_dtypes(include=['number']).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        stats = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'numeric_columns': len(numeric_cols),
            'categorical_columns': len(categorical_cols),
            'missing_values': df.isnull().sum().to_dict(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
        }
        
        if len(numeric_cols) > 0:
            stats['numeric_summary'] = df[numeric_cols].describe().to_dict()
        
        return stats