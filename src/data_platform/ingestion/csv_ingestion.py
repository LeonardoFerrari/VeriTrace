"""
Módulo de Ingestão de Dados CSV
"""
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
from ..core.exceptions import IngestionError


class CSVIngestion:
    """Gerencia ingestão de arquivos CSV com validação e tratamento de erros"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.staging_path = Path(config.get('paths.staging', 'staging/'))
        self.staging_path.mkdir(parents=True, exist_ok=True)
    
    def ingest(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """
        Ingere arquivo CSV e converte para formato otimizado
        
        Args:
            file_path: Caminho do arquivo CSV
            **kwargs: Parâmetros adicionais para pandas read_csv
            
        Returns:
            Dicionário com resultados da ingestão
        """
        try:
            self.logger.info(f"Iniciando ingestão CSV: {file_path}")
            
            # Valida se o arquivo existe
            if not Path(file_path).exists():
                raise IngestionError(f"Arquivo CSV não encontrado: {file_path}")
            
            # Lê o CSV com tratamento de erro
            try:
                df = pd.read_csv(file_path, **kwargs)
            except Exception as e:
                raise IngestionError(f"Falha ao ler arquivo CSV: {str(e)}")
            
            # Validação básica
            if df.empty:
                raise IngestionError("Arquivo CSV está vazio")
            
            # Gera caminho de saída
            input_name = Path(file_path).stem
            output_path = self.staging_path / f"{input_name}.parquet"
            
            # Salva como Parquet para melhor performance
            df.to_parquet(output_path, index=False)
            
            # Calcula estatísticas básicas
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
            
            self.logger.info(f"Ingestão CSV concluída: {len(df)} linhas, {len(df.columns)} colunas")
            return result
            
        except Exception as e:
            self.logger.error(f"Ingestão CSV falhou: {str(e)}")
            raise IngestionError(f"Ingestão CSV falhou: {str(e)}")
    
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