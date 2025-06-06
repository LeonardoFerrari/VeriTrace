import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional

class DataQualityValidator:
    """Data quality validações e checagens"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def validate(self, df: pd.DataFrame, config: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Realiza a validação de qualidade dos dados
        
        Args:
            df: Dataframe a ser validado
            config: Configuração opcional
            
        Returns:
            Dicionário com os resultados da validação
        """
        try:
            self.logger.info("Starting data quality validation")
            
            issues = []
            
            missing_issues = self._check_missing_values(df)
            issues.extend(missing_issues)
            
            duplicate_issues = self._check_duplicates(df)
            issues.extend(duplicate_issues)
            
            dtype_issues = self._check_data_types(df)
            issues.extend(dtype_issues)
            
            # z-score 
            outlier_issues = self._check_outliers(df)
            issues.extend(outlier_issues)
            
            summary = self._generate_summary(df, issues)
            
            result = {
                'issues': issues,
                'summary': summary,
                'total_issues': len(issues)
            }
            
            self.logger.info(f"Data quality validation completed. Found {len(issues)} issues")
            return result
            
        except Exception as e:
            self.logger.error(f"Data quality validation failed: {str(e)}")
            raise
    def _check_missing_values(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """valores ausentes"""
        self.logger.info("iniciando verificação de valores ausentes")
        issues = []
        
        for column in df.columns:
            self.logger.debug(f"analisando coluna: {column}")
            missing_count = df[column].isnull().sum()
            missing_percentage = (missing_count / len(df)) * 100
            
            if missing_count > 0:
                issues.append({
                    'type': 'missing_values',
                    'column': column,
                    'count': int(missing_count),
                    'percentage': float(missing_percentage),
                    'severity': 'high' if missing_percentage > 50 else 'medium' if missing_percentage > 10 else 'low'
                })
        
        return issues

    def _check_duplicates(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """duplicate"""
        self.logger.info("iniciando verificação de linhas duplicadas")
        issues = []
        
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            duplicate_percentage = (duplicate_count / len(df)) * 100
            issues.append({
                'type': 'duplicate_rows',
                'column': 'all_columns',
                'count': int(duplicate_count),
                'percentage': float(duplicate_percentage),
                'severity': 'medium' if duplicate_percentage > 5 else 'low'
            })
        
        return issues

    def _check_data_types(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Erro de tipo de dado"""
        self.logger.info("iniciando verificação de tipos de dados")
        issues = []
        
        for column in df.columns:
            if df[column].dtype == 'object':
                # Check if numeric values are stored as strings
                try:
                    pd.to_numeric(df[column], errors='raise')
                    issues.append({
                        'type': 'data_type_mismatch',
                        'column': column,
                        'current_type': 'object',
                        'suggested_type': 'numeric',
                        'severity': 'medium'
                    })
                except:
                    pass
        
        return issues

    def _check_outliers(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Outlier usando z-score"""
        self.logger.info("iniciando detecção de outliers com z-score")
        issues = []
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        for column in numeric_columns:
            if df[column].std() > 0:
                z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
                outlier_count = (z_scores > 3).sum()  # |z-score| > 3
                
                if outlier_count > 0:
                    outlier_percentage = (outlier_count / len(df)) * 100
                    issues.append({
                        'type': 'outliers',
                        'column': column,
                        'count': int(outlier_count),
                        'percentage': float(outlier_percentage),
                        'method': 'z_score',
                        'threshold': 3,
                        'severity': 'medium' if outlier_percentage > 5 else 'low'
                    })
        
        return issues

    def _generate_summary(self, df: pd.DataFrame, issues: List[Dict]) -> Dict[str, Any]:
        """Gera um resumo da validação de qualidade dos dados"""
        self.logger.info("gerando resumo da validação")
        return {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'total_issues': len(issues),
            'issue_types': list(set(issue['type'] for issue in issues)),
            'columns_with_issues': list(set(issue['column'] for issue in issues if issue['column'] != 'all_columns')),
            'severity_counts': {
                'high': len([i for i in issues if i.get('severity') == 'high']),
                'medium': len([i for i in issues if i.get('severity') == 'medium']),
                'low': len([i for i in issues if i.get('severity') == 'low'])
            }
        }
