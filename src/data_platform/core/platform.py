import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
import pandas as pd

from .config import Config
from .exceptions import PlatformError, ValidationError, IngestionError
from ..ingestion.csv_ingestion import CSVIngestion
from ..ingestion.json_ingestion import JSONIngestion  
from ..ingestion.excel_ingestion import ExcelIngestion
from ..validation.anomaly_detection import AnomalyDetector
from ..validation.data_quality import DataQualityValidator
from ..versioning.version_manager import VersionManager
from ..blockchain.audit_logger import AuditLogger
from ..governance.metadata_manager import MetadataManager
from ..utils.logging_config import setup_logging

class DataReliabilityPlatform:
    """
    Classe principal da plataforma que coordena todas as operações 
    de confiabilidade dos dados de forma integrada
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """Inicia a plataforma com as configurações necessárias"""
        self.config = Config(config_file)
        setup_logging(self.config.get('logging', {}))
        self.logger = logging.getLogger(__name__)
        
        # Inicializa os componentes
        self._init_components()
        
        self.logger.info("Data Reliability Platform initialized successfully")
    
    def _init_components(self):
        """Inicializa todos os componentes necessários da plataforma"""
        try:
            # Componentes de ingestão de dados
            self.csv_ingestion = CSVIngestion(self.config)
            self.json_ingestion = JSONIngestion(self.config)
            self.excel_ingestion = ExcelIngestion(self.config)
            
            # Componentes de validação
            self.anomaly_detector = AnomalyDetector(self.config)
            self.quality_validator = DataQualityValidator(self.config)
            
            # Controle de versão
            self.version_manager = VersionManager(self.config)
            
            # Auditoria em blockchain
            self.audit_logger = AuditLogger(self.config)
            
            # Governança de dados
            self.metadata_manager = MetadataManager(self.config)
            
        except Exception as e:
            raise PlatformError(f"Falha ao inicializar componentes da plataforma: {str(e)}")
    
    def ingest_data(self, source_path: str, source_type: str = 'auto', **kwargs) -> Dict[str, Any]:
        """
        Realiza a ingestão de dados de diferentes fontes
        
        Args:
            source_path: Caminho para a fonte de dados
            source_type: Tipo da fonte ('csv', 'json', 'excel', 'auto')
            **kwargs: Parâmetros adicionais para a ingestão
            
        Returns:
            Dicionário com os resultados da ingestão
        """
        try:
            self.logger.info(f"Starting data ingestion from {source_path}")
            
            if source_type == 'auto':
                source_type = self._detect_source_type(source_path)
            if source_type == 'csv':
                result = self.csv_ingestion.ingest(source_path, **kwargs)
            elif source_type == 'json':
                result = self.json_ingestion.ingest(source_path, **kwargs)
            elif source_type == 'excel':
                result = self.excel_ingestion.ingest(source_path, **kwargs)
            else:
                raise IngestionError(f"Unsupported source type: {source_type}")
            
            self.logger.info(f"Data ingestion completed. Processed {result.get('rows', 0)} rows")
            return result
            
        except Exception as e:
            self.logger.error(f"Data ingestion failed: {str(e)}")
            raise IngestionError(f"Ingestion failed: {str(e)}")
    
    def validate_data(self, df: pd.DataFrame, validation_config: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Valida os dados usando IA e verificações de qualidade
        
        Args:
            df: dataframe a ser validado
            validation_config: configuração personalizada de validação
            
        Returns:
            dict com os resultados da validação
        """
        try:
            self.logger.info("Starting data validation")
            
            results = {
                'anomalies': {},
                'quality_issues': {},
                'summary': {}
            }
            
            anomaly_results = self.anomaly_detector.detect_anomalies(df, validation_config)
            results['anomalies'] = anomaly_results
            
            quality_results = self.quality_validator.validate(df, validation_config)
            results['quality_issues'] = quality_results
            
            total_anomalies = sum(anomaly_results.get('anomaly_counts', {}).values())
            total_quality_issues = len(quality_results.get('issues', []))
            
            results['summary'] = {
                'total_rows': len(df),
                'total_anomalies': total_anomalies,
                'total_quality_issues': total_quality_issues,
                'anomaly_rate': total_anomalies / len(df) if len(df) > 0 else 0,
                'validation_passed': total_anomalies == 0 and total_quality_issues == 0
            }
            
            self.logger.info(f"Validation completed. Found {total_anomalies} anomalies and {total_quality_issues} quality issues")
            return results
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            raise ValidationError(f"Validation failed: {str(e)}")
    
    def version_data(self, data_path: str, message: str, branch: str = 'main') -> Dict[str, Any]:
        """
        Controle de versão para os dados usando LakeFS
        
        Args:
            data_path: caminho dos dados a serem versionados
            message: mensagem commit
            branch: Nome da branch Git
            
        Returns:
            Dicionário com os resultados do versionamento
        """
        try:
            self.logger.info(f"Creating data version for {data_path}")
            
            result = self.version_manager.commit_data(data_path, message, branch)
            
            self.logger.info(f"Data versioned successfully. Commit: {result.get('commit_id')}")
            return result
            
        except Exception as e:
            self.logger.error(f"Data versioning failed: {str(e)}")
            raise PlatformError(f"Versioning failed: {str(e)}")
    
    def audit_transaction(self, operation: str, data_hash: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Registra uma transação no blockchain para auditoria
        
        Args:
            operation: Tipo da operação (ingestão, validação, transformação, etc.)
            data_hash: Hash SHA-256 dos dados
            metadata: Metadados adicionais da transação
            
        Returns:
            Dicionário com os detalhes da transação na chain
        """
        try:
            self.logger.info(f"Logging audit transaction for operation: {operation}")
            
            result = self.audit_logger.log_transaction(operation, data_hash, metadata)
            
            self.logger.info(f"Audit transaction logged. Transaction ID: {result.get('transaction_id')}")
            return result
            
        except Exception as e:
            self.logger.error(f"Audit logging failed: {str(e)}")
            raise PlatformError(f"Audit logging failed: {str(e)}")
    
    def full_pipeline(self, source_path: str, output_path: str, **kwargs) -> Dict[str, Any]:
        """
        Executa o pipeline completo de confiabilidade de dados
        
        Args:
            source_path: Caminho para os dados de entrada
            output_path: Caminho para saída processada
            **kwargs: Parâmetros adicionais de configuração
            
        Returns:
            Dicionário com os resultados completos do pipeline
        """
        try:
            self.logger.info(f"Starting full pipeline: {source_path} -> {output_path}")
            
            pipeline_results = {
                'ingestion': {},
                'validation': {},
                'versioning': {},
                'audit': {},
                'metadata': {}
            }
            
            # Ingestão dos dados
            ingestion_result = self.ingest_data(source_path, **kwargs)
            pipeline_results['ingestion'] = ingestion_result
            df = ingestion_result['dataframe']
            
            # Validação dos dados
            validation_result = self.validate_data(df, kwargs.get('validation_config'))
            pipeline_results['validation'] = validation_result
            
            # Salva os dados processados
            processed_df = df.copy()
            if 'anomalies' in validation_result and 'anomaly_flags' in validation_result['anomalies']:
                processed_df = pd.concat([processed_df, validation_result['anomalies']['anomaly_flags']], axis=1)
            
            processed_df.to_parquet(output_path, index=False)
            
            # Versiona os dados
            version_result = self.version_data(
                output_path, 
                f"Processed data from {Path(source_path).name}",
                kwargs.get('branch', 'main')
            )
            pipeline_results['versioning'] = version_result
            
            # Gera hash dos dados e registra auditoria
            data_hash = self._generate_data_hash(output_path)
            audit_metadata = {
                'source_file': source_path,
                'output_file': output_path,
                'rows_processed': len(df),
                'validation_passed': validation_result['summary']['validation_passed'],
                'commit_id': version_result.get('commit_id')
            }
            
            audit_result = self.audit_transaction('full_pipeline', data_hash, audit_metadata)
            pipeline_results['audit'] = audit_result
            
            # Atualiza o catálogo de metadados
            metadata_result = self.metadata_manager.register_dataset(
                output_path,
                {
                    'source': source_path,
                    'pipeline_run': audit_result.get('transaction_id'),
                    'validation_summary': validation_result['summary']
                }
            )
            pipeline_results['metadata'] = metadata_result
            
            self.logger.info("Full pipeline completed successfully")
            return pipeline_results
            
        except Exception as e:
            self.logger.error(f"Full pipeline failed: {str(e)}")
            raise PlatformError(f"Pipeline execution failed: {str(e)}")
    
    def _detect_source_type(self, source_path: str) -> str:
        """Detecta automaticamente o tipo da fonte com base na extensão do arquivo"""
        path = Path(source_path)
        extension = path.suffix.lower()
        
        if extension == '.csv':
            return 'csv'
        elif extension == '.json':
            return 'json'
        elif extension in ['.xlsx', '.xls']:
            return 'excel'
        else:
            raise IngestionError(f"Unsupported file type: {extension}")
    
    def _generate_data_hash(self, file_path: str) -> str:
        """Gera o hash SHA-256 de um arquivo"""
        import hashlib
        
        with open(file_path, 'rb') as f:
            file_hash = hashlib.sha256()
            for chunk in iter(lambda: f.read(4096), b""):
                file_hash.update(chunk)
        
        return file_hash.hexdigest()
    
    def get_status(self) -> Dict[str, Any]:
        """Obtém informações sobre o status e saúde da plataforma"""
        return {
            'platform': 'Data Reliability Platform',
            'version': '1.0.0',
            'status': 'running',
            'components': {
                'ingestion': 'active',
                'validation': 'active', 
                'versioning': 'active',
                'blockchain': 'active',
                'governance': 'active'
            },
            'config': {
                'lakefs_endpoint': self.config.get('lakefs.endpoint'),
                'bigchaindb_endpoint': self.config.get('bigchaindb.endpoint'),
                'data_sources_path': self.config.get('paths.data_sources')
            }
        }