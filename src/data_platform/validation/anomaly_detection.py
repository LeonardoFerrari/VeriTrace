import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

class AnomalyDetector:
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.isolation_config = config.get('validation.isolation_forest', {})
        self.dbscan_config = config.get('validation.dbscan', {})
        self.isolation_forest = None
        self.dbscan = None
        self.scaler = StandardScaler()
        self.pca = None
    
    def detect_anomalies(self, df: pd.DataFrame, config: Optional[Dict] = None) -> Dict[str, Any]:
        """        
        Args:
            df: Os dados para analisar
            config: Umas configs extras caso
            
        Returns:
            Um dict com tudo que achou de bizarro
        """
        try:
            self.logger.info(f"Starting anomaly detection on {len(df)} rows")
            
            # Arruma os números pra não dar ruim depois
            numeric_data = self._prepare_numeric_data(df)
            
            if numeric_data.empty:
                self.logger.warning("No numeric columns found for anomaly detection")
                return {
                    'anomaly_counts': {'isolation_forest': 0, 'dbscan': 0},
                    'anomaly_flags': pd.DataFrame(),
                    'summary': 'No numeric data available for analysis'
                }
            
            results = {
                'anomaly_counts': {},
                'anomaly_flags': pd.DataFrame(),
                'model_details': {},
                'summary': {}
            }
            
            # método 1: Isolation Forest
            if_results = self._isolation_forest_detection(numeric_data, config)
            results['anomaly_counts']['isolation_forest'] = if_results['count']
            results['model_details']['isolation_forest'] = if_results['details']
            
            # método 2: DBSCAN Clustering
            dbscan_results = self._dbscan_detection(numeric_data, config)
            results['anomaly_counts']['dbscan'] = dbscan_results['count']
            results['model_details']['dbscan'] = dbscan_results['details']
            
            # Combinação
            anomaly_flags = pd.DataFrame(index=df.index)
            anomaly_flags['isolation_forest_anomaly'] = if_results['flags']
            anomaly_flags['dbscan_anomaly'] = dbscan_results['flags']
            anomaly_flags['combined_anomaly'] = (
                anomaly_flags['isolation_forest_anomaly'] | 
                anomaly_flags['dbscan_anomaly']
            )
            
            results['anomaly_flags'] = anomaly_flags
            
            # resumo
            total_anomalies = anomaly_flags['combined_anomaly'].sum()
            anomaly_rate = total_anomalies / len(df) if len(df) > 0 else 0
            
            results['summary'] = {
                'total_rows': len(df),
                'total_anomalies': int(total_anomalies),
                'anomaly_rate': float(anomaly_rate),
                'methods_used': ['isolation_forest', 'dbscan'],
                'numeric_columns_analyzed': list(numeric_data.columns)
            }
            
            self.logger.info(f"Anomaly detection completed. Found {total_anomalies} anomalies ({anomaly_rate:.2%} rate)")
            return results
            
        except Exception as e:
            self.logger.error(f"Anomaly detection failed: {str(e)}")
            raise
    
    def _prepare_numeric_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Dá uma arrumada nos números"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if not numeric_cols:
            return pd.DataFrame()
        
        numeric_data = df[numeric_cols].copy()
        
        # valores faltantes
        numeric_data = numeric_data.fillna(numeric_data.median())
        
        # Tirar colunas com variancia zero
        variance_filter = numeric_data.var() > 0
        numeric_data = numeric_data.loc[:, variance_filter]
        
        return numeric_data
    
    def _isolation_forest_detection(self, data: pd.DataFrame, config: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            contamination = (config or {}).get('isolation_forest', {}).get('contamination', 
                                                self.isolation_config.get('contamination', 0.01))
            random_state = (config or {}).get('isolation_forest', {}).get('random_state',
                                             self.isolation_config.get('random_state', 42))
            
            # normalização
            scaled_data = self.scaler.fit_transform(data)
            
            # Train 
            self.isolation_forest = IsolationForest(
                contamination=contamination,
                random_state=random_state,
                n_jobs=-1
            )
            
            # Predict (-1 = anomalia, 1 = normal)
            predictions = self.isolation_forest.fit_predict(scaled_data)
            anomaly_flags = predictions == -1
            
            # Calcula score
            anomaly_scores = self.isolation_forest.decision_function(scaled_data)
            
            return {
                'flags': anomaly_flags,
                'count': np.sum(anomaly_flags),
                'details': {
                    'contamination': contamination,
                    'anomaly_scores_mean': float(np.mean(anomaly_scores)),
                    'anomaly_scores_std': float(np.std(anomaly_scores)),
                    'threshold': float(np.percentile(anomaly_scores, contamination * 100))
                }
            }
            
        except Exception as e:
            self.logger.error(f"Isolation Forest detection failed: {str(e)}")
            return {'flags': np.zeros(len(data), dtype=bool), 'count': 0, 'details': {}}
    
    def _dbscan_detection(self, data: pd.DataFrame, config: Optional[Dict] = None) -> Dict[str, Any]:
        """agrupar os dados"""
        try:
            eps = (config or {}).get('dbscan', {}).get('eps', self.dbscan_config.get('eps', 0.5))
            min_samples = (config or {}).get('dbscan', {}).get('min_samples', 
                                            self.dbscan_config.get('min_samples', 5))
            
            scaled_data = self.scaler.fit_transform(data)
            
            # Se tiver muitos dados, PCA 
            if data.shape[1] > 10:
                self.pca = PCA(n_components=min(10, data.shape[1]))
                scaled_data = self.pca.fit_transform(scaled_data)
            
            # DBSCAN
            self.dbscan = DBSCAN(eps=eps, min_samples=min_samples, n_jobs=-1)
            cluster_labels = self.dbscan.fit_predict(scaled_data)
            
            # anoamlia se -1
            anomaly_flags = cluster_labels == -1
            
            unique_clusters = np.unique(cluster_labels[cluster_labels != -1])
            cluster_sizes = [np.sum(cluster_labels == cluster) for cluster in unique_clusters]
            
            return {
                'flags': anomaly_flags,
                'count': np.sum(anomaly_flags),
                'details': {
                    'eps': eps,
                    'min_samples': min_samples,
                    'n_clusters': len(unique_clusters),
                    'cluster_sizes': cluster_sizes,
                    'noise_points': int(np.sum(anomaly_flags)),
                    'pca_components': self.pca.n_components_ if self.pca else data.shape[1]
                }
            }
            
        except Exception as e:
            self.logger.error(f"DBSCAN detection failed: {str(e)}")
            return {'flags': np.zeros(len(data), dtype=bool), 'count': 0, 'details': {}}
    
    def explain_anomalies(self, df: pd.DataFrame, anomaly_flags: pd.Series, top_n: int = 10) -> Dict[str, Any]:
        """
        Explicação
        
        Args:
            df: Os dados originais
            anomaly_flags: Lista dos casos suspeitos
            top_n: Casos mais suspeitos pra explicar
            
        Returns:
            Um dict explicando as tretas detectadas
        """
        try:
            anomalous_rows = df[anomaly_flags].head(top_n)
            normal_rows = df[~anomaly_flags]
            
            explanations = []
            
            for idx, row in anomalous_rows.iterrows():
                explanation = {
                    'row_index': idx,
                    'anomaly_reasons': []
                }
                
                # compara com normal
                for col in df.select_dtypes(include=[np.number]).columns:
                    value = row[col]
                    normal_mean = normal_rows[col].mean()
                    normal_std = normal_rows[col].std()
                    
                    if normal_std > 0:
                        z_score = abs((value - normal_mean) / normal_std)
                        if z_score > 2:  # > 2 desvio padrao
                            explanation['anomaly_reasons'].append({
                                'column': col,
                                'value': float(value),
                                'normal_mean': float(normal_mean),
                                'normal_std': float(normal_std),
                                'z_score': float(z_score),
                                'severity': 'high' if z_score > 3 else 'medium'
                            })
                
                explanations.append(explanation)
            
            return {
                'anomaly_explanations': explanations,
                'summary': {
                    'total_anomalies_explained': len(explanations),
                    'columns_analyzed': list(df.select_dtypes(include=[np.number]).columns)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Erro na explicação de anomalias: {str(e)}")
            return {'anomaly_explanations': [], 'summary': {}}
    
    def get_feature_importance(self) -> Dict[str, Any]:
        """Descobre quais campos são os mais importantes, tipo um ranking"""
        importance_data = {}
        
        if self.isolation_forest is not None:
            importance_data['isolation_forest'] = {
                'available': True,
                'note': 'Isolation Forest importance based on average path lengths'
            }
        
        if self.pca is not None:
            # PCA 
            importance_data['pca'] = {
                'explained_variance_ratio': self.pca.explained_variance_ratio_.tolist(),
                'n_components': self.pca.n_components_
            }
        
        return importance_data