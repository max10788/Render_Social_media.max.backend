# wallet_classifier/dust_sweeper/stage1/features.py

from typing import Dict, List, Any
import numpy as np

class Stage1Features:
    """Feature extraction for Stage 1 Dust Sweeper analysis"""
    
    @staticmethod
    def extract_input_patterns(transactions: List[Dict]) -> Dict[str, float]:
        """Extract input-related patterns"""
        features = {}
        
        # Multi-input transaction ratio
        multi_input_txs = sum(1 for tx in transactions 
                              if tx.get('input_count', 0) > 1)
        features['multi_input_ratio'] = multi_input_txs / len(transactions) if transactions else 0
        
        # Average inputs per transaction
        input_counts = [tx.get('input_count', 0) for tx in transactions]
        features['avg_inputs'] = np.mean(input_counts) if input_counts else 0
        
        # Maximum inputs in single transaction
        features['max_inputs'] = max(input_counts) if input_counts else 0
        
        return features
    
    @staticmethod
    def extract_value_patterns(transactions: List[Dict]) -> Dict[str, float]:
        """Extract value-related patterns"""
        features = {}
        
        values = [tx.get('value', 0) for tx in transactions]
        
        if values:
            features['dust_ratio'] = sum(1 for v in values if 0 < v < 100) / len(values)
            features['micro_tx_ratio'] = sum(1 for v in values if 0 < v < 10) / len(values)
            features['value_variance'] = np.var(values)
        else:
            features['dust_ratio'] = 0
            features['micro_tx_ratio'] = 0
            features['value_variance'] = 0
        
        return features
