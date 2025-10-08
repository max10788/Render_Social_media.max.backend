# wallet_classifier/hodler/stage1/features.py

from typing import Dict, List, Any
import numpy as np
from datetime import datetime

class Stage1Features:
    """Feature extraction for Stage 1 Hodler analysis"""
    
    @staticmethod
    def extract_holding_metrics(transactions: List[Dict], balance: float) -> Dict[str, float]:
        """Extract holding-related metrics"""
        features = {}
        
        if not transactions:
            return {'avg_hold_days': 0, 'longest_hold': 0, 'balance_age': 0}
        
        current_time = datetime.now()
        
        # Calculate average holding time
        hold_times = []
        for tx in transactions:
            if tx.get('type') == 'receive':
                tx_time = datetime.fromtimestamp(tx.get('timestamp', 0))
                hold_days = (current_time - tx_time).days
                hold_times.append(hold_days)
        
        features['avg_hold_days'] = np.mean(hold_times) if hold_times else 0
        features['longest_hold'] = max(hold_times) if hold_times else 0
        
        # Balance-weighted age
        if balance > 0:
            weighted_age = sum(h * (1/len(hold_times)) for h in hold_times) if hold_times else 0
            features['balance_age'] = weighted_age
        else:
            features['balance_age'] = 0
        
        return features
    
    @staticmethod
    def extract_accumulation_metrics(transactions: List[Dict]) -> Dict[str, float]:
        """Extract accumulation behavior metrics"""
        features = {}
        
        receives = [tx for tx in transactions if tx.get('type') == 'receive']
        sends = [tx for tx in transactions if tx.get('type') == 'send']
        
        features['receive_count'] = len(receives)
        features['send_count'] = len(sends)
        features['accumulation_ratio'] = len(receives) / (len(sends) + 1)
        
        # Net accumulation value
        total_received = sum(tx.get('value', 0) for tx in receives)
        total_sent = sum(tx.get('value', 0) for tx in sends)
        features['net_accumulation'] = total_received - total_sent
        
        return features
