# ============================================================================
# wallet_classifier/core/utils.py
# ============================================================================
"""Utility functions for transaction analysis"""

import numpy as np
from collections import Counter
from typing import List, Dict, Any

class TransactionUtils:
    """Utility-Funktionen für Transaktionsanalyse"""
    
    @staticmethod
    def calculate_entropy(values: List[Any]) -> float:
        """Berechnet die Shannon-Entropie einer Werteliste"""
        if not values:
            return 0.0
        counter = Counter(values)
        total = len(values)
        entropy = -sum((count/total) * np.log2(count/total) 
                      for count in counter.values() if count > 0)
        return entropy
    
    @staticmethod
    def calculate_time_differences(transactions: List[Dict]) -> List[float]:
        """Berechnet Zeitdifferenzen zwischen Transaktionen"""
        if len(transactions) < 2:
            return []
        times = sorted([tx.get('timestamp', 0) for tx in transactions])
        return [times[i+1] - times[i] for i in range(len(times)-1)]
    
    @staticmethod
    def get_unique_addresses(transactions: List[Dict], direction: str = 'both') -> int:
        """Zählt einzigartige Adressen (inputs/outputs)"""
        addresses = set()
        for tx in transactions:
            if direction in ['input', 'both']:
                addresses.update(tx.get('inputs', []))
            if direction in ['output', 'both']:
                addresses.update(tx.get('outputs', []))
        return len(addresses)
    
    @staticmethod
    def calculate_volume_stats(transactions: List[Dict]) -> Dict[str, float]:
        """Berechnet Volumen-Statistiken"""
        volumes = [tx.get('value', 0) for tx in transactions]
        if not volumes:
            return {'mean': 0, 'median': 0, 'std': 0, 'total': 0}
        return {
            'mean': np.mean(volumes),
            'median': np.median(volumes),
            'std': np.std(volumes),
            'total': sum(volumes)
        }
    
    @staticmethod
    def calculate_frequency(transactions: List[Dict]) -> float:
        """Berechnet Transaktionsfrequenz (Tx pro Tag)"""
        if not transactions:
            return 0.0
        time_span = max(tx.get('timestamp', 0) for tx in transactions) - \
                   min(tx.get('timestamp', 0) for tx in transactions)
        if time_span == 0:
            return 0.0
        return len(transactions) / (time_span / 86400)
