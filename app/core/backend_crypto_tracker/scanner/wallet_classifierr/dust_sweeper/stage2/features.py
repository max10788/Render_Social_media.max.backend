# wallet_classifier/dust_sweeper/stage2/features.py

from typing import Dict, List, Any
import numpy as np
from datetime import datetime, timedelta

class Stage2Features:
    """Feature extraction for Stage 2 Dust Sweeper analysis"""
    
    @staticmethod
    def extract_temporal_patterns(transactions: List[Dict]) -> Dict[str, float]:
        """Extract time-based behavioral patterns"""
        features = {}
        
        if len(transactions) < 2:
            return {'temporal_regularity': 0, 'burst_coefficient': 0}
        
        timestamps = sorted([tx.get('timestamp', 0) for tx in transactions])
        intervals = np.diff(timestamps)
        
        if len(intervals) > 0:
            # Temporal regularity (lower CV = more regular)
            cv = np.std(intervals) / np.mean(intervals) if np.mean(intervals) > 0 else 0
            features['temporal_regularity'] = 1.0 - min(1.0, cv)
            
            # Burst coefficient (identifies clustered activity)
            median_interval = np.median(intervals)
            burst_intervals = sum(1 for i in intervals if i < median_interval * 0.1)
            features['burst_coefficient'] = burst_intervals / len(intervals)
        else:
            features['temporal_regularity'] = 0
            features['burst_coefficient'] = 0
        
        return features
    
    @staticmethod
    def extract_network_patterns(transactions: List[Dict]) -> Dict[str, float]:
        """Extract network interaction patterns"""
        features = {}
        
        # Source diversity
        sources = []
        for tx in transactions:
            inputs = tx.get('inputs', [])
            for inp in inputs:
                addr = inp.get('address', inp.get('from', ''))
                if addr:
                    sources.append(addr)
        
        if sources:
            unique_sources = len(set(sources))
            features['source_diversity'] = unique_sources / len(sources)
            
            # Source concentration (Herfindahl index)
            source_counts = {}
            for s in sources:
                source_counts[s] = source_counts.get(s, 0) + 1
            
            total = len(sources)
            hhi = sum((count/total)**2 for count in source_counts.values())
            features['source_concentration'] = hhi
        else:
            features['source_diversity'] = 0
            features['source_concentration'] = 0
        
        return features
