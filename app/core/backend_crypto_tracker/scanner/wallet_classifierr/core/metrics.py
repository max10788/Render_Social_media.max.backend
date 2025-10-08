# wallet_classifier/core/metrics.py

from typing import List, Dict, Any, Tuple
import numpy as np
from datetime import datetime, timedelta

class MetricCalculator:
    """Utility class for calculating blockchain metrics"""
    
    @staticmethod
    def calculate_transaction_velocity(transactions: List[Dict], period_days: int = 30) -> float:
        """Calculate transaction frequency over a period"""
        if not transactions:
            return 0.0
        
        current_time = datetime.now()
        period_start = current_time - timedelta(days=period_days)
        
        recent_txs = [
            tx for tx in transactions 
            if datetime.fromtimestamp(tx.get('timestamp', 0)) >= period_start
        ]
        
        return len(recent_txs) / period_days
    
    @staticmethod
    def calculate_value_distribution(transactions: List[Dict]) -> Dict[str, float]:
        """Calculate statistical distribution of transaction values"""
        if not transactions:
            return {'mean': 0, 'median': 0, 'std': 0, 'min': 0, 'max': 0}
        
        values = [tx.get('value', 0) for tx in transactions]
        
        return {
            'mean': np.mean(values),
            'median': np.median(values),
            'std': np.std(values),
            'min': np.min(values),
            'max': np.max(values),
            'coefficient_variation': np.std(values) / np.mean(values) if np.mean(values) > 0 else 0
        }
    
    @staticmethod
    def calculate_holding_period(transactions: List[Dict], balance: float) -> float:
        """Calculate average holding period in days"""
        if not transactions or balance <= 0:
            return 0.0
        
        # Calculate FIFO-based holding period
        current_time = datetime.now()
        received_txs = sorted(
            [tx for tx in transactions if tx.get('type') == 'receive'],
            key=lambda x: x.get('timestamp', 0)
        )
        
        remaining_balance = balance
        weighted_age = 0.0
        
        for tx in reversed(received_txs):
            tx_value = tx.get('value', 0)
            tx_time = datetime.fromtimestamp(tx.get('timestamp', 0))
            age_days = (current_time - tx_time).days
            
            if remaining_balance <= 0:
                break
            
            weight = min(tx_value, remaining_balance)
            weighted_age += weight * age_days
            remaining_balance -= weight
        
        return weighted_age / balance if balance > 0 else 0.0
    
    @staticmethod
    def calculate_network_centrality(address: str, transactions: List[Dict]) -> float:
        """Calculate network centrality score based on unique connections"""
        if not transactions:
            return 0.0
        
        unique_addresses = set()
        for tx in transactions:
            if tx.get('from') and tx.get('from') != address:
                unique_addresses.add(tx['from'])
            if tx.get('to') and tx.get('to') != address:
                unique_addresses.add(tx['to'])
        
        # Normalize by log scale (assumes power law distribution)
        return np.log1p(len(unique_addresses)) / 10.0
    
    @staticmethod
    def detect_mixing_patterns(transactions: List[Dict]) -> float:
        """Detect potential mixing behavior patterns"""
        if len(transactions) < 10:
            return 0.0
        
        scores = []
        
        # Check for equal output values
        values = [tx.get('value', 0) for tx in transactions]
        value_counts = {}
        for v in values:
            if v > 0:
                # Round to handle floating point
                rounded = round(v, 8)
                value_counts[rounded] = value_counts.get(rounded, 0) + 1
        
        # High frequency of same values indicates mixing
        if value_counts:
            max_freq = max(value_counts.values())
            equal_output_score = max_freq / len(values) if len(values) > 0 else 0
            scores.append(equal_output_score)
        
        # Check for timing patterns (regular intervals)
        if len(transactions) > 2:
            timestamps = sorted([tx.get('timestamp', 0) for tx in transactions])
            intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
            
            if intervals:
                # Low variance in intervals suggests automated/mixing behavior
                interval_variance = np.std(intervals) / np.mean(intervals) if np.mean(intervals) > 0 else 1
                timing_score = 1.0 - min(1.0, interval_variance)
                scores.append(timing_score)
        
        return np.mean(scores) if scores else 0.0
    
    @staticmethod
    def calculate_consolidation_ratio(transactions: List[Dict]) -> float:
        """Calculate ratio of multi-input to single-output transactions"""
        if not transactions:
            return 0.0
        
        consolidation_txs = 0
        for tx in transactions:
            inputs = tx.get('input_count', len(tx.get('inputs', [])))
            outputs = tx.get('output_count', len(tx.get('outputs', [])))
            
            # Consolidation pattern: many inputs, few outputs
            if inputs > 3 and outputs <= 2:
                consolidation_txs += 1
        
        return consolidation_txs / len(transactions) if transactions else 0.0
