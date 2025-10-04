# ============================================================================
# wallet_classifier/trader/stage1/metrics.py
# ============================================================================
"""Stage 1 metrics for trader classification"""

from typing import List, Dict
from .features import *

def compute_stage1_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 1 Metriken"""
    freq = calculate_trading_frequency(transactions)
    freq_score = min(freq / 10.0, 1.0)  # Normalisiert auf 10 Tx/Tag
    
    avg_time = calculate_avg_time_between_txs(transactions)
    time_score = 1.0 - min(avg_time / 24.0, 1.0) if avg_time > 0 else 0.0
    
    hf_ratio = calculate_high_frequency_ratio(transactions)
    
    return {
        'trading_frequency': freq_score,
        'avg_time_between_txs': time_score,
        'high_frequency_ratio': hf_ratio
    }
