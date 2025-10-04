# ============================================================================
# wallet_classifier/hodler/stage1/metrics.py
# ============================================================================
"""Stage 1 metrics for hodler classification"""

from typing import List, Dict
from .features import *

def compute_stage1_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 1 Metriken"""
    freq_score = calculate_low_tx_frequency(transactions)
    
    hold_time = calculate_avg_hold_time(transactions)
    hold_score = min(hold_time / 30.0, 1.0)  # Normalisiert auf 30 Tage
    
    ratio = calculate_incoming_outgoing_ratio(transactions)
    
    return {
        'low_tx_frequency': freq_score,
        'avg_hold_time': hold_score,
        'incoming_outgoing_ratio': ratio
    }
