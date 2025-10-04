# ============================================================================
# wallet_classifier/whale/stage1/metrics.py
# ============================================================================
"""Stage 1 metrics for whale classification"""

from typing import List, Dict
from .features import *

def compute_stage1_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 1 Metriken"""
    return {
        'high_avg_value': calculate_high_avg_value(transactions),
        'total_volume': calculate_total_volume(transactions),
        'large_tx_ratio': calculate_large_tx_ratio(transactions)
    }
