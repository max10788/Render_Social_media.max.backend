# ============================================================================
# wallet_classifier/dust_sweeper/stage1/metrics.py
# ============================================================================
"""Stage 1 metrics for dust sweeper classification"""

from typing import List, Dict
from .features import *

def compute_stage1_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 1 Metriken"""
    return {
        'dust_ratio': calculate_dust_ratio(transactions),
        'outgoing_frequency': calculate_outgoing_frequency(transactions),
        'input_aggregation': calculate_small_input_aggregation(transactions)
    }
