# ============================================================================
# wallet_classifier/hodler/stage3/metrics.py
# ============================================================================
"""Stage 3 metrics for hodler classification"""

from typing import List, Dict
from .features import *

def compute_stage3_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 3 Metriken"""
    return {
        'long_term_holding': calculate_long_term_holding(transactions),
        'withdrawal_resistance': calculate_withdrawal_resistance(transactions),
        'hodl_conviction': calculate_hodl_conviction_score(transactions)
    }
