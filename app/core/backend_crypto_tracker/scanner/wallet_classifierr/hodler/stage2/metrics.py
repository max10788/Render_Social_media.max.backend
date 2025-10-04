# ============================================================================
# wallet_classifier/hodler/stage2/metrics.py
# ============================================================================
"""Stage 2 metrics for hodler classification"""

from typing import List, Dict
from .features import *

def compute_stage2_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 2 Metriken"""
    return {
        'balance_growth': calculate_balance_growth(transactions),
        'low_diversity': calculate_low_address_diversity(transactions),
        'accumulation_consistency': calculate_accumulation_consistency(transactions)
