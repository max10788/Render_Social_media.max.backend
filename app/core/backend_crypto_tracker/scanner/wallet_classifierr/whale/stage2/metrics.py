# ============================================================================
# wallet_classifier/whale/stage2/metrics.py
# ============================================================================
"""Stage 2 metrics for whale classification"""

from typing import List, Dict
from .features import *

def compute_stage2_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 2 Metriken"""
    return {
        'market_impact': calculate_market_impact(transactions),
        'exchange_frequency': calculate_exchange_frequency(transactions),
        'whale_behavior': calculate_whale_behavior_score(transactions)
    }
