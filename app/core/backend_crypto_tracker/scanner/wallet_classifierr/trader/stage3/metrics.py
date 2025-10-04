# ============================================================================
# wallet_classifier/trader/stage3/metrics.py
# ============================================================================
"""Stage 3 metrics for trader classification"""

from typing import List, Dict
from .features import *

def compute_stage3_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 3 Metriken"""
    pattern_score = min(calculate_temporal_transaction_pattern(transactions) / 2.0, 1.0)
    arb_score = min(calculate_arbitrage_behavior_score(transactions), 1.0)
    cluster_score = calculate_cluster_diversity(transactions)
    
    return {
        'temporal_pattern': pattern_score,
        'arbitrage_behavior': arb_score,
        'cluster_diversity': cluster_score
    }
