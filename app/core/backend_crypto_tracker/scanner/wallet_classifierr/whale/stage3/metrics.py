# ============================================================================
# wallet_classifier/whale/stage3/metrics.py
# ============================================================================
"""Stage 3 metrics for whale classification"""

from typing import List, Dict
from .features import *

def compute_stage3_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 3 Metriken"""
    return {
        'otc_pattern': calculate_otc_pattern(transactions),
        'institutional_pattern': calculate_institutional_pattern(transactions),
        'liquidity_provision': calculate_liquidity_provision_score(transactions)
    }
