# ============================================================================
# wallet_classifier/mixer/stage1/metrics.py
# ============================================================================
"""Stage 1 metrics for mixer classification"""

from typing import List, Dict
from .features import *

def compute_stage1_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 1 Metriken"""
    return {
        'high_tx_count': calculate_high_tx_count_score(transactions),
        'value_uniformity': calculate_value_uniformity(transactions),
        'rapid_turnover': calculate_rapid_turnover(transactions)
    }

