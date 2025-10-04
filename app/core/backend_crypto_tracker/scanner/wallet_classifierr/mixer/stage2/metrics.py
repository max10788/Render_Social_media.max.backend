# ============================================================================
# wallet_classifier/mixer/stage2/metrics.py
# ============================================================================
"""Stage 2 metrics for mixer classification"""

from typing import List, Dict
from .features import *

def compute_stage2_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 2 Metriken"""
    return {
        'address_diversity': calculate_address_diversity(transactions),
        'mixer_service': calculate_mixer_service_detection(transactions),
        'anonymity_pattern': calculate_anonymity_pattern(transactions)
    }
