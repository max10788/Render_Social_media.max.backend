# ============================================================================
# wallet_classifier/mixer/stage3/metrics.py
# ============================================================================
"""Stage 3 metrics for mixer classification"""

from typing import List, Dict
from .features import *

def compute_stage3_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 3 Metriken"""
    return {
        'privacy_pattern': calculate_privacy_pattern_score(transactions),
        'mixing_rounds': calculate_mixing_rounds(transactions),
        'obfuscation_score': calculate_obfuscation_score(transactions)
    }
