# ============================================================================
# wallet_classifier/dust_sweeper/stage2/metrics.py
# ============================================================================
"""Stage 2 metrics for dust sweeper classification"""

from typing import List, Dict
from .features import *

def compute_stage2_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 2 Metriken"""
    return {
        'consolidation_pattern': calculate_consolidation_pattern(transactions),
        'batch_sweeping': calculate_batch_sweeping_score(transactions),
        'collection_efficiency': calculate_dust_collection_efficiency(transactions)
    }
