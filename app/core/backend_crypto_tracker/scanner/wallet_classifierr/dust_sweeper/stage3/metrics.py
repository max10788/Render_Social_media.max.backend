# ============================================================================
# wallet_classifier/dust_sweeper/stage3/metrics.py
# ============================================================================
"""Stage 3 metrics for dust sweeper classification"""

from typing import List, Dict
from .features import *

def compute_stage3_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 3 Metriken"""
    return {
        'sweep_efficiency': calculate_sweep_efficiency(transactions),
        'gas_optimization': calculate_gas_optimization_score(transactions),
        'automated_pattern': calculate_automated_sweeping_pattern(transactions)
    }
