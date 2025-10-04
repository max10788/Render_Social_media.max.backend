# ============================================================================
# wallet_classifier/trader/stage2/metrics.py
# ============================================================================
"""Stage 2 metrics for trader classification"""

from typing import List, Dict
from .features import *

def compute_stage2_metrics(transactions: List[Dict]) -> Dict[str, float]:
    """Berechnet alle Stage 2 Metriken"""
    dex_score = min(calculate_dex_interaction_count(transactions), 1.0)
    diversity_score = min(calculate_input_output_diversity(transactions), 1.0)
    entropy = calculate_time_entropy(transactions)
    entropy_score = entropy / 4.5  # Max Entropy für 24 Stunden ≈ 4.58
    
    return {
        'dex_interaction_count': dex_score,
        'input_output_diversity': diversity_score,
        'time_entropy': entropy_score
    }
