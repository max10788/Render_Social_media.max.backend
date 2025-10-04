# ============================================================================
# wallet_classifier/mixer/stage1/features.py
# ============================================================================
"""Stage 1 features for mixer classification"""

import numpy as np
from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_high_tx_count_score(transactions: List[Dict]) -> float:
    """Bewertet hohe Transaktionsanzahl"""
    return min(len(transactions) / 100.0, 1.0)

def calculate_value_uniformity(transactions: List[Dict]) -> float:
    """Berechnet Gleichförmigkeit der Transaktionswerte"""
    vol_stats = TransactionUtils.calculate_volume_stats(transactions)
    if vol_stats['mean'] == 0:
        return 0.0
    return 1.0 - min(vol_stats['std'] / vol_stats['mean'], 1.0)

def calculate_rapid_turnover(transactions: List[Dict]) -> float:
    """Berechnet schnellen Umschlag von Mitteln"""
    time_diffs = TransactionUtils.calculate_time_differences(transactions)
    if not time_diffs:
        return 0.0
    avg_time = np.mean(time_diffs) / 3600  # In Stunden
    return 1.0 - min(avg_time / 24.0, 1.0)
