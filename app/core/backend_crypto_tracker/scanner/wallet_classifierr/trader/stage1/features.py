# ============================================================================
# wallet_classifier/trader/stage1/features.py
# ============================================================================
"""Stage 1 features for trader classification"""

import numpy as np
from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_trading_frequency(transactions: List[Dict]) -> float:
    """Berechnet Handelsfrequenz (Transaktionen pro Tag)"""
    return TransactionUtils.calculate_frequency(transactions)

def calculate_avg_time_between_txs(transactions: List[Dict]) -> float:
    """Berechnet durchschnittliche Zeit zwischen Transaktionen in Stunden"""
    time_diffs = TransactionUtils.calculate_time_differences(transactions)
    if not time_diffs:
        return 0.0
    return np.mean(time_diffs) / 3600

def calculate_high_frequency_ratio(transactions: List[Dict]) -> float:
    """Berechnet Anteil von Transaktionen mit kurzen Zeitabständen"""
    time_diffs = TransactionUtils.calculate_time_differences(transactions)
    if not time_diffs:
        return 0.0
    short_intervals = sum(1 for diff in time_diffs if diff < 3600)  # < 1 Stunde
    return short_intervals / len(time_diffs)
