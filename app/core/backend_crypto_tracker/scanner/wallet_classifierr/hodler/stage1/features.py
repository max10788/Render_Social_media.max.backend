# ============================================================================
# wallet_classifier/hodler/stage1/features.py
# ============================================================================
"""Stage 1 features for hodler classification"""

import numpy as np
from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_low_tx_frequency(transactions: List[Dict]) -> float:
    """Berechnet inverse Transaktionsfrequenz (niedrig = gut für Hodler)"""
    freq = TransactionUtils.calculate_frequency(transactions)
    return 1.0 - min(freq / 1.0, 1.0)

def calculate_avg_hold_time(transactions: List[Dict]) -> float:
    """Berechnet durchschnittliche Haltezeit in Tagen"""
    time_diffs = TransactionUtils.calculate_time_differences(transactions)
    if not time_diffs:
        return 0.0
    return np.mean(time_diffs) / 86400

def calculate_incoming_outgoing_ratio(transactions: List[Dict]) -> float:
    """Berechnet Verhältnis von eingehenden zu ausgehenden Transaktionen"""
    incoming = sum(1 for tx in transactions if tx.get('type') == 'incoming')
    outgoing = sum(1 for tx in transactions if tx.get('type') == 'outgoing')
    total = incoming + outgoing
    if total == 0:
        return 0.0
    return incoming / total
