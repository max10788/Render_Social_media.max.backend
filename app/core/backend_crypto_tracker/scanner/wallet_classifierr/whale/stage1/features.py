# ============================================================================
# wallet_classifier/whale/stage1/features.py
# ============================================================================
"""Stage 1 features for whale classification"""

from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_high_avg_value(transactions: List[Dict]) -> float:
    """Berechnet hohen durchschnittlichen Transaktionswert"""
    vol_stats = TransactionUtils.calculate_volume_stats(transactions)
    return min(vol_stats['mean'] / 100.0, 1.0)

def calculate_total_volume(transactions: List[Dict]) -> float:
    """Berechnet Gesamtvolumen"""
    vol_stats = TransactionUtils.calculate_volume_stats(transactions)
    return min(vol_stats['total'] / 1000.0, 1.0)

def calculate_large_tx_ratio(transactions: List[Dict]) -> float:
    """Berechnet Anteil großer Transaktionen"""
    if not transactions:
        return 0.0
    large_txs = sum(1 for tx in transactions if tx.get('value', 0) > 50)
    return large_txs / len(transactions)
