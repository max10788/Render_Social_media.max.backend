# ============================================================================
# wallet_classifier/whale/stage3/features.py
# ============================================================================
"""Stage 3 features for whale classification"""

from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_otc_pattern(transactions: List[Dict]) -> float:
    """Erkennt OTC (Over-The-Counter) Handelsmuster"""
    time_diffs = TransactionUtils.calculate_time_differences(transactions)
    if not time_diffs:
        return 0.0
    # Unregelmäßige Zeitabstände deuten auf OTC-Handel hin
    time_buckets = [int(d/3600) for d in time_diffs]  # In Stunden
    entropy = TransactionUtils.calculate_entropy(time_buckets)
    return entropy / 5.0

def calculate_institutional_pattern(transactions: List[Dict]) -> float:
    """Erkennt institutionelle Handelsmuster"""
    if not transactions:
        return 0.0
    # Große, aber seltene Transaktionen
    vol_stats = TransactionUtils.calculate_volume_stats(transactions)
    freq = TransactionUtils.calculate_frequency(transactions)
    if freq == 0:
        return 0.0
    return min((vol_stats['mean'] / freq) / 100.0, 1.0)

def calculate_liquidity_provision_score(transactions: List[Dict]) -> float:
    """Bewertet Liquiditätsbereitstellung"""
    # Sucht nach gleichmäßigen, großen Transaktionen
    vol_stats = TransactionUtils.calculate_volume_stats(transactions)
    if vol_stats['mean'] == 0:
        return 0.0
    consistency = 1.0 - min(vol_stats['std'] / vol_stats['mean'], 1.0)
    size_factor = min(vol_stats['mean'] / 50.0, 1.0)
    return (consistency + size_factor) / 2.0
