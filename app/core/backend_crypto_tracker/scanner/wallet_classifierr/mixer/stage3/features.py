# ============================================================================
# wallet_classifier/mixer/stage3/features.py
# ============================================================================
"""Stage 3 features for mixer classification"""

from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_privacy_pattern_score(transactions: List[Dict]) -> float:
    """Bewertet Privacy-Muster"""
    time_diffs = TransactionUtils.calculate_time_differences(transactions)
    if not time_diffs:
        return 0.0
    
    # Hohe Zeit-Entropie deutet auf randomisierte Timing-Patterns
    time_buckets = [int(d/60) for d in time_diffs]  # In Minuten
    entropy = TransactionUtils.calculate_entropy(time_buckets)
    return entropy / 6.0

def calculate_mixing_rounds(transactions: List[Dict]) -> float:
    """Schätzt Anzahl der Mixing-Runden"""
    # Basierend auf Cluster-Bildung ähnlicher Werte
    values = [tx.get('value', 0) for tx in transactions]
    if not values:
        return 0.0
    
    from collections import Counter
    value_counts = Counter(values)
    # Viele gleiche Werte deuten auf Mixing-Runden hin
    max_count = max(value_counts.values())
    return min(max_count / len(transactions), 1.0)

def calculate_obfuscation_score(transactions: List[Dict]) -> float:
    """Bewertet Verschleierungstechniken"""
    # Kombiniert mehrere Faktoren
    diversity = TransactionUtils.get_unique_addresses(transactions) / len(transactions) if transactions else 0
    
    time_diffs = TransactionUtils.calculate_time_differences(transactions)
    if time_diffs:
        time_variance = len(set([int(d/3600) for d in time_diffs])) / len(time_diffs)
    else:
        time_variance = 0
    
    return (diversity + time_variance) / 2.0
