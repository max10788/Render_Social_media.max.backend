# ============================================================================
# wallet_classifier/hodler/stage2/features.py
# ============================================================================
"""Stage 2 features for hodler classification"""

from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_balance_growth(transactions: List[Dict]) -> float:
    """Berechnet Balance-Wachstumsmuster"""
    if not transactions:
        return 0.0
    
    balance = 0
    balances = []
    for tx in sorted(transactions, key=lambda x: x.get('timestamp', 0)):
        if tx.get('type') == 'incoming':
            balance += tx.get('value', 0)
        else:
            balance -= tx.get('value', 0)
        balances.append(balance)
    
    return 1.0 if balances[-1] > balances[0] else 0.3

def calculate_low_address_diversity(transactions: List[Dict]) -> float:
    """Berechnet niedrige Adress-Diversität (gut für Hodler)"""
    unique_addrs = TransactionUtils.get_unique_addresses(transactions)
    if not transactions:
        return 0.0
    diversity = unique_addrs / len(transactions)
    return 1.0 - min(diversity, 1.0)

def calculate_accumulation_consistency(transactions: List[Dict]) -> float:
    """Berechnet Konsistenz der Akkumulation"""
    vol_stats = TransactionUtils.calculate_volume_stats(transactions)
    if vol_stats['mean'] == 0:
        return 0.0
    return 1.0 - min(vol_stats['std'] / vol_stats['mean'], 1.0)
