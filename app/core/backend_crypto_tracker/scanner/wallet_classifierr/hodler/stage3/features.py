# ============================================================================
# wallet_classifier/hodler/stage3/features.py
# ============================================================================
"""Stage 3 features for hodler classification"""

from typing import List, Dict

def calculate_long_term_holding(transactions: List[Dict]) -> float:
    """Berechnet Langzeit-Haltedauer"""
    if not transactions:
        return 0.0
    first_tx = min(tx.get('timestamp', 0) for tx in transactions)
    last_tx = max(tx.get('timestamp', 0) for tx in transactions)
    hold_period_days = (last_tx - first_tx) / 86400
    return min(hold_period_days / 365.0, 1.0)

def calculate_withdrawal_resistance(transactions: List[Dict]) -> float:
    """Berechnet Widerstand gegen Abhebungen"""
    incoming_vol = sum(tx.get('value', 0) for tx in transactions 
                      if tx.get('type') == 'incoming')
    outgoing_vol = sum(tx.get('value', 0) for tx in transactions 
                      if tx.get('type') == 'outgoing')
    if incoming_vol == 0:
        return 0.0
    return 1.0 - min(outgoing_vol / incoming_vol, 1.0)

def calculate_hodl_conviction_score(transactions: List[Dict]) -> float:
    """Berechnet Hodl-Überzeugungsscore"""
    if not transactions:
        return 0.0
    # Kombiniert verschiedene Faktoren
    incoming = sum(1 for tx in transactions if tx.get('type') == 'incoming')
    outgoing = sum(1 for tx in transactions if tx.get('type') == 'outgoing')
    total = incoming + outgoing
    if total == 0:
        return 0.0
    return (incoming - outgoing) / total
