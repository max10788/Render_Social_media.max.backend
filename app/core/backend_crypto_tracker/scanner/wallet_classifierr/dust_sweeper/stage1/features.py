# ============================================================================
# wallet_classifier/dust_sweeper/stage1/features.py
# ============================================================================
"""Stage 1 features for dust sweeper classification"""

from typing import List, Dict

def calculate_dust_ratio(transactions: List[Dict]) -> float:
    """Berechnet Anteil sehr kleiner Transaktionen"""
    if not transactions:
        return 0.0
    dust_threshold = 0.01  # < 0.01 ETH
    dust_txs = sum(1 for tx in transactions if tx.get('value', 0) < dust_threshold)
    return dust_txs / len(transactions)

def calculate_outgoing_frequency(transactions: List[Dict]) -> float:
    """Berechnet Häufigkeit ausgehender Transaktionen"""
    if not transactions:
        return 0.0
    outgoing = sum(1 for tx in transactions if tx.get('type') == 'outgoing')
    return outgoing / len(transactions)

def calculate_small_input_aggregation(transactions: List[Dict]) -> float:
    """Bewertet Aggregation kleiner Inputs"""
    incoming = [tx for tx in transactions if tx.get('type') == 'incoming']
    outgoing = [tx for tx in transactions if tx.get('type') == 'outgoing']
    
    if not incoming or not outgoing:
        return 0.0
    
    avg_in = sum(tx.get('value', 0) for tx in incoming) / len(incoming)
    avg_out = sum(tx.get('value', 0) for tx in outgoing) / len(outgoing)
    
    # Viele kleine Inputs, wenige große Outputs
    if avg_in < 0.01 and avg_out > avg_in * 5:
        return 1.0
    return 0.5
