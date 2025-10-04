# ============================================================================
# wallet_classifier/trader/stage2/features.py
# ============================================================================
"""Stage 2 features for trader classification"""

from datetime import datetime
from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_dex_interaction_count(transactions: List[Dict]) -> float:
    """Zählt DEX-Interaktionen"""
    dex_keywords = ['swap', 'exchange', 'uniswap', 'pancakeswap', 'sushiswap']
    dex_txs = sum(1 for tx in transactions 
                  if any(kw in str(tx.get('method', '')).lower() for kw in dex_keywords))
    return dex_txs / len(transactions) if transactions else 0.0

def calculate_input_output_diversity(transactions: List[Dict]) -> float:
    """Berechnet Diversität der Ein- und Ausgänge"""
    unique_addrs = TransactionUtils.get_unique_addresses(transactions)
    return unique_addrs / (len(transactions) * 0.5) if transactions else 0.0

def calculate_time_entropy(transactions: List[Dict]) -> float:
    """Berechnet Entropie der Transaktionszeiten"""
    if not transactions:
        return 0.0
    hours = [datetime.fromtimestamp(tx.get('timestamp', 0)).hour 
            for tx in transactions]
    return TransactionUtils.calculate_entropy(hours)
