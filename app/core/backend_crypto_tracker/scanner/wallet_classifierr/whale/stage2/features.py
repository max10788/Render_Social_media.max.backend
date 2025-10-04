# ============================================================================
# wallet_classifier/whale/stage2/features.py
# ============================================================================
"""Stage 2 features for whale classification"""

from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_market_impact(transactions: List[Dict]) -> float:
    """Berechnet potenziellen Markteinfluss"""
    vol_stats = TransactionUtils.calculate_volume_stats(transactions)
    if vol_stats['std'] == 0:
        return 0.0
    return min(vol_stats['std'] / 50.0, 1.0)

def calculate_exchange_frequency(transactions: List[Dict]) -> float:
    """Berechnet Exchange-Interaktionsfrequenz"""
    exchange_keywords = ['binance', 'coinbase', 'kraken', 'exchange', 'cex']
    exchange_txs = sum(1 for tx in transactions 
                      if any(kw in str(tx.get('to', '')).lower() 
                            for kw in exchange_keywords))
    return exchange_txs / len(transactions) if transactions else 0.0

def calculate_whale_behavior_score(transactions: List[Dict]) -> float:
    """Berechnet Whale-Verhaltensscore"""
    vol_stats = TransactionUtils.calculate_volume_stats(transactions)
    if vol_stats['mean'] == 0:
        return 0.0
    # Hohe Varianz bei hohem Durchschnitt deutet auf Whale-Aktivität
    return min((vol_stats['std'] * vol_stats['mean']) / 1000.0, 1.0)
