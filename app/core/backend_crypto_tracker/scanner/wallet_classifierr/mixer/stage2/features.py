# ============================================================================
# wallet_classifier/mixer/stage2/features.py
# ============================================================================
"""Stage 2 features for mixer classification"""

from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_address_diversity(transactions: List[Dict]) -> float:
    """Berechnet hohe Adress-Diversität"""
    unique_addrs = TransactionUtils.get_unique_addresses(transactions)
    return min(unique_addrs / len(transactions), 1.0) if transactions else 0.0

def calculate_mixer_service_detection(transactions: List[Dict]) -> float:
    """Erkennt bekannte Mixer-Services"""
    mixer_keywords = ['tornado', 'mixer', 'tumbler', 'privacy', 'wasabi', 'coinjoin']
    mixer_txs = sum(1 for tx in transactions 
                   if any(kw in str(tx.get('method', '')).lower() or 
                         kw in str(tx.get('to', '')).lower()
                         for kw in mixer_keywords))
    return min(mixer_txs / len(transactions), 1.0) if transactions else 0.0

def calculate_anonymity_pattern(transactions: List[Dict]) -> float:
    """Erkennt Anonymitätsmuster"""
    # Viele verschiedene Adressen + ähnliche Werte = Mixing
    diversity = calculate_address_diversity(transactions)
    uniformity = calculate_value_uniformity(transactions)
    return (diversity + uniformity) / 2.0
