# ============================================================================
# wallet_classifier/trader/stage3/features.py
# ============================================================================
"""Stage 3 features for trader classification"""

import numpy as np
from typing import List, Dict
from ...core.utils import TransactionUtils

def calculate_temporal_transaction_pattern(transactions: List[Dict]) -> float:
    """Analysiert zeitliche Transaktionsmuster"""
    if len(transactions) < 10:
        return 0.0
    values = [tx.get('value', 0) for tx in transactions[-10:]]
    mean_val = np.mean(values)
    if mean_val == 0:
        return 0.0
    return np.std(values) / mean_val

def calculate_arbitrage_behavior_score(transactions: List[Dict]) -> float:
    """Bewertet Arbitrage-Verhalten"""
    vol_stats = TransactionUtils.calculate_volume_stats(transactions)
    if vol_stats['mean'] == 0:
        return 0.0
    return vol_stats['std'] / vol_stats['mean']

def calculate_cluster_diversity(transactions: List[Dict]) -> float:
    """Berechnet Cluster-Diversität der Transaktionsvolumen (ohne ML)"""
    if len(transactions) < 5:
        return 0.5
    
    volumes = sorted([tx.get('value', 0) for tx in transactions])
    
    # Einfache Clusterbildung basierend auf Quartilen
    q1 = np.percentile(volumes, 25)
    q2 = np.percentile(volumes, 50)
    q3 = np.percentile(volumes, 75)
    
    clusters = []
    for vol in volumes:
        if vol <= q1:
            clusters.append(0)
        elif vol <= q2:
            clusters.append(1)
        elif vol <= q3:
            clusters.append(2)
        else:
            clusters.append(3)
    
    # Diversität basierend auf Anzahl verwendeter Cluster
    unique_clusters = len(set(clusters))
    return unique_clusters / 4.0
