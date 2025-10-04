# ============================================================================
# wallet_classifier/dust_sweeper/stage2/features.py
# ============================================================================
"""Stage 2 features for dust sweeper classification"""

import numpy as np
from typing import List, Dict

def calculate_consolidation_pattern(transactions: List[Dict]) -> float:
    """Erkennt Konsolidierungsmuster"""
    outgoing = [tx for tx in transactions if tx.get('type') == 'outgoing']
    if not outgoing:
        return 0.0
    
    values = [tx.get('value', 0) for tx in outgoing]
    avg_value = np.mean(values)
    
    # Normalisiert auf erwartete Dust-Konsolidierung
    return min(avg_value / 0.1, 1.0)

def calculate_batch_sweeping_score(transactions: List[Dict]) -> float:
    """Bewertet Batch-Sweeping-Verhalten"""
    if not transactions:
        return 0.0
    
    # Sucht nach Clustern von Transaktionen
    sorted_txs = sorted(transactions, key=lambda x: x.get('timestamp', 0))
    clusters = []
    current_cluster = []
    
    for i, tx in enumerate(sorted_txs):
        if i == 0:
            current_cluster.append(tx)
            continue
        
        time_diff = tx.get('timestamp', 0) - sorted_txs[i-1].get('timestamp', 0)
        if time_diff < 3600:  # Innerhalb 1 Stunde
            current_cluster.append(tx)
        else:
            if len(current_cluster) > 1:
                clusters.append(current_cluster)
            current_cluster = [tx]
    
    if len(current_cluster) > 1:
        clusters.append(current_cluster)
    
    return min(len(clusters) / (len(transactions) / 5), 1.0)

def calculate_dust_collection_efficiency(transactions: List[Dict]) -> float:
    """Berechnet Effizienz der Dust-Sammlung"""
    incoming_dust = sum(tx.get('value', 0) for tx in transactions 
                       if tx.get('type') == 'incoming' and tx.get('value', 0) < 0.01)
    outgoing_total = sum(tx.get('value', 0) for tx in transactions 
                        if tx.get('type') == 'outgoing')
    
    if incoming_dust == 0:
        return 0.0
    
    # Hohe Effizienz = gute Konsolidierung
    return min(outgoing_total / incoming_dust, 1.0)
