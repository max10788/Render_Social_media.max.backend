# ============================================================================
# wallet_classifier/dust_sweeper/stage3/features.py
# ============================================================================
"""Stage 3 features for dust sweeper classification"""

import numpy as np
from typing import List, Dict

def calculate_sweep_efficiency(transactions: List[Dict]) -> float:
    """Berechnet detaillierte Sweep-Effizienz"""
    incoming_dust = sum(tx.get('value', 0) for tx in transactions 
                       if tx.get('type') == 'incoming' and tx.get('value', 0) < 0.01)
    outgoing_consolidated = sum(tx.get('value', 0) for tx in transactions 
                               if tx.get('type') == 'outgoing')
    
    if incoming_dust == 0:
        return 0.0
    
    efficiency = outgoing_consolidated / incoming_dust
    return min(efficiency, 1.0)

def calculate_gas_optimization_score(transactions: List[Dict]) -> float:
    """Bewertet Gas-Optimierung beim Sweeping"""
    outgoing = [tx for tx in transactions if tx.get('type') == 'outgoing']
    if not outgoing:
        return 0.0
    
    # Weniger Outputs = bessere Gas-Optimierung
    incoming_count = sum(1 for tx in transactions if tx.get('type') == 'incoming')
    outgoing_count = len(outgoing)
    
    if incoming_count == 0:
        return 0.0
    
    compression_ratio = 1.0 - (outgoing_count / incoming_count)
    return max(compression_ratio, 0.0)

def calculate_automated_sweeping_pattern(transactions: List[Dict]) -> float:
    """Erkennt automatisierte Sweeping-Muster"""
    outgoing = [tx for tx in transactions if tx.get('type') == 'outgoing']
    if len(outgoing) < 3:
        return 0.0
    
    # Regelmäßige Zeitabstände deuten auf Automation hin
    times = sorted([tx.get('timestamp', 0) for tx in outgoing])
    time_diffs = [times[i+1] - times[i] for i in range(len(times)-1)]
    
    if not time_diffs:
        return 0.0
    
    # Niedrige Standardabweichung = regelmäßig
    mean_diff = np.mean(time_diffs)
    std_diff = np.std(time_diffs)
    
    if mean_diff == 0:
        return 0.0
    
    regularity = 1.0 - min(std_diff / mean_diff, 1.0)
    return regularity
