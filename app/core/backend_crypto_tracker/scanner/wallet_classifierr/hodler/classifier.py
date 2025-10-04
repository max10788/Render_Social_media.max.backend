# ============================================================================
# wallet_classifier/hodler/classifier.py
# ============================================================================
"""Hodler wallet classifier"""

from typing import List, Dict
from ..core.base_classifier import BaseClassifier
from .stage1.metrics import compute_stage1_metrics
from .stage2.metrics import compute_stage2_metrics
from .stage3.metrics import compute_stage3_metrics

class HodlerClassifier(BaseClassifier):
    """Klassifizierer für Hodler-Wallets"""
    
    STAGE1_WEIGHTS = {
        'low_tx_frequency': 1.8,
        'avg_hold_time': 2.0,
        'incoming_outgoing_ratio': 1.5
    }
    
    STAGE2_WEIGHTS = {
        'balance_growth': 1.7,
        'low_diversity': 1.4,
        'accumulation_consistency': 1.6
    }
    
    STAGE3_WEIGHTS = {
        'long_term_holding': 2.2,
        'withdrawal_resistance': 2.0,
        'hodl_conviction': 1.8
    }
    
    def __init__(self, stage: int = 1):
        super().__init__('hodler', stage)
        self.threshold = 0.65
    
    def stage1_analysis(self, transactions: List[Dict]):
        metrics = compute_stage1_metrics(transactions)
        for name, value in metrics.items():
            self.calculator.add_result(name, value, self.STAGE1_WEIGHTS[name])
    
    def stage2_analysis(self, transactions: List[Dict]):
        metrics = compute_stage2_metrics(transactions)
        for name, value in metrics.items():
            self.calculator.add_result(name, value, self.STAGE2_WEIGHTS[name])
    
    def stage3_analysis(self, transactions: List[Dict]):
        metrics = compute_stage3_metrics(transactions)
        for name, value in metrics.items():
            self.calculator.add_result(name, value, self.STAGE3_WEIGHTS[name])
