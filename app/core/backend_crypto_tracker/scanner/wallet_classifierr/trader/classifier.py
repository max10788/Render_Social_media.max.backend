# ============================================================================
# wallet_classifier/trader/classifier.py
# ============================================================================
"""Trader wallet classifier"""

from typing import List, Dict
from ..core.base_classifier import BaseClassifier
from .stage1.metrics import compute_stage1_metrics
from .stage2.metrics import compute_stage2_metrics
from .stage3.metrics import compute_stage3_metrics

class TraderClassifier(BaseClassifier):
    """Klassifizierer für Trader-Wallets"""
    
    STAGE1_WEIGHTS = {
        'trading_frequency': 1.5,
        'avg_time_between_txs': 1.2,
        'high_frequency_ratio': 1.3
    }
    
    STAGE2_WEIGHTS = {
        'dex_interaction_count': 1.8,
        'input_output_diversity': 1.4,
        'time_entropy': 1.3
    }
    
    STAGE3_WEIGHTS = {
        'temporal_pattern': 2.0,
        'arbitrage_behavior': 1.7,
        'cluster_diversity': 1.5
    }
    
    def __init__(self, stage: int = 1):
        super().__init__('trader', stage)
        self.threshold = 0.6
    
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
