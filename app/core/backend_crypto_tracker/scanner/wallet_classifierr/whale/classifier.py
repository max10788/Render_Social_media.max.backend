# ============================================================================
# wallet_classifier/whale/classifier.py
# ============================================================================
"""Whale wallet classifier"""

from typing import List, Dict
from ..core.base_classifier import BaseClassifier
from .stage1.metrics import compute_stage1_metrics
from .stage2.metrics import compute_stage2_metrics
from .stage3.metrics import compute_stage3_metrics

class WhaleClassifier(BaseClassifier):
    """Klassifizierer für Whale-Wallets"""
    
    STAGE1_WEIGHTS = {
        'high_avg_value': 2.5,
        'total_volume': 2.0,
        'large_tx_ratio': 1.8
    }
    
    STAGE2_WEIGHTS = {
        'market_impact': 1.9,
        'exchange_frequency': 1.7,
        'whale_behavior': 2.0
    }
    
    STAGE3_WEIGHTS = {
        'otc_pattern': 2.0,
        'institutional_pattern': 2.2,
        'liquidity_provision': 1.8
    }
    
    def __init__(self, stage: int = 1):
        super().__init__('whale', stage)
        self.threshold = 0.7
    
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
