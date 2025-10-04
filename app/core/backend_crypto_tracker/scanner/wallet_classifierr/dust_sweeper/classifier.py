# ============================================================================
# wallet_classifier/dust_sweeper/classifier.py
# ============================================================================
"""Dust Sweeper wallet classifier"""

from typing import List, Dict
from ..core.base_classifier import BaseClassifier
from .stage1.metrics import compute_stage1_metrics
from .stage2.metrics import compute_stage2_metrics
from .stage3.metrics import compute_stage3_metrics

class DustSweeperClassifier(BaseClassifier):
    """Klassifizierer für Dust Sweeper-Wallets"""
    
    STAGE1_WEIGHTS = {
        'dust_ratio': 2.5,
        'outgoing_frequency': 1.8,
        'input_aggregation': 2.0
    }
    
    STAGE2_WEIGHTS = {
        'consolidation_pattern': 2.0,
        'batch_sweeping': 1.9,
        'collection_efficiency': 2.2
    }
    
    STAGE3_WEIGHTS = {
        'sweep_efficiency': 2.3,
        'gas_optimization': 2.0,
        'automated_pattern': 1.8
    }
    
    def __init__(self, stage: int = 1):
        super().__init__('dust_sweeper', stage)
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
