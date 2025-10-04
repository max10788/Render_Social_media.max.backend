# ============================================================================
# wallet_classifier/mixer/classifier.py
# ============================================================================
"""Mixer wallet classifier"""

from typing import List, Dict
from ..core.base_classifier import BaseClassifier
from .stage1.metrics import compute_stage1_metrics
from .stage2.metrics import compute_stage2_metrics
from .stage3.metrics import compute_stage3_metrics

class MixerClassifier(BaseClassifier):
    """Klassifizierer für Mixer/Tumbler-Wallets"""
    
    STAGE1_WEIGHTS = {
        'high_tx_count': 1.5,
        'value_uniformity': 2.0,
        'rapid_turnover': 1.7
    }
    
    STAGE2_WEIGHTS = {
        'address_diversity': 2.2,
        'mixer_service': 3.0,
        'anonymity_pattern': 2.0
    }
    
    STAGE3_WEIGHTS = {
        'privacy_pattern': 2.5,
        'mixing_rounds': 2.2,
        'obfuscation_score': 2.3
    }
    
    def __init__(self, stage: int = 1):
        super().__init__('mixer', stage)
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
