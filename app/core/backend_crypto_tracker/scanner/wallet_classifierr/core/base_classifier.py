# ============================================================================
# wallet_classifier/core/base_classifier.py
# ============================================================================
"""Base classifier for all wallet types"""

from typing import List, Dict, Any
from .metrics import MetricCalculator

class BaseClassifier:
    """Basis-Klassifizierer für alle Wallet-Typen"""
    
    def __init__(self, wallet_type: str, stage: int = 1):
        self.wallet_type = wallet_type
        self.stage = stage
        self.calculator = MetricCalculator()
        self.threshold = 0.5
    
    def analyze(self, transactions: List[Dict]) -> Dict[str, Any]:
        """Führt Analyse durch und gibt Ergebnisse zurück"""
        self.calculator.clear()
        
        if self.stage >= 1:
            self.stage1_analysis(transactions)
        if self.stage >= 2:
            self.stage2_analysis(transactions)
        if self.stage >= 3:
            self.stage3_analysis(transactions)
        
        score = self.calculator.get_weighted_score()
        return {
            'wallet_type': self.wallet_type,
            'stage': self.stage,
            'score': score,
            'is_match': score >= self.threshold,
            'metrics': self.calculator.get_results_dict()
        }
    
    def stage1_analysis(self, transactions: List[Dict]):
        """Stage 1: Schnelle Heuristik (überschreiben in Subklassen)"""
        pass
    
    def stage2_analysis(self, transactions: List[Dict]):
        """Stage 2: Mustererkennung (überschreiben in Subklassen)"""
        pass
    
    def stage3_analysis(self, transactions: List[Dict]):
        """Stage 3: ML-basierte Analyse (überschreiben in Subklassen)"""
        pass

