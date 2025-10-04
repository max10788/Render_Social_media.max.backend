# ============================================================================
# wallet_classifier/analyzer.py
# ============================================================================
"""Wallet Analyzer - Hauptklasse für Wallet-Klassifizierung"""

from typing import List, Dict, Any
from .trader.classifier import TraderClassifier
from .hodler.classifier import HodlerClassifier
from .whale.classifier import WhaleClassifier
from .miner.classifier import MinerClassifier
from .mixer.classifier import MixerClassifier
from .dust_sweeper.classifier import DustSweeperClassifier


class WalletAnalyzer:
    """Hauptanalyse-Klasse für Wallet-Klassifizierung"""
    
    def __init__(self, stage: int = 1):
        """
        Initialisiert den WalletAnalyzer
        
        Args:
            stage: Analysetiefe (1=schnell, 2=mittel, 3=detailliert)
        """
        if stage not in [1, 2, 3]:
            raise ValueError("Stage muss 1, 2 oder 3 sein")
        
        self.stage = stage
        self.classifiers = [
            TraderClassifier(stage),
            HodlerClassifier(stage),
            WhaleClassifier(stage),
            MinerClassifier(stage),
            MixerClassifier(stage),
            DustSweeperClassifier(stage)
        ]
    
    def analyze_wallet(self, transactions: List[Dict]) -> Dict[str, Any]:
        """
        Analysiert Wallet und gibt alle Klassifizierungsergebnisse zurück
        
        Args:
            transactions: Liste von Transaktions-Dictionaries
            
        Returns:
            Dictionary mit Analyseergebnissen
        """
        if not transactions:
            return {
                'dominant_type': 'unknown',
                'confidence': 0.0,
                'stage': self.stage,
                'transaction_count': 0,
                'all_results': {},
                'error': 'Keine Transaktionen zum Analysieren'
            }
        
        results = {}
        for classifier in self.classifiers:
            try:
                result = classifier.analyze(transactions)
                results[classifier.wallet_type] = result
            except Exception as e:
                results[classifier.wallet_type] = {
                    'wallet_type': classifier.wallet_type,
                    'stage': self.stage,
                    'score': 0.0,
                    'is_match': False,
                    'metrics': {},
                    'error': str(e)
                }
        
        # Bestimme dominanten Wallet-Typ
        valid_results = {k: v for k, v in results.items() if v.get('score', 0) > 0}
        
        if valid_results:
            best_match = max(valid_results.items(), key=lambda x: x[1]['score'])
            dominant_type = best_match[0] if best_match[1]['is_match'] else 'unknown'
            confidence = best_match[1]['score']
        else:
            dominant_type = 'unknown'
            confidence = 0.0
        
        return {
            'dominant_type': dominant_type,
            'confidence': confidence,
            'stage': self.stage,
            'transaction_count': len(transactions),
            'all_results': results
        }
    
    def get_top_matches(self, transactions: List[Dict], top_n: int = 3) -> List[Dict[str, Any]]:
        """
        Gibt die Top-N wahrscheinlichsten Wallet-Typen zurück
        
        Args:
            transactions: Liste von Transaktionen
            top_n: Anzahl der zurückzugebenden Ergebnisse
            
        Returns:
            Liste der Top-N Matches sortiert nach Score
        """
        results = self.analyze_wallet(transactions)
        
        sorted_results = sorted(
            results['all_results'].items(),
            key=lambda x: x[1].get('score', 0),
            reverse=True
        )
        
        return [
            {
                'wallet_type': wallet_type,
                'score': data['score'],
                'is_match': data['is_match'],
                'metrics': data['metrics']
            }
            for wallet_type, data in sorted_results[:top_n]
        ]
