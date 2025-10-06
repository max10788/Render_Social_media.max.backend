# ============================================================================
# wallet_classifier/analyzer.py
# ============================================================================
"""Wallet Analyzer - Hauptklasse für Wallet-Klassifizierung"""

import logging
from typing import List, Dict, Any
from .trader.classifier import TraderClassifier
from .hodler.classifier import HodlerClassifier
from .whale.classifier import WhaleClassifier
from .mixer.classifier import MixerClassifier
from .dust_sweeper.classifier import DustSweeperClassifier

# Logger konfigurieren
logger = logging.getLogger(__name__)


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
            MixerClassifier(stage),
            DustSweeperClassifier(stage)
        ]
        
        # Mindestschwellenwerte für Scores (könnten angepasst werden)
        self.min_score_threshold = 0.1  # Mindestscore für gültige Klassifizierung
        
        logger.info(f"WalletAnalyzer initialisiert mit Stage {stage} und {len(self.classifiers)} Klassifizierern")
    
    def validate_transactions(self, transactions: List[Dict]) -> bool:
        """
        Validiert die Transaktionsdaten
        
        Args:
            transactions: Liste von Transaktions-Dictionaries
            
        Returns:
            True wenn valide, False sonst
        """
        if not transactions:
            logger.warning("Keine Transaktionen zur Validierung")
            return False
        
        required_fields = ['hash', 'value', 'timestamp']
        optional_fields = ['from', 'to', 'gas_price', 'gas_used', 'block_number', 'status']
        
        for i, tx in enumerate(transactions):
            # Prüfe erforderliche Felder
            missing_required = [field for field in required_fields if field not in tx]
            if missing_required:
                logger.warning(f"Transaktion {i} fehlt erforderliche Felder: {missing_required}")
                return False
            
            # Prüfe Datentypen
            if not isinstance(tx['value'], (int, float, str)):
                logger.warning(f"Transaktion {i} hat ungültigen Wert-Typ: {type(tx['value'])}")
                return False
            
            # Prüfe Wertebereich
            try:
                value = float(tx['value'])
                if value < 0:
                    logger.warning(f"Transaktion {i} hat negativen Wert: {value}")
                    return False
            except (ValueError, TypeError):
                logger.warning(f"Transaktion {i} hat ungültigen Wert: {tx['value']}")
                return False
        
        logger.info(f"Transaktionsvalidierung erfolgreich für {len(transactions)} Transaktionen")
        return True
    
    def analyze_wallet(self, transactions: List[Dict]) -> Dict[str, Any]:
        """
        Analysiert Wallet und gibt alle Klassifizierungsergebnisse zurück
        
        Args:
            transactions: Liste von Transaktions-Dictionaries
            
        Returns:
            Dictionary mit Analyseergebnissen
        """
        if not transactions:
            logger.warning("Keine Transaktionen zum Analysieren")
            return {
                'dominant_type': 'unknown',
                'confidence': 0.0,
                'stage': self.stage,
                'transaction_count': 0,
                'all_results': {},
                'error': 'Keine Transaktionen zum Analysieren'
            }
        
        # Validiere Transaktionen
        if not self.validate_transactions(transactions):
            logger.warning("Transaktionsvalidierung fehlgeschlagen")
            return {
                'dominant_type': 'unknown',
                'confidence': 0.0,
                'stage': self.stage,
                'transaction_count': len(transactions),
                'all_results': {},
                'error': 'Ungültige Transaktionsdaten'
            }
        
        results = {}
        debug_info = {}
        
        for classifier in self.classifiers:
            try:
                logger.debug(f"Analysiere mit {classifier.wallet_type}-Klassifizierer")
                result = classifier.analyze(transactions)
                
                # Debug-Informationen sammeln
                debug_info[classifier.wallet_type] = {
                    'score': result.get('score', 0),
                    'is_match': result.get('is_match', False),
                    'metrics': result.get('metrics', {})
                }
                
                # Normalisiere den Score (falls nötig)
                score = result.get('score', 0)
                if score > 1.0:
                    logger.warning(f"Score > 1.0 für {classifier.wallet_type}: {score}")
                    score = min(score, 1.0)
                
                result['score'] = score
                
                results[classifier.wallet_type] = result
                
                logger.debug(f"{classifier.wallet_type}-Ergebnis: Score={score:.4f}, Match={result.get('is_match', False)}")
                
            except Exception as e:
                logger.error(f"Fehler bei {classifier.wallet_type}-Klassifizierung: {str(e)}", exc_info=True)
                results[classifier.wallet_type] = {
                    'wallet_type': classifier.wallet_type,
                    'stage': self.stage,
                    'score': 0.0,
                    'is_match': False,
                    'metrics': {},
                    'error': str(e)
                }
        
        # Logge Debug-Informationen
        logger.info(f"Analyseergebnisse: {debug_info}")
        
        # Bestimme dominanten Wallet-Typ
        valid_results = {k: v for k, v in results.items() 
                        if v.get('score', 0) >= self.min_score_threshold}
        
        if valid_results:
            best_match = max(valid_results.items(), key=lambda x: x[1]['score'])
            dominant_type = best_match[0] if best_match[1]['is_match'] else 'unknown'
            confidence = best_match[1]['score']
            logger.info(f"Dominanter Typ: {dominant_type} mit Konfidenz {confidence:.4f}")
        else:
            dominant_type = 'unknown'
            confidence = 0.0
            logger.warning("Kein gültiger Klassifizierer gefunden")
        
        return {
            'dominant_type': dominant_type,
            'confidence': confidence,
            'stage': self.stage,
            'transaction_count': len(transactions),
            'all_results': results,
            'debug_info': debug_info  # Für Debugging-Zwecke
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
        logger.info(f"Suche Top-{top_n} Matches für {len(transactions)} Transaktionen")
        
        results = self.analyze_wallet(transactions)
        
        sorted_results = sorted(
            results['all_results'].items(),
            key=lambda x: x[1].get('score', 0),
            reverse=True
        )
        
        top_matches = [
            {
                'wallet_type': wallet_type,
                'score': data['score'],
                'is_match': data['is_match'],
                'metrics': data.get('metrics', {})
            }
            for wallet_type, data in sorted_results[:top_n]
        ]
        
        logger.info(f"Top-{top_n} Matches: {[m['wallet_type'] for m in top_matches]}")
        return top_matches
    
    def set_score_threshold(self, threshold: float):
        """
        Setzt den Mindestschwellenwert für Scores
        
        Args:
            threshold: Neuer Schwellenwert (0.0 - 1.0)
        """
        if 0.0 <= threshold <= 1.0:
            self.min_score_threshold = threshold
            logger.info(f"Mindestscore-Schwelle auf {threshold} gesetzt")
        else:
            raise ValueError("Schwellenwert muss zwischen 0.0 und 1.0 liegen")
