# ============================================================================
# api/controllers/wallet_controller.py
# ============================================================================
"""Controller für Wallet-Analyse-Endpunkte"""

from typing import Dict, Any, Optional
from datetime import datetime
import numpy as np
import pandas as pd

# Fix: Import the correct analyzer class
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.analyzer import WalletAnalyzer


def convert_numpy_types(obj):
    """
    Konvertiert numpy/pandas-Typen rekursiv in native Python-Typen
    
    Args:
        obj: Zu konvertierendes Objekt
        
    Returns:
        Objekt mit nativen Python-Typen
    """
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, pd.Series):
        return convert_numpy_types(obj.to_dict())
    elif isinstance(obj, pd.DataFrame):
        return convert_numpy_types(obj.to_dict('records'))
    elif pd.isna(obj):
        return None
    return obj


class WalletController:
    """Controller für Wallet-Analyse-Operationen"""
    
    @staticmethod
    def analyze_wallet(
        transactions: list,
        stage: int = 1,
        wallet_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Analysiert eine Wallet basierend auf Transaktionen
        
        Args:
            transactions: Liste von Transaktionen
            stage: Analysetiefe (1-3)
            wallet_address: Optional - Wallet-Adresse für Logging
            
        Returns:
            Analyse-Ergebnis als Dictionary
        """
        try:
            # Validierung
            if not transactions:
                return {
                    'success': False,
                    'error': 'Keine Transaktionen bereitgestellt',
                    'error_code': 'NO_TRANSACTIONS'
                }
            
            if stage not in [1, 2, 3]:
                return {
                    'success': False,
                    'error': 'Stage muss zwischen 1 und 3 liegen',
                    'error_code': 'INVALID_STAGE'
                }
            
            # Analysiere Wallet
            analyzer = WalletAnalyzer(stage=stage)
            results = analyzer.analyze_wallet(transactions)
            
            # WICHTIG: Konvertiere alle numpy-Typen zu nativen Python-Typen
            results = convert_numpy_types(results)
            
            # Formatiere Response
            response = {
                'success': True,
                'data': {
                    'wallet_address': wallet_address,
                    'analysis': {
                        'dominant_type': results['dominant_type'],
                        'confidence': round(float(results['confidence']), 4),
                        'stage': int(results['stage']),
                        'transaction_count': int(results['transaction_count'])
                    },
                    'classifications': []
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Füge alle Klassifizierungen hinzu (sortiert nach Score)
            for wallet_type, data in sorted(
                results['all_results'].items(),
                key=lambda x: float(x[1].get('score', 0)),
                reverse=True
            ):
                classification = {
                    'type': wallet_type,
                    'score': round(float(data.get('score', 0)), 4),
                    'is_match': bool(data.get('is_match', False)),
                    'metrics': {}
                }
                
                # Konvertiere Metriken sicher
                metrics = data.get('metrics', {})
                if metrics:
                    classification['metrics'] = {
                        k: round(float(v), 4) if isinstance(v, (int, float, np.number)) else v
                        for k, v in metrics.items()
                    }
                
                response['data']['classifications'].append(classification)
            
            return response
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'error_code': 'ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }
    
    @staticmethod
    def get_top_matches(
        transactions: list,
        stage: int = 1,
        top_n: int = 3
    ) -> Dict[str, Any]:
        """
        Gibt die Top-N wahrscheinlichsten Wallet-Typen zurück
        
        Args:
            transactions: Liste von Transaktionen
            stage: Analysetiefe (1-3)
            top_n: Anzahl der Ergebnisse
            
        Returns:
            Top-N Matches als Dictionary
        """
        try:
            if not transactions:
                return {
                    'success': False,
                    'error': 'Keine Transaktionen bereitgestellt',
                    'error_code': 'NO_TRANSACTIONS'
                }
            
            analyzer = WalletAnalyzer(stage=stage)
            top_matches = analyzer.get_top_matches(transactions, top_n=top_n)
            
            # Konvertiere numpy-Typen
            top_matches = convert_numpy_types(top_matches)
            
            return {
                'success': True,
                'data': {
                    'top_matches': [
                        {
                            'rank': idx + 1,
                            'type': match['wallet_type'],
                            'score': round(float(match['score']), 4),
                            'is_match': bool(match['is_match'])
                        }
                        for idx, match in enumerate(top_matches)
                    ],
                    'stage': int(stage),
                    'transaction_count': int(len(transactions))
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'error_code': 'ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }
    
    @staticmethod
    def batch_analyze(
        wallets: list,
        stage: int = 1
    ) -> Dict[str, Any]:
        """
        Analysiert mehrere Wallets gleichzeitig
        
        Args:
            wallets: Liste von Wallet-Objekten mit 'address' und 'transactions'
            stage: Analysetiefe (1-3)
            
        Returns:
            Batch-Analyse-Ergebnisse
        """
        try:
            analyzer = WalletAnalyzer(stage=stage)
            results = []
            
            for wallet in wallets:
                address = wallet.get('address', 'unknown')
                transactions = wallet.get('transactions', [])
                
                if not transactions:
                    results.append({
                        'address': address,
                        'success': False,
                        'error': 'Keine Transaktionen'
                    })
                    continue
                
                analysis = analyzer.analyze_wallet(transactions)
                
                # Konvertiere numpy-Typen
                analysis = convert_numpy_types(analysis)
                
                results.append({
                    'address': address,
                    'success': True,
                    'dominant_type': analysis['dominant_type'],
                    'confidence': round(float(analysis['confidence']), 4),
                    'transaction_count': int(analysis['transaction_count'])
                })
            
            return {
                'success': True,
                'data': {
                    'analyzed_wallets': int(len(results)),
                    'stage': int(stage),
                    'results': results
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'error_code': 'BATCH_ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }
