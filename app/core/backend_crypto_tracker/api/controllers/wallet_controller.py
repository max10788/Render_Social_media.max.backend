# ============================================================================
# api/controllers/wallet_controller.py
# ============================================================================
"""Controller für Wallet-Analyse-Endpunkte"""

from typing import Dict, Any, Optional
from datetime import datetime

# Fix: Import the correct analyzer class
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.wallet_analyzer import WalletAnalyzer


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
            
            # Formatiere Response
            response = {
                'success': True,
                'data': {
                    'wallet_address': wallet_address,
                    'analysis': {
                        'dominant_type': results['dominant_type'],
                        'confidence': round(results['confidence'], 4),
                        'stage': results['stage'],
                        'transaction_count': results['transaction_count']
                    },
                    'classifications': []
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Füge alle Klassifizierungen hinzu (sortiert nach Score)
            for wallet_type, data in sorted(
                results['all_results'].items(),
                key=lambda x: x[1].get('score', 0),
                reverse=True
            ):
                response['data']['classifications'].append({
                    'type': wallet_type,
                    'score': round(data.get('score', 0), 4),
                    'is_match': data.get('is_match', False),
                    'metrics': {
                        k: round(v, 4) for k, v in data.get('metrics', {}).items()
                    }
                })
            
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
            
            return {
                'success': True,
                'data': {
                    'top_matches': [
                        {
                            'rank': idx + 1,
                            'type': match['wallet_type'],
                            'score': round(match['score'], 4),
                            'is_match': match['is_match']
                        }
                        for idx, match in enumerate(top_matches)
                    ],
                    'stage': stage,
                    'transaction_count': len(transactions)
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
                results.append({
                    'address': address,
                    'success': True,
                    'dominant_type': analysis['dominant_type'],
                    'confidence': round(analysis['confidence'], 4),
                    'transaction_count': analysis['transaction_count']
                })
            
            return {
                'success': True,
                'data': {
                    'analyzed_wallets': len(results),
                    'stage': stage,
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
