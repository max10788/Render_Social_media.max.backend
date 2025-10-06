# ============================================================================
# api/controllers/wallet_controller.py
# ============================================================================
"""Controller für Wallet-Analyse-Endpunkte mit Blockchain-Integration"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import numpy as np
import pandas as pd
import logging

# Fix: Import the correct analyzer class
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.analyzer import WalletAnalyzer

# Blockchain data fetchers
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_address_transactions import execute_get_address_transactions as get_eth_transactions
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_confirmed_signatures_for_address2 import execute_get_confirmed_signatures_for_address2 as get_sol_signatures
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_transaction_details import execute_get_transaction_details as get_sol_transaction
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_transaction_blocks import execute_get_transaction_blocks as get_sui_transactions

# Logger konfigurieren
logger = logging.getLogger(__name__)


def convert_numpy_types(obj):
    """Konvertiert numpy/pandas-Typen rekursiv in native Python-Typen"""
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


def convert_timestamps_to_unix(transactions: List[Dict]) -> List[Dict]:
    """
    Konvertiert Zeitstempel von ISO-Format zu Unix-Timestamps
    
    Args:
        transactions: Liste von Transaktions-Dictionaries
        
    Returns:
        Transaktionen mit konvertierten Zeitstempeln
    """
    converted = []
    
    for tx in transactions:
        tx_copy = tx.copy()
        
        # Konvertiere Zeitstempel, falls vorhanden
        if 'timestamp' in tx_copy:
            timestamp_str = tx_copy['timestamp']
            
            # Wenn es bereits ein numerischer Wert ist, nichts tun
            if isinstance(timestamp_str, (int, float)):
                continue
                
            try:
                # Versuche, ISO-Format zu parsen
                if isinstance(timestamp_str, str):
                    # Entferne 'Z' für UTC und ersetze durch +00:00
                    if timestamp_str.endswith('Z'):
                        timestamp_str = timestamp_str[:-1] + '+00:00'
                    
                    # Parse den Zeitstempel
                    dt = datetime.fromisoformat(timestamp_str)
                    tx_copy['timestamp'] = int(dt.timestamp())
                    logger.debug(f"Konvertiert {tx['timestamp']} zu {tx_copy['timestamp']}")
            except (ValueError, TypeError) as e:
                logger.warning(f"Konnte Zeitstempel nicht konvertieren: {tx['timestamp']}, Fehler: {str(e)}")
                # Behalte den Originalwert bei Fehlern
                pass
        
        converted.append(tx_copy)
    
    return converted


class BlockchainDataFetcher:
    """Holt Transaktionsdaten von verschiedenen Blockchains"""
    
    @staticmethod
    def fetch_ethereum_transactions(address: str, limit: int = 100) -> List[Dict]:
        """Holt Ethereum-Transaktionen für eine Adresse"""
        try:
            transactions = get_eth_transactions(address, limit=limit)
            return transactions if transactions else []
        except Exception as e:
            raise Exception(f"Fehler beim Abrufen von Ethereum-Transaktionen: {str(e)}")
    
    @staticmethod
    def fetch_solana_transactions(address: str, limit: int = 100) -> List[Dict]:
        """Holt Solana-Transaktionen für eine Adresse"""
        try:
            # Hole Signaturen
            signatures = get_sol_signatures(address, limit=limit)
            if not signatures:
                return []
            
            # Hole Details für jede Signatur
            transactions = []
            for sig_info in signatures:
                signature = sig_info.get('signature')
                if signature:
                    tx_detail = get_sol_transaction(signature)
                    if tx_detail:
                        transactions.append(tx_detail)
            
            return transactions
        except Exception as e:
            raise Exception(f"Fehler beim Abrufen von Solana-Transaktionen: {str(e)}")
    
    @staticmethod
    def fetch_sui_transactions(address: str, limit: int = 100) -> List[Dict]:
        """Holt Sui-Transaktionen für eine Adresse"""
        try:
            transactions = get_sui_transactions(address, limit=limit)
            return transactions if transactions else []
        except Exception as e:
            raise Exception(f"Fehler beim Abrufen von Sui-Transaktionen: {str(e)}")
    
    @staticmethod
    def fetch_transactions(
        address: str,
        blockchain: str,
        limit: int = 100
    ) -> List[Dict]:
        """
        Universelle Methode zum Abrufen von Transaktionen
        
        Args:
            address: Wallet-Adresse
            blockchain: Blockchain-Name (ethereum, solana, sui)
            limit: Maximale Anzahl von Transaktionen
            
        Returns:
            Liste von Transaktionen
        """
        blockchain = blockchain.lower()
        
        if blockchain == 'ethereum' or blockchain == 'eth':
            return BlockchainDataFetcher.fetch_ethereum_transactions(address, limit)
        elif blockchain == 'solana' or blockchain == 'sol':
            return BlockchainDataFetcher.fetch_solana_transactions(address, limit)
        elif blockchain == 'sui':
            return BlockchainDataFetcher.fetch_sui_transactions(address, limit)
        else:
            raise ValueError(f"Unbekannte Blockchain: {blockchain}. Unterstützte Blockchains: ethereum, solana, sui")


class WalletController:
    """Controller für Wallet-Analyse-Operationen"""
    
    @staticmethod
    def analyze_wallet(
        transactions: Optional[list] = None,
        wallet_address: Optional[str] = None,
        blockchain: Optional[str] = None,
        stage: int = 1,
        fetch_limit: int = 100
    ) -> Dict[str, Any]:
        """
        Analysiert eine Wallet basierend auf Transaktionen
        
        Args:
            transactions: Liste von Transaktionen (optional wenn wallet_address + blockchain)
            wallet_address: Wallet-Adresse (für automatisches Abrufen)
            blockchain: Blockchain-Name (für automatisches Abrufen)
            stage: Analysetiefe (1-3)
            fetch_limit: Maximale Anzahl abzurufender Transaktionen
            
        Returns:
            Analyse-Ergebnis als Dictionary
        """
        try:
            # Validierung
            if stage not in [1, 2, 3]:
                return {
                    'success': False,
                    'error': 'Stage muss zwischen 1 und 3 liegen',
                    'error_code': 'INVALID_STAGE'
                }
            
            # Entscheide ob Transaktionen abgerufen oder verwendet werden müssen
            if transactions is None:
                if not wallet_address or not blockchain:
                    return {
                        'success': False,
                        'error': 'Entweder Transaktionen oder (wallet_address + blockchain) müssen angegeben werden',
                        'error_code': 'MISSING_DATA'
                    }
                
                # Hole Transaktionen von der Blockchain
                try:
                    transactions = BlockchainDataFetcher.fetch_transactions(
                        address=wallet_address,
                        blockchain=blockchain,
                        limit=fetch_limit
                    )
                except Exception as e:
                    return {
                        'success': False,
                        'error': f'Fehler beim Abrufen von Transaktionen: {str(e)}',
                        'error_code': 'FETCH_ERROR'
                    }
            
            if not transactions:
                return {
                    'success': False,
                    'error': 'Keine Transaktionen verfügbar',
                    'error_code': 'NO_TRANSACTIONS'
                }
            
            # Konvertiere Zeitstempel zu Unix-Timestamps
            try:
                logger.info(f"Konvertiere Zeitstempel für {len(transactions)} Transaktionen")
                transactions = convert_timestamps_to_unix(transactions)
            except Exception as e:
                logger.error(f"Fehler bei der Zeitstempel-Konvertierung: {str(e)}")
                return {
                    'success': False,
                    'error': f'Fehler bei der Zeitstempel-Konvertierung: {str(e)}',
                    'error_code': 'TIMESTAMP_CONVERSION_ERROR'
                }
            
            # Analysiere Wallet
            analyzer = WalletAnalyzer(stage=stage)
            results = analyzer.analyze_wallet(transactions)
            
            # Konvertiere alle numpy-Typen zu nativen Python-Typen
            results = convert_numpy_types(results)
            
            # Formatiere Response
            response = {
                'success': True,
                'data': {
                    'wallet_address': wallet_address,
                    'blockchain': blockchain,
                    'data_source': 'blockchain' if wallet_address and blockchain else 'manual',
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
            logger.error(f"Unerwarteter Fehler in analyze_wallet: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'error_code': 'ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }
    
    @staticmethod
    def get_top_matches(
        transactions: Optional[list] = None,
        wallet_address: Optional[str] = None,
        blockchain: Optional[str] = None,
        stage: int = 1,
        top_n: int = 3,
        fetch_limit: int = 100
    ) -> Dict[str, Any]:
        """Gibt die Top-N wahrscheinlichsten Wallet-Typen zurück"""
        try:
            # Hole Transaktionen falls nötig
            if transactions is None:
                if not wallet_address or not blockchain:
                    return {
                        'success': False,
                        'error': 'Entweder Transaktionen oder (wallet_address + blockchain) müssen angegeben werden',
                        'error_code': 'MISSING_DATA'
                    }
                
                try:
                    transactions = BlockchainDataFetcher.fetch_transactions(
                        address=wallet_address,
                        blockchain=blockchain,
                        limit=fetch_limit
                    )
                except Exception as e:
                    return {
                        'success': False,
                        'error': f'Fehler beim Abrufen von Transaktionen: {str(e)}',
                        'error_code': 'FETCH_ERROR'
                    }
            
            if not transactions:
                return {
                    'success': False,
                    'error': 'Keine Transaktionen verfügbar',
                    'error_code': 'NO_TRANSACTIONS'
                }
            
            # Konvertiere Zeitstempel zu Unix-Timestamps
            try:
                logger.info(f"Konvertiere Zeitstempel für {len(transactions)} Transaktionen")
                transactions = convert_timestamps_to_unix(transactions)
            except Exception as e:
                logger.error(f"Fehler bei der Zeitstempel-Konvertierung: {str(e)}")
                return {
                    'success': False,
                    'error': f'Fehler bei der Zeitstempel-Konvertierung: {str(e)}',
                    'error_code': 'TIMESTAMP_CONVERSION_ERROR'
                }
            
            analyzer = WalletAnalyzer(stage=stage)
            top_matches = analyzer.get_top_matches(transactions, top_n=top_n)
            
            # Konvertiere numpy-Typen
            top_matches = convert_numpy_types(top_matches)
            
            return {
                'success': True,
                'data': {
                    'wallet_address': wallet_address,
                    'blockchain': blockchain,
                    'data_source': 'blockchain' if wallet_address and blockchain else 'manual',
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
            logger.error(f"Unerwarteter Fehler in get_top_matches: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'error_code': 'ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }
    
    @staticmethod
    def batch_analyze(
        wallets: list,
        stage: int = 1,
        fetch_limit: int = 100
    ) -> Dict[str, Any]:
        """
        Analysiert mehrere Wallets gleichzeitig
        
        Args:
            wallets: Liste von Wallet-Objekten mit:
                - Entweder: 'address' + 'blockchain' (für automatisches Abrufen)
                - Oder: 'address' + 'transactions' (manuelle Daten)
            stage: Analysetiefe (1-3)
            fetch_limit: Maximale Anzahl abzurufender Transaktionen pro Wallet
            
        Returns:
            Batch-Analyse-Ergebnisse
        """
        try:
            analyzer = WalletAnalyzer(stage=stage)
            results = []
            
            for wallet in wallets:
                address = wallet.get('address', 'unknown')
                transactions = wallet.get('transactions')
                blockchain = wallet.get('blockchain')
                
                # Hole Transaktionen falls nötig
                if transactions is None and blockchain:
                    try:
                        transactions = BlockchainDataFetcher.fetch_transactions(
                            address=address,
                            blockchain=blockchain,
                            limit=fetch_limit
                        )
                    except Exception as e:
                        results.append({
                            'address': address,
                            'blockchain': blockchain,
                            'success': False,
                            'error': f'Fehler beim Abrufen: {str(e)}'
                        })
                        continue
                
                if not transactions:
                    results.append({
                        'address': address,
                        'blockchain': blockchain,
                        'success': False,
                        'error': 'Keine Transaktionen verfügbar'
                    })
                    continue
                
                # Konvertiere Zeitstempel zu Unix-Timestamps
                try:
                    transactions = convert_timestamps_to_unix(transactions)
                except Exception as e:
                    results.append({
                        'address': address,
                        'blockchain': blockchain,
                        'success': False,
                        'error': f'Fehler bei der Zeitstempel-Konvertierung: {str(e)}'
                    })
                    continue
                
                analysis = analyzer.analyze_wallet(transactions)
                analysis = convert_numpy_types(analysis)
                
                results.append({
                    'address': address,
                    'blockchain': blockchain,
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
            logger.error(f"Unerwarteter Fehler in batch_analyze: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'error_code': 'BATCH_ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }
