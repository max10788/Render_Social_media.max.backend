# api/controllers/wallet_controller.py

from typing import Dict, Any, Optional, List
from datetime import datetime
import numpy as np
import pandas as pd
import logging
import os

from app.core.backend_crypto_tracker.scanner.wallet_classifierr import WalletClassifier

# Blockchain data fetchers - Direkte Imports
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_address_transactions import execute_get_address_transactions as get_eth_transactions
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_confirmed_signatures_for_address2 import execute_get_confirmed_signatures_for_address2 as get_sol_signatures
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_transaction_details import execute_get_transaction_details as get_sol_transaction
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_transaction_blocks import execute_get_transaction_blocks as get_sui_transactions

logger = logging.getLogger(__name__)

# ✅ ETHERSCAN KONFIGURATION
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"

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
    """Konvertiert Zeitstempel von ISO-Format zu Unix-Timestamps"""
    converted = []
    
    for tx in transactions:
        tx_copy = tx.copy()
        
        if 'timestamp' in tx_copy:
            timestamp_value = tx_copy['timestamp']
            
            if isinstance(timestamp_value, (int, float)):
                converted.append(tx_copy)
                continue
            
            if isinstance(timestamp_value, datetime):
                tx_copy['timestamp'] = int(timestamp_value.timestamp())
                converted.append(tx_copy)
                continue
                
            try:
                if isinstance(timestamp_value, str):
                    if timestamp_value.endswith('Z'):
                        timestamp_value = timestamp_value[:-1] + '+00:00'
                    
                    dt = datetime.fromisoformat(timestamp_value)
                    tx_copy['timestamp'] = int(dt.timestamp())
                    logger.debug(f"Konvertiert {tx['timestamp']} zu {tx_copy['timestamp']}")
            except (ValueError, TypeError) as e:
                logger.warning(f"Konnte Zeitstempel nicht konvertieren: {tx['timestamp']}, Fehler: {str(e)}")
        
        converted.append(tx_copy)
    
    return converted


class BlockchainDataFetcher:
    """Holt Transaktionsdaten von verschiedenen Blockchains"""
    
    @staticmethod
    async def fetch_ethereum_transactions(address: str, limit: int = 100) -> List[Dict]:
        """
        Holt Ethereum-Transaktionen für eine Adresse direkt von Etherscan
        
        Args:
            address: Ethereum-Adresse
            limit: Maximale Anzahl von Transaktionen
            
        Returns:
            Liste von Transaktionen
        """
        try:
            # ✅ API-Key aus Umgebungsvariablen holen
            api_key = os.getenv('ETHERSCAN_API_KEY') or os.getenv('ETHEREUM_API_KEY')
            
            if not api_key:
                raise Exception(
                    "❌ Kein Etherscan API-Key gefunden. "
                    "Bitte setzen Sie ETHERSCAN_API_KEY in Ihrer .env Datei"
                )
            
            logger.info(f"🔍 Rufe Ethereum-Transaktionen für {address} ab...")
            logger.info(f"   API-Key vorhanden: {api_key[:8]}...{api_key[-4:]}")
            logger.info(f"   Base URL: {ETHERSCAN_BASE_URL}")
            
            # ✅ Direkte Funktion mit expliziter base_url
            transactions = await get_eth_transactions(
                address=address,
                api_key=api_key,
                start_block=0,
                end_block=99999999,
                sort='desc',
                base_url=ETHERSCAN_BASE_URL  # ✅ Explizit gesetzt
            )
            
            # ✅ Behandle None-Fall
            if transactions is None:
                logger.error("❌ API-Aufruf fehlgeschlagen - None zurückgegeben")
                raise Exception("Etherscan API-Aufruf fehlgeschlagen")
            
            # ✅ Leere Liste ist OK (keine Transaktionen)
            if len(transactions) == 0:
                logger.info("ℹ️  Keine Transaktionen gefunden (neue Wallet?)")
                return []
            
            # Begrenze die Anzahl der Transaktionen
            if len(transactions) > limit:
                transactions = transactions[:limit]
                logger.info(f"📊 Limitiert auf {limit} von {len(transactions)} Transaktionen")
            
            # Konvertiere datetime-Objekte zu Unix-Timestamps
            for tx in transactions:
                if 'timestamp' in tx and isinstance(tx['timestamp'], datetime):
                    tx['timestamp'] = int(tx['timestamp'].timestamp())
            
            logger.info(f"✅ {len(transactions)} Ethereum-Transaktionen erfolgreich abgerufen")
            return transactions
            
        except Exception as e:
            logger.error(f"❌ Fehler beim Abrufen von Ethereum-Transaktionen: {str(e)}", exc_info=True)
            raise Exception(f"Ethereum-API-Fehler: {str(e)}")
    
    @staticmethod
    def fetch_solana_transactions_sync(address: str, limit: int = 100) -> List[Dict]:
        """Holt Solana-Transaktionen für eine Adresse"""
        try:
            signatures = get_sol_signatures(address, limit=limit)
            if not signatures:
                return []
            
            transactions = []
            for sig_info in signatures:
                signature = sig_info.get('signature')
                if signature:
                    tx_detail = get_sol_transaction(signature)
                    if tx_detail:
                        transactions.append(tx_detail)
            
            return transactions
        except Exception as e:
            logger.error(f"Fehler beim Abrufen von Solana-Transaktionen: {str(e)}")
            raise Exception(f"Solana-API-Fehler: {str(e)}")
    
    @staticmethod
    def fetch_sui_transactions_sync(address: str, limit: int = 100) -> List[Dict]:
        """Holt Sui-Transaktionen für eine Adresse"""
        try:
            transactions = get_sui_transactions(address, limit=limit)
            return transactions if transactions else []
        except Exception as e:
            logger.error(f"Fehler beim Abrufen von Sui-Transaktionen: {str(e)}")
            raise Exception(f"Sui-API-Fehler: {str(e)}")
    
    @staticmethod
    async def fetch_transactions(
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
        
        if blockchain in ['ethereum', 'eth']:
            return await BlockchainDataFetcher.fetch_ethereum_transactions(address, limit)
        elif blockchain in ['solana', 'sol']:
            return BlockchainDataFetcher.fetch_solana_transactions_sync(address, limit)
        elif blockchain == 'sui':
            return BlockchainDataFetcher.fetch_sui_transactions_sync(address, limit)
        else:
            raise ValueError(
                f"Unbekannte Blockchain: {blockchain}. "
                f"Unterstützte Blockchains: ethereum, solana, sui"
            )


class WalletController:
    """Controller für Wallet-Analyse-Operationen"""
    
    @staticmethod
    async def analyze_wallet(
        transactions: Optional[list] = None,
        wallet_address: Optional[str] = None,
        blockchain: Optional[str] = None,
        stage: int = 1,
        fetch_limit: int = 100
    ) -> Dict[str, Any]:
        """Analysiert eine Wallet"""
        try:
            if stage not in [1, 2, 3]:
                return {
                    'success': False,
                    'error': 'Stage muss zwischen 1 und 3 liegen',
                    'error_code': 'INVALID_STAGE'
                }
            
            if transactions is None:
                if not wallet_address or not blockchain:
                    return {
                        'success': False,
                        'error': 'Entweder Transaktionen oder (wallet_address + blockchain) müssen angegeben werden',
                        'error_code': 'MISSING_DATA'
                    }
                
                try:
                    logger.info(f"Hole Transaktionen für {wallet_address} von {blockchain}")
                    transactions = await BlockchainDataFetcher.fetch_transactions(
                        address=wallet_address,
                        blockchain=blockchain,
                        limit=fetch_limit
                    )
                    logger.info(f"Erfolgreich {len(transactions)} Transaktionen abgerufen")
                except Exception as e:
                    logger.error(f"Fehler beim Abrufen von Transaktionen: {str(e)}")
                    return {
                        'success': False,
                        'error': f'Fehler beim Abrufen von Transaktionen: {str(e)}',
                        'error_code': 'FETCH_ERROR'
                    }
            
            # ✅ WICHTIG: Leere Transaktionsliste ist kein Fehler mehr
            if not transactions:
                logger.info("ℹ️  Keine Transaktionen - wahrscheinlich neue Wallet")
                return {
                    'success': True,
                    'data': {
                        'wallet_address': wallet_address,
                        'blockchain': blockchain,
                        'data_source': 'blockchain' if wallet_address and blockchain else 'manual',
                        'analysis': {
                            'dominant_type': 'New Wallet',
                            'confidence': 1.0,
                            'stage': int(stage),
                            'transaction_count': 0
                        },
                        'classifications': [],
                        'message': 'Keine Transaktionen gefunden - dies ist eine neue oder inaktive Wallet'
                    },
                    'timestamp': datetime.utcnow().isoformat()
                }
            
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
            
            classifier = WalletClassifier()
            
            blockchain_data = {
                'txs': transactions,
                'balance': 0,
                'inputs': [],
                'outputs': []
            }
            
            for tx in transactions:
                tx_hash = tx.get('hash', tx.get('tx_hash', ''))
                
                for inp in tx.get('inputs', []):
                    inp['tx_hash'] = tx_hash
                    blockchain_data['inputs'].append(inp)
                
                for out in tx.get('outputs', []):
                    out['tx_hash'] = tx_hash
                    blockchain_data['outputs'].append(out)
            
            results = classifier.classify(
                address=wallet_address or 'unknown',
                blockchain_data=blockchain_data,
                config={'stage': stage}
            )
            
            results = convert_numpy_types(results)
            
            dominant_type = results.get('primary_class', 'Unknown')
            confidence = 0.0
            
            if dominant_type != 'Unknown' and dominant_type in results:
                confidence = results[dominant_type].get('score', 0.0)
            
            response = {
                'success': True,
                'data': {
                    'wallet_address': wallet_address,
                    'blockchain': blockchain,
                    'data_source': 'blockchain' if wallet_address and blockchain else 'manual',
                    'analysis': {
                        'dominant_type': dominant_type,
                        'confidence': round(float(confidence), 4),
                        'stage': int(stage),
                        'transaction_count': int(len(transactions))
                    },
                    'classifications': []
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
            for wallet_type, data in sorted(
                results.items(),
                key=lambda x: float(x[1].get('score', 0)) if isinstance(x[1], dict) else 0,
                reverse=True
            ):
                if wallet_type == 'primary_class' or not isinstance(data, dict):
                    continue
                    
                classification = {
                    'type': wallet_type,
                    'score': round(float(data.get('score', 0)), 4),
                    'is_match': bool(data.get('is_class', False)),
                    'metrics': {}
                }
                
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
                'error_code': 'BATCH_ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }'ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }
    
    @staticmethod
    async def get_top_matches(
        transactions: Optional[list] = None,
        wallet_address: Optional[str] = None,
        blockchain: Optional[str] = None,
        stage: int = 1,
        top_n: int = 3,
        fetch_limit: int = 100
    ) -> Dict[str, Any]:
        """Gibt die Top-N wahrscheinlichsten Wallet-Typen zurück"""
        try:
            if transactions is None:
                if not wallet_address or not blockchain:
                    return {
                        'success': False,
                        'error': 'Entweder Transaktionen oder (wallet_address + blockchain) müssen angegeben werden',
                        'error_code': 'MISSING_DATA'
                    }
                
                try:
                    logger.info(f"Hole Transaktionen für {wallet_address} von {blockchain}")
                    transactions = await BlockchainDataFetcher.fetch_transactions(
                        address=wallet_address,
                        blockchain=blockchain,
                        limit=fetch_limit
                    )
                    logger.info(f"Erfolgreich {len(transactions)} Transaktionen abgerufen")
                except Exception as e:
                    logger.error(f"Fehler beim Abrufen von Transaktionen: {str(e)}")
                    return {
                        'success': False,
                        'error': f'Fehler beim Abrufen von Transaktionen: {str(e)}',
                        'error_code': 'FETCH_ERROR'
                    }
            
            # ✅ Leere Transaktionsliste ist OK
            if not transactions:
                logger.info("ℹ️  Keine Transaktionen - neue Wallet")
                return {
                    'success': True,
                    'data': {
                        'wallet_address': wallet_address,
                        'blockchain': blockchain,
                        'data_source': 'blockchain' if wallet_address and blockchain else 'manual',
                        'top_matches': [
                            {
                                'rank': 1,
                                'type': 'New Wallet',
                                'score': 1.0,
                                'is_match': True
                            }
                        ],
                        'stage': int(stage),
                        'transaction_count': 0,
                        'message': 'Keine Transaktionen gefunden - neue oder inaktive Wallet'
                    },
                    'timestamp': datetime.utcnow().isoformat()
                }
            
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
            
            classifier = WalletClassifier()
            
            blockchain_data = {
                'txs': transactions,
                'balance': 0,
                'inputs': [],
                'outputs': []
            }
            
            for tx in transactions:
                tx_hash = tx.get('hash', tx.get('tx_hash', ''))
                
                for inp in tx.get('inputs', []):
                    inp['tx_hash'] = tx_hash
                    blockchain_data['inputs'].append(inp)
                
                for out in tx.get('outputs', []):
                    out['tx_hash'] = tx_hash
                    blockchain_data['outputs'].append(out)
            
            results = classifier.classify(
                address=wallet_address or 'unknown',
                blockchain_data=blockchain_data,
                config={'stage': stage}
            )
            
            results = convert_numpy_types(results)
            
            top_matches = []
            for wallet_type, data in sorted(
                results.items(),
                key=lambda x: float(x[1].get('score', 0)) if isinstance(x[1], dict) else 0,
                reverse=True
            ):
                if wallet_type == 'primary_class' or not isinstance(data, dict):
                    continue
                    
                top_matches.append({
                    'wallet_type': wallet_type,
                    'score': float(data.get('score', 0)),
                    'is_match': bool(data.get('is_class', False))
                })
                
                if len(top_matches) >= top_n:
                    break
            
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
    async def batch_analyze(
        wallets: list,
        stage: int = 1,
        fetch_limit: int = 100
    ) -> Dict[str, Any]:
        """Analysiert mehrere Wallets gleichzeitig"""
        try:
            classifier = WalletClassifier()
            results = []
            
            for wallet in wallets:
                address = wallet.get('address', 'unknown')
                transactions = wallet.get('transactions')
                blockchain = wallet.get('blockchain')
                
                if transactions is None and blockchain:
                    try:
                        logger.info(f"Hole Transaktionen für {address} von {blockchain}")
                        transactions = await BlockchainDataFetcher.fetch_transactions(
                            address=address,
                            blockchain=blockchain,
                            limit=fetch_limit
                        )
                        logger.info(f"Erfolgreich {len(transactions)} Transaktionen abgerufen")
                    except Exception as e:
                        results.append({
                            'address': address,
                            'blockchain': blockchain,
                            'success': False,
                            'error': f'Fehler beim Abrufen: {str(e)}'
                        })
                        continue
                
                # ✅ Leere Transaktionen sind OK
                if not transactions:
                    results.append({
                        'address': address,
                        'blockchain': blockchain,
                        'success': True,
                        'dominant_type': 'New Wallet',
                        'confidence': 1.0,
                        'transaction_count': 0,
                        'message': 'Keine Transaktionen'
                    })
                    continue
                
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
                
                blockchain_data = {
                    'txs': transactions,
                    'balance': 0,
                    'inputs': [],
                    'outputs': []
                }
                
                for tx in transactions:
                    tx_hash = tx.get('hash', tx.get('tx_hash', ''))
                    
                    for inp in tx.get('inputs', []):
                        inp['tx_hash'] = tx_hash
                        blockchain_data['inputs'].append(inp)
                    
                    for out in tx.get('outputs', []):
                        out['tx_hash'] = tx_hash
                        blockchain_data['outputs'].append(out)
                
                analysis = classifier.classify(
                    address=address,
                    blockchain_data=blockchain_data,
                    config={'stage': stage}
                )
                
                analysis = convert_numpy_types(analysis)
                
                dominant_type = analysis.get('primary_class', 'Unknown')
                confidence = 0.0
                
                if dominant_type != 'Unknown' and dominant_type in analysis:
                    confidence = analysis[dominant_type].get('score', 0.0)
                
                results.append({
                    'address': address,
                    'blockchain': blockchain,
                    'success': True,
                    'dominant_type': dominant_type,
                    'confidence': round(float(confidence), 4),
                    'transaction_count': int(len(transactions))
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
                'error_code':
