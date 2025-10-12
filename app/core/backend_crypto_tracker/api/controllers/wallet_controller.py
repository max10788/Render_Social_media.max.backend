from typing import Dict, Any, Optional, List
from datetime import datetime
import numpy as np
import pandas as pd
import logging
import os

from solana.rpc.api import Client as SolanaClient

from app.core.backend_crypto_tracker.scanner.wallet_classifierr import WalletClassifier
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_address_transactions import execute_get_address_transactions as get_eth_transactions
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_confirmed_signatures_for_address2 import execute_get_confirmed_signatures_for_address2 as get_sol_signatures
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_transaction_details import execute_get_transaction_details as get_sol_transaction
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_transaction_blocks import execute_get_transaction_blocks as get_sui_transactions

logger = logging.getLogger(__name__)

ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"
DEFAULT_TX_LIMIT = 25


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
            except (ValueError, TypeError) as e:
                logger.warning(f"Konnte Zeitstempel nicht konvertieren: {str(e)}")
        
        converted.append(tx_copy)
    
    return converted


def normalize_blockchain_data(transactions: List[Dict], blockchain: str) -> Dict[str, Any]:
    """Normalisiert Blockchain-Daten in die erwartete Struktur"""
    blockchain_lower = blockchain.lower() if blockchain else ''
    
    utxo_blockchains = ['bitcoin', 'btc', 'litecoin', 'ltc', 'dogecoin', 'doge']
    is_utxo = blockchain_lower in utxo_blockchains
    
    blockchain_data = {
        'txs': transactions,
        'balance': 0,
        'address': '',
        'inputs': [],
        'outputs': [],
        'outputs_per_tx': {},
        'inputs_per_tx': {}
    }
    
    if not transactions:
        return blockchain_data
    
    for tx in transactions:
        tx_hash = tx.get('hash', tx.get('tx_hash', tx.get('signature', '')))
        
        if not tx_hash:
            tx_hash = f"tx_{len(blockchain_data['outputs_per_tx'])}"
        
        blockchain_data['outputs_per_tx'][tx_hash] = 0
        blockchain_data['inputs_per_tx'][tx_hash] = 0
        
        if is_utxo:
            inputs = tx.get('inputs', [])
            for inp in inputs:
                inp_copy = inp.copy()
                inp_copy['tx_hash'] = tx_hash
                blockchain_data['inputs'].append(inp_copy)
                blockchain_data['inputs_per_tx'][tx_hash] += 1
        
        outputs = tx.get('outputs', [])
        if outputs:
            for out in outputs:
                out_copy = out.copy() if isinstance(out, dict) else {'value': 0}
                out_copy['tx_hash'] = tx_hash
                blockchain_data['outputs'].append(out_copy)
                blockchain_data['outputs_per_tx'][tx_hash] += 1
        
        # Stelle sicher dass jede TX mindestens 1 Output hat
        if blockchain_data['outputs_per_tx'][tx_hash] == 0:
            blockchain_data['outputs_per_tx'][tx_hash] = 1
            blockchain_data['outputs'].append({
                'tx_hash': tx_hash,
                'value': 0,
                'index': 0
            })
    
    return blockchain_data


class BlockchainDataFetcher:
    """Holt Transaktionsdaten von verschiedenen Blockchains"""
    
    @staticmethod
    async def fetch_ethereum_transactions(address: str, limit: int = DEFAULT_TX_LIMIT) -> List[Dict]:
        """Holt Ethereum-Transaktionen für eine Adresse"""
        try:
            api_key = os.getenv('ETHERSCAN_API_KEY') or os.getenv('ETHEREUM_API_KEY')
            
            if not api_key:
                raise Exception("Kein Etherscan API-Key gefunden")
            
            logger.info(f"Rufe Ethereum-Transaktionen für {address} ab (limit={limit})")
            
            transactions = await get_eth_transactions(
                address=address,
                api_key=api_key,
                start_block=0,
                end_block=99999999,
                sort='desc',
                base_url=ETHERSCAN_BASE_URL
            )
            
            if transactions is None:
                return []
            
            if len(transactions) > limit:
                transactions = transactions[:limit]
                logger.info(f"Limitiert auf {limit} Transaktionen")
            
            for tx in transactions:
                if 'timestamp' in tx and isinstance(tx['timestamp'], datetime):
                    tx['timestamp'] = int(tx['timestamp'].timestamp())
            
            logger.info(f"Erfolgreich {len(transactions)} Ethereum-Transaktionen abgerufen")
            return transactions
            
        except Exception as e:
            logger.error(f"Fehler beim Abrufen von Ethereum-Transaktionen: {str(e)}", exc_info=True)
            raise Exception(f"Ethereum-API-Fehler: {str(e)}")
    
    @staticmethod
    async def get_solana_provider():
        """Erstellt einen Solana RPC Provider"""
        try:
            solana_rpc_url = os.getenv('SOLANA_RPC_URL')
            
            if not solana_rpc_url:
                raise Exception("Kein Solana RPC URL gefunden")
            
            logger.info(f"Verbinde mit Solana RPC")
            provider = SolanaClient(solana_rpc_url)
            return provider
            
        except Exception as e:
            logger.error(f"Fehler beim Erstellen des Solana Providers: {str(e)}")
            raise Exception(f"Solana Provider-Fehler: {str(e)}")
    
    @staticmethod
    async def fetch_solana_transactions_sync(address: str, limit: int = DEFAULT_TX_LIMIT) -> List[Dict]:
        """Holt Solana-Transaktionen für eine Adresse"""
        try:
            provider = await BlockchainDataFetcher.get_solana_provider()
            
            logger.info(f"Rufe Solana-Signaturen für {address} ab (limit={limit})")
            
            signatures = await get_sol_signatures(
                provider=provider,
                address=address,
                limit=limit
            )
            
            if not signatures:
                logger.info(f"Keine Signaturen für Solana-Adresse {address} gefunden")
                return []
            
            logger.info(f"Gefunden {len(signatures)} Signaturen")
            
            transactions = []
            for idx, sig_info in enumerate(signatures):
                signature = sig_info.get('signature')
                if signature:
                    try:
                        tx_detail = await get_sol_transaction(
                            provider=provider,
                            signature=signature
                        )
                        if tx_detail:
                            transactions.append(tx_detail)
                        
                        if (idx + 1) % 10 == 0:
                            logger.debug(f"Verarbeitet {idx + 1}/{len(signatures)} Signaturen")
                            
                    except Exception as e:
                        logger.warning(f"Fehler bei Signatur {signature}: {str(e)}")
                        continue
            
            logger.info(f"Erfolgreich {len(transactions)} Solana-Transaktionen abgerufen")
            return transactions
            
        except Exception as e:
            logger.error(f"Fehler beim Abrufen von Solana-Transaktionen: {str(e)}", exc_info=True)
            raise Exception(f"Solana-API-Fehler: {str(e)}")
        
    @staticmethod
    def fetch_sui_transactions_sync(address: str, limit: int = DEFAULT_TX_LIMIT) -> List[Dict]:
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
        limit: int = DEFAULT_TX_LIMIT
    ) -> List[Dict]:
        """Universelle Methode zum Abrufen von Transaktionen"""
        blockchain = blockchain.lower()
        
        if blockchain in ['ethereum', 'eth']:
            return await BlockchainDataFetcher.fetch_ethereum_transactions(address, limit)
        elif blockchain in ['solana', 'sol']:
            return await BlockchainDataFetcher.fetch_solana_transactions_sync(address, limit)
        elif blockchain == 'sui':
            return BlockchainDataFetcher.fetch_sui_transactions_sync(address, limit)
        else:
            raise ValueError(f"Unbekannte Blockchain: {blockchain}")


class WalletController:
    """Controller für Wallet-Analyse-Operationen"""
    
    @staticmethod
    async def analyze_wallet(
        transactions: Optional[list] = None,
        wallet_address: Optional[str] = None,
        blockchain: Optional[str] = None,
        stage: int = 1,
        fetch_limit: int = DEFAULT_TX_LIMIT
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
                        'error': 'Entweder Transaktionen oder (wallet_address + blockchain) erforderlich',
                        'error_code': 'MISSING_DATA'
                    }
                
                try:
                    logger.info(f"Hole Transaktionen für {wallet_address} von {blockchain}")
                    transactions = await BlockchainDataFetcher.fetch_transactions(
                        address=wallet_address,
                        blockchain=blockchain,
                        limit=fetch_limit
                    )
                except Exception as e:
                    logger.error(f"Fehler beim Abrufen: {str(e)}")
                    return {
                        'success': False,
                        'error': f'Fehler beim Abrufen: {str(e)}',
                        'error_code': 'FETCH_ERROR'
                    }
            
            if not transactions:
                return {
                    'success': True,
                    'data': {
                        'wallet_address': wallet_address,
                        'blockchain': blockchain,
                        'analysis': {
                            'dominant_type': 'New Wallet',
                            'confidence': 1.0,
                            'stage': int(stage),
                            'transaction_count': 0
                        },
                        'classifications': [],
                        'message': 'Keine Transaktionen gefunden'
                    },
                    'timestamp': datetime.utcnow().isoformat()
                }
            
            try:
                transactions = convert_timestamps_to_unix(transactions)
            except Exception as e:
                return {
                    'success': False,
                    'error': f'Zeitstempel-Fehler: {str(e)}',
                    'error_code': 'TIMESTAMP_ERROR'
                }
            
            blockchain_data = normalize_blockchain_data(transactions, blockchain)
            blockchain_data['address'] = wallet_address
            
            logger.info(f"Klassifiziere Wallet {wallet_address}")
            logger.debug(f"Blockchain-Daten: outputs_per_tx={len(blockchain_data['outputs_per_tx'])}, txs={len(blockchain_data['txs'])}")
            
            classifier = WalletClassifier()
            results = classifier.classify(
                address=wallet_address or 'unknown',
                blockchain_data=blockchain_data,
                config={'stage': stage},
                blockchain=blockchain
            )
            
            results = convert_numpy_types(results)
            
            dominant_type = results.get('primary_class', 'Unknown')
            confidence = 0.0
            
            if dominant_type != 'Unknown' and dominant_type in results:
                confidence = float(results[dominant_type].get('score', 0.0))
            
            response = {
                'success': True,
                'data': {
                    'wallet_address': wallet_address,
                    'blockchain': blockchain,
                    'analysis': {
                        'dominant_type': dominant_type,
                        'confidence': round(confidence, 4),
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
                if wallet_type in ['primary_class', 'blockchain', 'address', 'hybrid_note', 'risk_flag', 'service_type']:
                    continue
                
                if not isinstance(data, dict):
                    continue
                
                classification = {
                    'type': wallet_type,
                    'score': round(float(data.get('score', 0)), 4),
                    'is_match': bool(data.get('is_class', False)),
                    'threshold': round(float(data.get('threshold', 0.5)), 4)
                }
                
                response['data']['classifications'].append(classification)
            
            return response
    
        except Exception as e:
            logger.error(f"Fehler in analyze_wallet: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'error_code': 'ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }
        
    @staticmethod
    async def get_top_matches(
        transactions: Optional[list] = None,
        wallet_address: Optional[str] = None,
        blockchain: Optional[str] = None,
        stage: int = 1,
        top_n: int = 3,
        fetch_limit: int = DEFAULT_TX_LIMIT
    ) -> Dict[str, Any]:
        """Gibt die Top-N wahrscheinlichsten Wallet-Typen zurück"""
        try:
            if transactions is None:
                if not wallet_address or not blockchain:
                    return {
                        'success': False,
                        'error': 'Transaktionen oder (wallet_address + blockchain) erforderlich',
                        'error_code': 'MISSING_DATA'
                    }
                
                try:
                    transactions = await BlockchainDataFetcher.fetch_transactions(
                        address=wallet_address,
                        blockchain=blockchain,
                        limit=fetch_limit
                    )
                except Exception as e:
                    return {
                        'success': False,
                        'error': f'Fehler beim Abrufen: {str(e)}',
                        'error_code': 'FETCH_ERROR'
                    }
            
            if not transactions:
                return {
                    'success': True,
                    'data': {
                        'wallet_address': wallet_address,
                        'blockchain': blockchain,
                        'top_matches': [{'rank': 1, 'type': 'New Wallet', 'score': 1.0, 'is_match': True}],
                        'stage': int(stage),
                        'transaction_count': 0
                    },
                    'timestamp': datetime.utcnow().isoformat()
                }
            
            try:
                transactions = convert_timestamps_to_unix(transactions)
            except Exception as e:
                return {
                    'success': False,
                    'error': f'Zeitstempel-Fehler: {str(e)}',
                    'error_code': 'TIMESTAMP_ERROR'
                }
            
            blockchain_data = normalize_blockchain_data(transactions, blockchain)
            blockchain_data['address'] = wallet_address
            
            classifier = WalletClassifier()
            
            results = classifier.classify(
                address=wallet_address or 'unknown',
                blockchain_data=blockchain_data,
                config={'stage': stage},
                blockchain=blockchain
            )
            
            results = convert_numpy_types(results)
            
            top_matches = []
            for wallet_type, data in sorted(
                results.items(),
                key=lambda x: float(x[1].get('score', 0)) if isinstance(x[1], dict) else 0,
                reverse=True
            ):
                if wallet_type in ['primary_class', 'blockchain', 'address', 'hybrid_note', 'risk_flag', 'service_type'] or not isinstance(data, dict):
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
            logger.error(f"Fehler in get_top_matches: {str(e)}", exc_info=True)
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
        fetch_limit: int = DEFAULT_TX_LIMIT
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
                    except Exception as e:
                        logger.error(f"Fehler beim Abrufen für {address}: {str(e)}")
                        results.append({
                            'address': address,
                            'blockchain': blockchain,
                            'success': False,
                            'error': str(e)
                        })
                        continue
                
                if not transactions:
                    results.append({
                        'address': address,
                        'blockchain': blockchain,
                        'success': True,
                        'dominant_type': 'New Wallet',
                        'confidence': 1.0,
                        'transaction_count': 0
                    })
                    continue
                
                try:
                    transactions = convert_timestamps_to_unix(transactions)
                except Exception as e:
                    logger.error(f"Zeitstempel-Fehler für {address}: {str(e)}")
                    results.append({
                        'address': address,
                        'blockchain': blockchain,
                        'success': False,
                        'error': str(e)
                    })
                    continue
                
                blockchain_data = normalize_blockchain_data(transactions, blockchain)
                blockchain_data['address'] = address
                
                try:
                    analysis = classifier.classify(
                        address=address,
                        blockchain_data=blockchain_data,
                        config={'stage': stage},
                        blockchain=blockchain
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
                    
                    logger.info(f"Wallet {address} analysiert: {dominant_type}")
                    
                except Exception as e:
                    logger.error(f"Klassifizierung fehlgeschlagen für {address}: {str(e)}", exc_info=True)
                    results.append({
                        'address': address,
                        'blockchain': blockchain,
                        'success': False,
                        'error': str(e)
                    })
            
            return {
                'success': True,
                'data': {
                    'analyzed_wallets': int(len(results)),
                    'successful_analyses': int(sum(1 for r in results if r.get('success'))),
                    'failed_analyses': int(sum(1 for r in results if not r.get('success'))),
                    'stage': int(stage),
                    'results': results
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Fehler in batch_analyze: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'error_code': 'BATCH_ANALYSIS_ERROR',
                'timestamp': datetime.utcnow().isoformat()
            }
