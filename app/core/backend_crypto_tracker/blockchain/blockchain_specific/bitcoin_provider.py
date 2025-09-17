"""
Bitcoin blockchain API provider implementation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from ..base_provider import BaseAPIProvider
from ...data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class BitcoinProvider(BaseAPIProvider):
    """Bitcoin Blockchain API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Bitcoin", "https://blockchain.info", api_key, "BLOCKCHAIN_API_KEY")
    
    async def get_address_balance(self, address: str) -> Optional[Dict[str, Any]]:
        """Holt den Kontostand einer Bitcoin-Adresse"""
        try:
            url = f"{self.base_url}/balance"
            params = {'active': address}
            
            data = await self._make_request(url, params)
            
            if data:
                return {
                    'address': address,
                    'balance': data.get(address, {}).get('final_balance', 0) / 100000000,  # Satoshi zu BTC
                    'total_received': data.get(address, {}).get('total_received', 0) / 100000000,
                    'total_sent': data.get(address, {}).get('total_sent', 0) / 100000000,
                    'transaction_count': data.get(address, {}).get('n_tx', 0),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Bitcoin address balance: {e}")
        
        return None
    
    async def get_address_transactions(self, address: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        """Holt Transaktionen für eine Bitcoin-Adresse"""
        try:
            url = f"{self.base_url}/rawaddr/{address}"
            params = {'limit': limit}
            
            data = await self._make_request(url, params)
            
            if data and data.get('txs'):
                transactions = []
                for tx in data['txs']:
                    transactions.append({
                        'tx_hash': tx.get('hash'),
                        'block_height': tx.get('block_height'),
                        'timestamp': datetime.fromtimestamp(tx.get('time')),
                        'inputs': tx.get('inputs', []),
                        'outputs': tx.get('out', []),
                        'fee': tx.get('fee', 0) / 100000000,
                        'result': sum(out.get('value', 0) for out in tx.get('out', [])) / 100000000
                    })
                
                return transactions
        except Exception as e:
            logger.error(f"Error fetching Bitcoin address transactions: {e}")
        
        return None
    
    async def get_transaction_details(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """Holt Details zu einer spezifischen Transaktion"""
        try:
            url = f"{self.base_url}/rawtx/{tx_hash}"
            
            data = await self._make_request(url, {})
            
            if data:
                return {
                    'tx_hash': data.get('hash'),
                    'block_height': data.get('block_height'),
                    'timestamp': datetime.fromtimestamp(data.get('time')),
                    'inputs': data.get('inputs', []),
                    'outputs': data.get('out', []),
                    'fee': data.get('fee', 0) / 100000000,
                    'size': data.get('size'),
                    'weight': data.get('weight'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Bitcoin transaction details: {e}")
        
        return None
    
    async def get_block_info(self, block_height: int) -> Optional[Dict[str, Any]]:
        """Holt Informationen zu einem Bitcoin-Block"""
        try:
            url = f"{self.base_url}/block-height/{block_height}"
            params = {'format': 'json'}
            
            data = await self._make_request(url, params)
            
            if data:
                return {
                    'block_height': data.get('height'),
                    'block_hash': data.get('hash'),
                    'timestamp': datetime.fromtimestamp(data.get('time')),
                    'transactions': data.get('tx', []),
                    'difficulty': data.get('difficulty'),
                    'size': data.get('size'),
                    'version': data.get('ver'),
                    'merkle_root': data.get('mrkl_root'),
                    'nonce': data.get('nonce'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Bitcoin block info: {e}")
        
        return None
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Bitcoin hat keine Token-Adressen, daher leere Implementierung"""
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 30, "requests_per_hour": 1800}
