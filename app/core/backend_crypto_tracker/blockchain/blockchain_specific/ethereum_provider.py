"""
Ethereum blockchain API provider implementation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from ..base_provider import BaseAPIProvider
from ...data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class EthereumProvider(BaseAPIProvider):
    """Ethereum Blockchain API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Ethereum", "https://api.etherscan.io/api", api_key, "ETHERSCAN_API_KEY")
    
    async def get_address_balance(self, address: str) -> Optional[Dict[str, Any]]:
        """Holt den Kontostand einer Ethereum-Adresse"""
        try:
            url = self.base_url
            params = {
                'module': 'account',
                'action': 'balance',
                'address': address,
                'tag': 'latest',
                'apikey': self.api_key
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1':
                balance_wei = int(data.get('result', 0))
                return {
                    'address': address,
                    'balance': balance_wei / 10**18,  # Wei zu ETH
                    'balance_wei': balance_wei,
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Ethereum address balance: {e}")
        
        return None
    
    async def get_address_transactions(self, address: str, start_block: int = 0, end_block: int = 99999999, sort: str = 'asc') -> Optional[List[Dict[str, Any]]]:
        """Holt Transaktionen für eine Ethereum-Adresse"""
        try:
            url = self.base_url
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': address,
                'startblock': start_block,
                'endblock': end_block,
                'sort': sort,
                'apikey': self.api_key
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1' and data.get('result'):
                transactions = []
                for tx in data['result']:
                    transactions.append({
                        'tx_hash': tx.get('hash'),
                        'block_number': int(tx.get('blockNumber', 0)),
                        'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                        'from_address': tx.get('from'),
                        'to_address': tx.get('to'),
                        'value': int(tx.get('value', 0)) / 10**18,
                        'gas': int(tx.get('gas', 0)),
                        'gas_price': int(tx.get('gasPrice', 0)) / 10**9,  # Wei zu Gwei
                        'gas_used': int(tx.get('gasUsed', 0)),
                        'contract_address': tx.get('contractAddress'),
                        'nonce': int(tx.get('nonce', 0)),
                        'transaction_index': int(tx.get('transactionIndex', 0)),
                        'confirmations': int(tx.get('confirmations', 0))
                    })
                
                return transactions
        except Exception as e:
            logger.error(f"Error fetching Ethereum address transactions: {e}")
        
        return None
    
    async def get_token_transfers(self, address: str, contract_address: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Holt Token-Transfers für eine Adresse"""
        try:
            url = self.base_url
            params = {
                'module': 'account',
                'action': 'tokentx',
                'address': address,
                'sort': 'desc',
                'apikey': self.api_key
            }
            
            if contract_address:
                params['contractaddress'] = contract_address
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1' and data.get('result'):
                transfers = []
                for tx in data['result']:
                    transfers.append({
                        'tx_hash': tx.get('hash'),
                        'block_number': int(tx.get('blockNumber', 0)),
                        'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                        'from_address': tx.get('from'),
                        'to_address': tx.get('to'),
                        'contract_address': tx.get('contractAddress'),
                        'token_symbol': tx.get('tokenSymbol'),
                        'token_name': tx.get('tokenName'),
                        'token_decimal': int(tx.get('tokenDecimal', 18)),
                        'value': int(tx.get('value', 0)) / (10 ** int(tx.get('tokenDecimal', 18))),
                        'transaction_index': int(tx.get('transactionIndex', 0)),
                        'gas': int(tx.get('gas', 0)),
                        'gas_price': int(tx.get('gasPrice', 0)) / 10**9,
                        'gas_used': int(tx.get('gasUsed', 0)),
                        'confirmations': int(tx.get('confirmations', 0))
                    })
                
                return transfers
        except Exception as e:
            logger.error(f"Error fetching Ethereum token transfers: {e}")
        
        return None
    
    async def get_contract_abi(self, contract_address: str) -> Optional[Dict[str, Any]]:
        """Holt das ABI eines Smart Contracts"""
        try:
            url = self.base_url
            params = {
                'module': 'contract',
                'action': 'getabi',
                'address': contract_address,
                'apikey': self.api_key
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1':
                return {
                    'contract_address': contract_address,
                    'abi': data.get('result'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Ethereum contract ABI: {e}")
        
        return None
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Ethereum-spezifische Token-Preisabfrage"""
        try:
            url = self.base_url
            params = {
                'module': 'stats',
                'action': 'tokenprice',
                'contractaddress': token_address,
                'apikey': self.api_key
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1':
                result = data.get('result', {})
                if result and result.get('ethusd'):
                    return TokenPriceData(
                        price=float(result.get('ethusd', 0)),
                        market_cap=0,  # Nicht verfügbar
                        volume_24h=0,  # Nicht verfügbar
                        price_change_percentage_24h=0,  # Nicht verfügbar
                        source=self.name,
                        last_updated=datetime.now()
                    )
        except Exception as e:
            logger.error(f"Error fetching Ethereum token price: {e}")
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_second": 5, "requests_per_minute": 300}
