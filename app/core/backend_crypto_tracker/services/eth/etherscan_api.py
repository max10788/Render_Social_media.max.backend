# services/eth/etherscan_api.py
import aiohttp
import logging
import os
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, RateLimitExceededException
from app.core.backend_crypto_tracker.utils.rate_limiter import RateLimiter

logger = get_logger(__name__)

@dataclass
class TokenHolder:
    address: str
    balance: float
    percentage: float

class EtherscanAPI:
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        self.api_key = api_key
        # Verwende die Umgebungsvariable ETHEREUM_RPC_URL oder den Standardwert
        self.base_url = base_url or os.getenv("ETHEREUM_RPC_URL", "https://api.etherscan.io/api")
        self.rate_limiter = RateLimiter()
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Interne Methode für API-Anfragen mit Rate-Limiting"""
        if not self.api_key:
            raise APIException("API key is required")
            
        params['apikey'] = self.api_key
        
        # Rate-Limiting prüfen (5 Anfragen pro Sekunde)
        if not await self.rate_limiter.acquire("etherscan", 5, 1):
            raise RateLimitExceededException("Etherscan", 5, "second")
        
        try:
            async with self.session.get(self.base_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('status') == '0':
                        message = data.get('message', 'Unknown error')
                        if message == 'NOTOK' and 'Invalid API key' in data.get('result', ''):
                            raise APIException("Invalid API key")
                        raise APIException(f"API error: {message}")
                    
                    return data
                else:
                    raise APIException(f"HTTP error: {response.status}")
        except aiohttp.ClientError as e:
            logger.error(f"Network error: {e}")
            raise APIException(f"Network error: {str(e)}")
    
    async def get_token_holders(self, token_address: str, limit: int = 100) -> List[TokenHolder]:
        """Holt die Top Token-Holder von Etherscan"""
        params = {
            'module': 'token',
            'action': 'tokenholderlist',
            'contractaddress': token_address,
            'page': 1,
            'offset': limit
        }
        
        try:
            data = await self._make_request(params)
            result = data.get('result', [])
            
            if not result:
                logger.warning(f"No holder data for token {token_address}")
                return []
            
            holders = []
            total_supply = 0
            
            # Zuerst Gesamtsumme berechnen
            for holder_data in result:
                balance = float(holder_data.get('TokenHolderQuantity', 0))
                total_supply += balance
            
            # Dann Holder mit Prozentsätzen erstellen
            for holder_data in result:
                address = holder_data.get('TokenHolderAddress', '')
                balance = float(holder_data.get('TokenHolderQuantity', 0))
                percentage = (balance / total_supply * 100) if total_supply > 0 else 0
                
                holders.append(TokenHolder(
                    address=address,
                    balance=balance,
                    percentage=percentage
                ))
            
            return holders
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching token holders: {e}")
            raise APIException(f"Error fetching token holders: {str(e)}")
    
    async def get_token_info(self, token_address: str) -> Dict[str, Any]:
        """Holt grundlegende Token-Informationen"""
        params = {
            'module': 'token',
            'action': 'tokeninfo',
            'contractaddress': token_address
        }
        
        try:
            data = await self._make_request(params)
            return data.get('result', {})
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching token info: {e}")
            raise APIException(f"Error fetching token info: {str(e)}")
    
    async def get_contract_abi(self, contract_address: str) -> List[Dict]:
        """Holt die ABI eines Smart Contracts"""
        params = {
            'module': 'contract',
            'action': 'getabi',
            'address': contract_address
        }
        
        try:
            data = await self._make_request(params)
            abi_str = data.get('result', '[]')
            import json
            return json.loads(abi_str)
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching contract ABI: {e}")
            raise APIException(f"Error fetching contract ABI: {str(e)}")
    
    async def get_transactions_by_address(self, address: str, start_block: int = 0, 
                                         end_block: int = 99999999, 
                                         sort: str = 'asc') -> List[Dict]:
        """Holt Transaktionen für eine Adresse"""
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': start_block,
            'endblock': end_block,
            'sort': sort
        }
        
        try:
            data = await self._make_request(params)
            return data.get('result', [])
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching transactions: {e}")
            raise APIException(f"Error fetching transactions: {str(e)}")

class BscScanAPI(EtherscanAPI):
    """BscScan API mit gleicher Schnittstelle wie Etherscan"""
    
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        # Verwende die Umgebungsvariable BSC_RPC_URL oder den Standardwert
        bsc_endpoint = base_url or os.getenv("BSC_RPC_URL", "https://api.bscscan.com/api")
        super().__init__(api_key=api_key, base_url=bsc_endpoint)
