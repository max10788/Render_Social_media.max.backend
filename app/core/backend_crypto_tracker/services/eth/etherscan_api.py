# services/eth/etherscan_api.py
import aiohttp
import logging
import os
import json
import asyncio
import time
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
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
    def __init__(self, etherscan_key: Optional[str] = None, bscscan_key: Optional[str] = None):
        self.etherscan_key = etherscan_key or os.getenv('ETHERSCAN_API_KEY')
        self.bscscan_key = bscscan_key or os.getenv('BSCSCAN_API_KEY')
        
        # URLs für verschiedene Blockchains
        self.etherscan_url = os.getenv("ETHEREUM_RPC_URL", "https://api.etherscan.io/api")
        self.bscscan_url = os.getenv("BSC_RPC_URL", "https://api.bscscan.com/api")
        
        self.rate_limiter = RateLimiter()
        self.session = None
        
        # Logging der API-Schlüssel (maskiert)
        if self.etherscan_key:
            if len(self.etherscan_key) > 8:
                masked_etherscan = self.etherscan_key[:4] + "..." + self.etherscan_key[-4:]
            else:
                masked_etherscan = "***"
            logger.info(f"Etherscan API key configured: {masked_etherscan}")
        else:
            logger.warning("No Etherscan API key configured")
            
        if self.bscscan_key:
            if len(self.bscscan_key) > 8:
                masked_bscscan = self.bscscan_key[:4] + "..." + self.bscscan_key[-4:]
            else:
                masked_bscscan = "***"
            logger.info(f"BscScan API key configured: {masked_bscscan}")
        else:
            logger.warning("No BscScan API key configured")
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _make_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Interne Methode für API-Anfragen mit Rate-Limiting"""
        # Rate-Limiting prüfen (5 Anfragen pro Sekunde)
        if not await self.rate_limiter.acquire("etherscan", 5, 1):
            raise RateLimitExceededException("Etherscan", 5, "second")
        
        try:
            async with self.session.get(url, params=params) as response:
                logger.debug(f"API request to {url} with params {params}")
                
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('status') == '0':
                        message = data.get('message', 'Unknown error')
                        result = data.get('result', '')
                        
                        if message == 'NOTOK' and 'Invalid API key' in result:
                            raise APIException("Invalid API key")
                        
                        logger.error(f"API error: {message} - {result}")
                        raise APIException(f"API error: {message}")
                    
                    return data
                else:
                    error_text = await response.text()
                    logger.error(f"HTTP error {response.status}: {error_text}")
                    raise APIException(f"HTTP error: {response.status}")
        except aiohttp.ClientError as e:
            logger.error(f"Network error: {e}")
            raise APIException(f"Network error: {str(e)}")
    
    async def get_token_holders(self, token_address: str, chain: str = 'ethereum', limit: int = 100) -> List[Dict[str, Any]]:
        """Holt die Top Token-Holder von Etherscan/BscScan"""
        try:
            if chain == 'ethereum':
                if not self.etherscan_key:
                    logger.warning("No Etherscan API key provided")
                    return []
                
                params = {
                    'module': 'token',
                    'action': 'tokenholderlist',
                    'contractaddress': token_address,
                    'page': '1',
                    'offset': str(limit),
                    'apikey': self.etherscan_key
                }
                
                data = await self._make_request(self.etherscan_url, params)
                return data.get('result', [])
                
            elif chain == 'bsc':
                if not self.bscscan_key:
                    logger.warning("No BscScan API key provided")
                    return []
                
                params = {
                    'module': 'token',
                    'action': 'tokenholderlist',
                    'contractaddress': token_address,
                    'page': '1',
                    'offset': str(limit),
                    'apikey': self.bscscan_key
                }
                
                data = await self._make_request(self.bscscan_url, params)
                return data.get('result', [])
            else:
                logger.warning(f"Unsupported chain for token holders: {chain}")
                return []
                
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching token holders for {token_address} on {chain}: {e}")
            return []
    
    async def get_wallet_transactions(self, wallet_address: str, chain: str = 'ethereum') -> Dict[str, Any]:
        """Holt Transaktionsdaten für eine Wallet"""
        try:
            if chain == 'ethereum':
                if not self.etherscan_key:
                    logger.warning("No Etherscan API key provided")
                    return {}
                
                params = {
                    'module': 'account',
                    'action': 'txlist',
                    'address': wallet_address,
                    'startblock': '0',
                    'endblock': '99999999',
                    'page': '1',
                    'offset': '10',
                    'sort': 'asc',
                    'apikey': self.etherscan_key
                }
                
                data = await self._make_request(self.etherscan_url, params)
                
                if data.get('status') == '1' and data.get('result'):
                    transactions = data['result']
                    
                    # Berechne Metriken
                    tx_count = len(transactions)
                    first_tx_time = None
                    last_tx_time = None
                    
                    if transactions:
                        # Erste und letzte Transaktion finden
                        first_tx = transactions[0]
                        last_tx = transactions[-1]
                        
                        if first_tx.get('timeStamp'):
                            first_tx_time = datetime.fromtimestamp(int(first_tx['timeStamp']))
                        
                        if last_tx.get('timeStamp'):
                            last_tx_time = datetime.fromtimestamp(int(last_tx['timeStamp']))
                    
                    return {
                        'tx_count': tx_count,
                        'first_tx_time': first_tx_time,
                        'last_tx_time': last_tx_time,
                        'transactions': transactions[:5]  # Nur die ersten 5 Transaktionen
                    }
                else:
                    logger.error(f"Etherscan API error: {data.get('message', 'Unknown error')}")
                    return {}
                    
            elif chain == 'bsc':
                if not self.bscscan_key:
                    logger.warning("No BscScan API key provided")
                    return {}
                
                params = {
                    'module': 'account',
                    'action': 'txlist',
                    'address': wallet_address,
                    'startblock': '0',
                    'endblock': '99999999',
                    'page': '1',
                    'offset': '10',
                    'sort': 'asc',
                    'apikey': self.bscscan_key
                }
                
                data = await self._make_request(self.bscscan_url, params)
                
                if data.get('status') == '1' and data.get('result'):
                    transactions = data['result']
                    
                    # Berechne Metriken
                    tx_count = len(transactions)
                    first_tx_time = None
                    last_tx_time = None
                    
                    if transactions:
                        # Erste und letzte Transaktion finden
                        first_tx = transactions[0]
                        last_tx = transactions[-1]
                        
                        if first_tx.get('timeStamp'):
                            first_tx_time = datetime.fromtimestamp(int(first_tx['timeStamp']))
                        
                        if last_tx.get('timeStamp'):
                            last_tx_time = datetime.fromtimestamp(int(last_tx['timeStamp']))
                    
                    return {
                        'tx_count': tx_count,
                        'first_tx_time': first_tx_time,
                        'last_tx_time': last_tx_time,
                        'transactions': transactions[:5]  # Nur die ersten 5 Transaktionen
                    }
                else:
                    logger.error(f"BscScan API error: {data.get('message', 'Unknown error')}")
                    return {}
            else:
                logger.warning(f"Unsupported chain for wallet transactions: {chain}")
                return {}
                
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching wallet transactions for {wallet_address} on {chain}: {e}")
            return {}
    
    async def get_contract_creation_tx(self, contract_address: str, chain: str = 'ethereum') -> Optional[str]:
        """Holt die Erstellungs-Transaktion eines Contracts"""
        try:
            if chain == 'ethereum':
                if not self.etherscan_key:
                    logger.warning("No Etherscan API key provided")
                    return None
                
                params = {
                    'module': 'contract',
                    'action': 'getcontractcreation',
                    'contractaddresses': contract_address,
                    'apikey': self.etherscan_key
                }
                
                data = await self._make_request(self.etherscan_url, params)
                
                if data.get('status') == '1' and data.get('result'):
                    return data['result'][0].get('txHash')
                else:
                    logger.error(f"Etherscan API error: {data.get('message', 'Unknown error')}")
                    return None
                    
            elif chain == 'bsc':
                if not self.bscscan_key:
                    logger.warning("No BscScan API key provided")
                    return None
                
                params = {
                    'module': 'contract',
                    'action': 'getcontractcreation',
                    'contractaddresses': contract_address,
                    'apikey': self.bscscan_key
                }
                
                data = await self._make_request(self.bscscan_url, params)
                
                if data.get('status') == '1' and data.get('result'):
                    return data['result'][0].get('txHash')
                else:
                    logger.error(f"BscScan API error: {data.get('message', 'Unknown error')}")
                    return None
            else:
                logger.warning(f"Unsupported chain for contract creation: {chain}")
                return None
                
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching contract creation tx for {contract_address} on {chain}: {e}")
            return None
    
    async def is_contract_verified(self, contract_address: str, chain: str = 'ethereum') -> bool:
        """Prüft, ob ein Contract verifiziert ist"""
        try:
            if chain == 'ethereum':
                if not self.etherscan_key:
                    logger.warning("No Etherscan API key provided")
                    return False
                
                params = {
                    'module': 'contract',
                    'action': 'getabi',
                    'address': contract_address,
                    'apikey': self.etherscan_key
                }
                
                data = await self._make_request(self.etherscan_url, params)
                
                # Wenn die Antwort eine ABI enthält, ist der Contract verifiziert
                return data.get('status') == '1' and data.get('message') != 'Contract source code not verified'
                
            elif chain == 'bsc':
                if not self.bscscan_key:
                    logger.warning("No BscScan API key provided")
                    return False
                
                params = {
                    'module': 'contract',
                    'action': 'getabi',
                    'address': contract_address,
                    'apikey': self.bscscan_key
                }
                
                data = await self._make_request(self.bscscan_url, params)
                
                # Wenn die Antwort eine ABI enthält, ist der Contract verifiziert
                return data.get('status') == '1' and data.get('message') != 'Contract source code not verified'
            else:
                logger.warning(f"Unsupported chain for contract verification: {chain}")
                return False
                
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error checking contract verification for {contract_address} on {chain}: {e}")
            return False
    
    async def get_token_info(self, token_address: str, chain: str = 'ethereum') -> Dict[str, Any]:
        """Holt grundlegende Token-Informationen"""
        try:
            if chain == 'ethereum':
                if not self.etherscan_key:
                    logger.warning("No Etherscan API key provided")
                    return {}
                
                params = {
                    'module': 'token',
                    'action': 'tokeninfo',
                    'contractaddress': token_address,
                    'apikey': self.etherscan_key
                }
                
                data = await self._make_request(self.etherscan_url, params)
                return data.get('result', {})
                
            elif chain == 'bsc':
                if not self.bscscan_key:
                    logger.warning("No BscScan API key provided")
                    return {}
                
                params = {
                    'module': 'token',
                    'action': 'tokeninfo',
                    'contractaddress': token_address,
                    'apikey': self.bscscan_key
                }
                
                data = await self._make_request(self.bscscan_url, params)
                return data.get('result', {})
            else:
                logger.warning(f"Unsupported chain for token info: {chain}")
                return {}
                
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching token info for {token_address} on {chain}: {e}")
            return {}
    
    async def get_contract_abi(self, contract_address: str, chain: str = 'ethereum') -> List[Dict]:
        """Holt die ABI eines Smart Contracts"""
        try:
            if chain == 'ethereum':
                if not self.etherscan_key:
                    logger.warning("No Etherscan API key provided")
                    return []
                
                params = {
                    'module': 'contract',
                    'action': 'getabi',
                    'address': contract_address,
                    'apikey': self.etherscan_key
                }
                
                data = await self._make_request(self.etherscan_url, params)
                abi_str = data.get('result', '[]')
                return json.loads(abi_str)
                
            elif chain == 'bsc':
                if not self.bscscan_key:
                    logger.warning("No BscScan API key provided")
                    return []
                
                params = {
                    'module': 'contract',
                    'action': 'getabi',
                    'address': contract_address,
                    'apikey': self.bscscan_key
                }
                
                data = await self._make_request(self.bscscan_url, params)
                abi_str = data.get('result', '[]')
                return json.loads(abi_str)
            else:
                logger.warning(f"Unsupported chain for contract ABI: {chain}")
                return []
                
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching contract ABI for {contract_address} on {chain}: {e}")
            return []
    
    async def get_transactions_by_address(self, address: str, chain: str = 'ethereum', 
                                         start_block: int = 0, end_block: int = 99999999, 
                                         sort: str = 'asc') -> List[Dict]:
        """Holt Transaktionen für eine Adresse"""
        try:
            if chain == 'ethereum':
                if not self.etherscan_key:
                    logger.warning("No Etherscan API key provided")
                    return []
                
                params = {
                    'module': 'account',
                    'action': 'txlist',
                    'address': address,
                    'startblock': str(start_block),
                    'endblock': str(end_block),
                    'sort': sort,
                    'apikey': self.etherscan_key
                }
                
                data = await self._make_request(self.etherscan_url, params)
                return data.get('result', [])
                
            elif chain == 'bsc':
                if not self.bscscan_key:
                    logger.warning("No BscScan API key provided")
                    return []
                
                params = {
                    'module': 'account',
                    'action': 'txlist',
                    'address': address,
                    'startblock': str(start_block),
                    'endblock': str(end_block),
                    'sort': sort,
                    'apikey': self.bscscan_key
                }
                
                data = await self._make_request(self.bscscan_url, params)
                return data.get('result', [])
            else:
                logger.warning(f"Unsupported chain for transactions: {chain}")
                return []
                
        except APIException:
            raise
        except Exception as e:
            logger.error(f"Error fetching transactions for {address} on {chain}: {e}")
            return []

class BscScanAPI(EtherscanAPI):
    """BscScan API mit gleicher Schnittstelle wie Etherscan"""
    
    def __init__(self, api_key: Optional[str] = None):
        # Verwende die Umgebungsvariable BSC_RPC_URL oder den Standardwert
        bsc_endpoint = os.getenv("BSC_RPC_URL", "https://api.bscscan.com/api")
        super().__init__(etherscan_key=None, bscscan_key=api_key)
        self.bscscan_url = bsc_endpoint
