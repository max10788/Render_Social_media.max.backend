import os
import json
import asyncio
import aiohttp
import hashlib
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
from functools import wraps
from cachetools import TTLCache

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, NotFoundException

logger = get_logger(__name__)

def retry(max_retries=3, delay=1, backoff=2):
    """Decorator for retrying async functions with exponential backoff."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            last_exception = None
            
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Retry {retries + 1}/{max_retries} for {func.__name__}: {str(e)}")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
                    retries += 1
            
            logger.error(f"Max retries exceeded for {func.__name__}: {str(last_exception)}")
            raise last_exception
        return wrapper
    return decorator

class ContractMetadataService:
    """Service for retrieving contract metadata from various blockchain explorers."""
    
    def __init__(self):
        """Initialize API clients and cache."""
        # Initialize cache with TTL of 1 hour
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        
        # API keys from environment variables
        self.etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
        self.bscscan_api_key = os.getenv("BSCSCAN_API_KEY")
        self.solana_rpc_url = os.getenv("SOLANA_RPC_URL")
        self.sui_rpc_url = os.getenv("SUI_RPC_URL")
        
        # Base URLs for different explorers
        self.base_urls = {
            "ethereum": "https://api.etherscan.io/api",
            "bsc": "https://api.bscscan.com/api",
            "solana": self.solana_rpc_url,
            "sui": self.sui_rpc_url
        }
        
        # Initialize session for HTTP requests
        self.session = None
        
        # Rate limiting configuration
        self.rate_limits = {
            "ethereum": {"calls": 5, "period": 1},  # 5 calls per second
            "bsc": {"calls": 5, "period": 1},       # 5 calls per second
            "solana": {"calls": 10, "period": 1},   # 10 calls per second
            "sui": {"calls": 10, "period": 1}       # 10 calls per second
        }
        
        # Track API calls for rate limiting
        self.api_call_history = {chain: [] for chain in self.base_urls}
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def _check_rate_limit(self, chain: str) -> None:
        """Check if we're within rate limits for the given chain."""
        now = time.time()
        history = self.api_call_history[chain]
        limit = self.rate_limits[chain]
        
        # Remove calls older than the period
        history[:] = [call_time for call_time in history if now - call_time < limit["period"]]
        
        # Check if we've exceeded the limit
        if len(history) >= limit["calls"]:
            sleep_time = limit["period"] - (now - history[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached for {chain}, sleeping for {sleep_time} seconds")
                await asyncio.sleep(sleep_time)
    
    async def _record_api_call(self, chain: str) -> None:
        """Record an API call for rate limiting purposes."""
        self.api_call_history[chain].append(time.time())
    
    @retry(max_retries=3, delay=1, backoff=2)
    async def _make_request(self, url: str, params: Dict[str, Any] = None, chain: str = None, 
                           method: str = "GET", json: Dict = None) -> Dict[str, Any]:
        """Make an HTTP request with retry logic and rate limiting."""
        if chain:
            await self._check_rate_limit(chain)
        
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        try:
            logger.info(f"Making {method} request to {url} with params {params}")
            if method.upper() == "GET":
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if chain:
                            await self._record_api_call(chain)
                        return data
                    elif response.status == 404:
                        raise NotFoundException(f"Resource not found: {url}")
                    else:
                        error_text = await response.text()
                        logger.error(f"API request failed with status {response.status}: {error_text}")
                        raise APIException(f"API request failed with status {response.status}: {error_text}")
            elif method.upper() == "POST":
                async with self.session.post(url, json=json) as response:
                    if response.status == 200:
                        data = await response.json()
                        if chain:
                            await self._record_api_call(chain)
                        return data
                    elif response.status == 404:
                        raise NotFoundException(f"Resource not found: {url}")
                    else:
                        error_text = await response.text()
                        logger.error(f"API request failed with status {response.status}: {error_text}")
                        raise APIException(f"API request failed with status {response.status}: {error_text}")
        except aiohttp.ClientError as e:
            logger.error(f"Network error when calling {url}: {str(e)}")
            raise APIException(f"Network error: {str(e)}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response from {url}: {str(e)}")
            raise APIException(f"Invalid JSON response: {str(e)}")
    
    def _get_cache_key(self, method: str, address: str, chain: str) -> str:
        """Generate a cache key for the given method, address, and chain."""
        key_str = f"{method}:{address}:{chain}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    async def get_contract_metadata(self, address: str, chain: str) -> Dict[str, Any]:
        """
        Get contract metadata from blockchain explorers.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            Dictionary containing contract metadata
        """
        cache_key = self._get_cache_key("get_contract_metadata", address, chain)
        if cache_key in self.cache:
            logger.info(f"Returning cached metadata for {address} on {chain}")
            return self.cache[cache_key]
        
        try:
            if chain.lower() == "ethereum":
                metadata = await self._get_ethereum_metadata(address)
            elif chain.lower() == "bsc":
                metadata = await self._get_bsc_metadata(address)
            elif chain.lower() == "solana":
                metadata = await self._get_solana_metadata(address)
            elif chain.lower() == "sui":
                metadata = await self._get_sui_metadata(address)
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            # Cache the result
            self.cache[cache_key] = metadata
            return metadata
        except Exception as e:
            logger.error(f"Failed to get contract metadata for {address} on {chain}: {str(e)}")
            raise
    
    async def _get_ethereum_metadata(self, address: str) -> Dict[str, Any]:
        """Get contract metadata from Etherscan."""
        if not self.etherscan_api_key:
            raise APIException("ETHERSCAN_API_KEY not set")
        
        # Get contract ABI
        abi_params = {
            "module": "contract",
            "action": "getabi",
            "address": address,
            "apikey": self.etherscan_api_key
        }
        
        abi_response = await self._make_request(self.base_urls["ethereum"], abi_params, "ethereum")
        
        # Get contract source code
        source_params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": address,
            "apikey": self.etherscan_api_key
        }
        
        source_response = await self._make_request(self.base_urls["ethereum"], source_params, "ethereum")
        
        # Get contract creation transaction
        creation_params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": address,
            "apikey": self.etherscan_api_key
        }
        
        creation_response = await self._make_request(self.base_urls["ethereum"], creation_params, "ethereum")
        
        # Parse responses
        is_verified = False
        name = ""
        symbol = ""
        contract_type = ""
        creator_address = None
        deployment_date = None
        abi_hash = None
        bytecode_hash = None
        
        if source_response["status"] == "1" and source_response["result"]:
            contract_data = source_response["result"][0]
            is_verified = contract_data.get("ContractName", "") != ""
            
            if is_verified:
                name = contract_data.get("ContractName", "")
                contract_type = contract_data.get("ContractType", "")
                
                # Calculate ABI hash
                if abi_response["status"] == "1" and abi_response["result"]:
                    abi = abi_response["result"]
                    abi_hash = hashlib.sha256(abi.encode()).hexdigest()
                
                # Calculate bytecode hash
                bytecode = contract_data.get("ByteCode", "")
                if bytecode:
                    bytecode_hash = hashlib.sha256(bytecode.encode()).hexdigest()
        
        # Get creator address and deployment date
        if creation_response["status"] == "1" and creation_response["result"]:
            creation_info = creation_response["result"][0]
            creator_address = creation_info.get("contractCreator")
            tx_hash = creation_info.get("txHash")
            
            if tx_hash:
                # Get transaction details to get timestamp
                tx_params = {
                    "module": "proxy",
                    "action": "eth_getTransactionByHash",
                    "txhash": tx_hash,
                    "apikey": self.etherscan_api_key
                }
                
                tx_response = await self._make_request(self.base_urls["ethereum"], tx_params, "ethereum")
                
                if tx_response["status"] == "1" and tx_response["result"]:
                    block_number = int(tx_response["result"].get("blockNumber", "0x0"), 16)
                    if block_number > 0:
                        # Get block details to get timestamp
                        block_params = {
                            "module": "proxy",
                            "action": "eth_getBlockByNumber",
                            "tag": hex(block_number),
                            "boolean": "false",
                            "apikey": self.etherscan_api_key
                        }
                        
                        block_response = await self._make_request(self.base_urls["ethereum"], block_params, "ethereum")
                        
                        if block_response["status"] == "1" and block_response["result"]:
                            timestamp = int(block_response["result"].get("timestamp", "0x0"), 16)
                            if timestamp > 0:
                                deployment_date = datetime.fromtimestamp(timestamp)
        
        # If contract is not verified, try to get basic info from ABI
        if not is_verified and abi_response["status"] == "1" and abi_response["result"]:
            try:
                abi = json.loads(abi_response["result"])
                for item in abi:
                    if item.get("type") == "function" and item.get("name") == "name":
                        name = "Unknown Contract"
                    if item.get("type") == "function" and item.get("name") == "symbol":
                        symbol = "UNKNOWN"
                    if item.get("type") == "constructor":
                        contract_type = "Unknown"
            except json.JSONDecodeError:
                pass
        
        return {
            "name": name,
            "symbol": symbol,
            "deployment_date": deployment_date,
            "creator_address": creator_address,
            "verification_status": is_verified,
            "contract_type": contract_type,
            "abi_hash": abi_hash,
            "bytecode_hash": bytecode_hash
        }
    
    async def _get_bsc_metadata(self, address: str) -> Dict[str, Any]:
        """Get contract metadata from BscScan."""
        if not self.bscscan_api_key:
            raise APIException("BSCSCAN_API_KEY not set")
        
        # Get contract ABI
        abi_params = {
            "module": "contract",
            "action": "getabi",
            "address": address,
            "apikey": self.bscscan_api_key
        }
        
        abi_response = await self._make_request(self.base_urls["bsc"], abi_params, "bsc")
        
        # Get contract source code
        source_params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": address,
            "apikey": self.bscscan_api_key
        }
        
        source_response = await self._make_request(self.base_urls["bsc"], source_params, "bsc")
        
        # Get contract creation transaction
        creation_params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": address,
            "apikey": self.bscscan_api_key
        }
        
        creation_response = await self._make_request(self.base_urls["bsc"], creation_params, "bsc")
        
        # Parse responses (similar to Ethereum)
        is_verified = False
        name = ""
        symbol = ""
        contract_type = ""
        creator_address = None
        deployment_date = None
        abi_hash = None
        bytecode_hash = None
        
        if source_response["status"] == "1" and source_response["result"]:
            contract_data = source_response["result"][0]
            is_verified = contract_data.get("ContractName", "") != ""
            
            if is_verified:
                name = contract_data.get("ContractName", "")
                contract_type = contract_data.get("ContractType", "")
                
                # Calculate ABI hash
                if abi_response["status"] == "1" and abi_response["result"]:
                    abi = abi_response["result"]
                    abi_hash = hashlib.sha256(abi.encode()).hexdigest()
                
                # Calculate bytecode hash
                bytecode = contract_data.get("ByteCode", "")
                if bytecode:
                    bytecode_hash = hashlib.sha256(bytecode.encode()).hexdigest()
        
        # Get creator address and deployment date
        if creation_response["status"] == "1" and creation_response["result"]:
            creation_info = creation_response["result"][0]
            creator_address = creation_info.get("contractCreator")
            tx_hash = creation_info.get("txHash")
            
            if tx_hash:
                # Get transaction details to get timestamp
                tx_params = {
                    "module": "proxy",
                    "action": "eth_getTransactionByHash",
                    "txhash": tx_hash,
                    "apikey": self.bscscan_api_key
                }
                
                tx_response = await self._make_request(self.base_urls["bsc"], tx_params, "bsc")
                
                if tx_response["status"] == "1" and tx_response["result"]:
                    block_number = int(tx_response["result"].get("blockNumber", "0x0"), 16)
                    if block_number > 0:
                        # Get block details to get timestamp
                        block_params = {
                            "module": "proxy",
                            "action": "eth_getBlockByNumber",
                            "tag": hex(block_number),
                            "boolean": "false",
                            "apikey": self.bscscan_api_key
                        }
                        
                        block_response = await self._make_request(self.base_urls["bsc"], block_params, "bsc")
                        
                        if block_response["status"] == "1" and block_response["result"]:
                            timestamp = int(block_response["result"].get("timestamp", "0x0"), 16)
                            if timestamp > 0:
                                deployment_date = datetime.fromtimestamp(timestamp)
        
        # If contract is not verified, try to get basic info from ABI
        if not is_verified and abi_response["status"] == "1" and abi_response["result"]:
            try:
                abi = json.loads(abi_response["result"])
                for item in abi:
                    if item.get("type") == "function" and item.get("name") == "name":
                        name = "Unknown Contract"
                    if item.get("type") == "function" and item.get("name") == "symbol":
                        symbol = "UNKNOWN"
                    if item.get("type") == "constructor":
                        contract_type = "Unknown"
            except json.JSONDecodeError:
                pass
        
        return {
            "name": name,
            "symbol": symbol,
            "deployment_date": deployment_date,
            "creator_address": creator_address,
            "verification_status": is_verified,
            "contract_type": contract_type,
            "abi_hash": abi_hash,
            "bytecode_hash": bytecode_hash
        }
    
    async def _get_solana_metadata(self, address: str) -> Dict[str, Any]:
        """Get contract metadata from Solana RPC."""
        if not self.solana_rpc_url:
            raise APIException("SOLANA_RPC_URL not set")
        
        # Solana doesn't have a direct equivalent to Etherscan
        # We need to use RPC calls to get account information
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                address,
                {"encoding": "base64"}
            ]
        }
        
        response = await self._make_request(
            self.solana_rpc_url,
            None,
            "solana",
            method="POST",
            json=payload
        )
        
        if "result" not in response or "value" not in response["result"]:
            raise NotFoundException(f"Account {address} not found on Solana")
        
        account_info = response["result"]["value"]
        if not account_info:
            raise NotFoundException(f"Account {address} not found on Solana")
        
        # For Solana programs (contracts), we can get some basic info
        # but detailed metadata like name, symbol, etc. depends on the program
        owner = account_info.get("owner", "")
        data = account_info.get("data", [""])[0]
        executable = account_info.get("executable", False)
        
        # Calculate data hash
        data_hash = hashlib.sha256(data.encode()).hexdigest() if data else None
        
        return {
            "name": "",  # Solana doesn't have standard contract names
            "symbol": "",  # Solana doesn't have standard contract symbols
            "deployment_date": None,  # Would need additional RPC calls to get this
            "creator_address": None,  # Would need additional RPC calls to get this
            "verification_status": False,  # Solana doesn't have contract verification like Ethereum
            "contract_type": "Solana Program" if executable else "Account",
            "abi_hash": None,  # Solana doesn't have standard ABIs
            "bytecode_hash": data_hash
        }
    
    async def _get_sui_metadata(self, address: str) -> Dict[str, Any]:
        """Get contract metadata from Sui RPC."""
        if not self.sui_rpc_url:
            raise APIException("SUI_RPC_URL not set")
        
        # Sui uses a different RPC API format
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_getObject",
            "params": [
                address,
                {"showType": True, "showContent": True, "showOwner": True}
            ]
        }
        
        response = await self._make_request(
            self.sui_rpc_url,
            None,
            "sui",
            method="POST",
            json=payload
        )
        
        if "result" not in response:
            raise NotFoundException(f"Object {address} not found on Sui")
        
        object_info = response["result"]
        
        # Parse object information
        object_type = object_info.get("type", "")
        content = object_info.get("content", {})
        owner = object_info.get("owner", {})
        
        # For Sui, we can get some basic info
        # but detailed metadata depends on the object type
        is_package = object_type.startswith("0x2::package")
        
        return {
            "name": "",  # Sui doesn't have standard contract names
            "symbol": "",  # Sui doesn't have standard contract symbols
            "deployment_date": None,  # Would need additional RPC calls to get this
            "creator_address": None,  # Would need additional RPC calls to get this
            "verification_status": False,  # Sui doesn't have contract verification like Ethereum
            "contract_type": "Package" if is_package else "Object",
            "abi_hash": None,  # Sui doesn't have standard ABIs
            "bytecode_hash": None  # Would need additional RPC calls to get this
        }
    
    async def verify_contract(self, address: str, chain: str) -> bool:
        """
        Check if a contract is verified on the blockchain explorer.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            True if the contract is verified, False otherwise
        """
        cache_key = self._get_cache_key("verify_contract", address, chain)
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            if chain.lower() == "ethereum":
                result = await self._verify_ethereum_contract(address)
            elif chain.lower() == "bsc":
                result = await self._verify_bsc_contract(address)
            elif chain.lower() == "solana":
                # Solana doesn't have contract verification like Ethereum
                result = False
            elif chain.lower() == "sui":
                # Sui doesn't have contract verification like Ethereum
                result = False
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            # Cache the result
            self.cache[cache_key] = result
            return result
        except Exception as e:
            logger.error(f"Failed to verify contract {address} on {chain}: {str(e)}")
            raise
    
    async def _verify_ethereum_contract(self, address: str) -> bool:
        """Check if a contract is verified on Etherscan."""
        if not self.etherscan_api_key:
            raise APIException("ETHERSCAN_API_KEY not set")
        
        params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": address,
            "apikey": self.etherscan_api_key
        }
        
        response = await self._make_request(self.base_urls["ethereum"], params, "ethereum")
        
        if response["status"] == "1" and response["result"]:
            contract_data = response["result"][0]
            return contract_data.get("ContractName", "") != ""
        
        return False
    
    async def _verify_bsc_contract(self, address: str) -> bool:
        """Check if a contract is verified on BscScan."""
        if not self.bscscan_api_key:
            raise APIException("BSCSCAN_API_KEY not set")
        
        params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": address,
            "apikey": self.bscscan_api_key
        }
        
        response = await self._make_request(self.base_urls["bsc"], params, "bsc")
        
        if response["status"] == "1" and response["result"]:
            contract_data = response["result"][0]
            return contract_data.get("ContractName", "") != ""
        
        return False
    
    async def get_abi(self, address: str, chain: str) -> Optional[str]:
        """
        Get the ABI of a contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            Contract ABI as a JSON string, or None if not available
        """
        cache_key = self._get_cache_key("get_abi", address, chain)
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            if chain.lower() == "ethereum":
                result = await self._get_ethereum_abi(address)
            elif chain.lower() == "bsc":
                result = await self._get_bsc_abi(address)
            elif chain.lower() == "solana":
                # Solana doesn't have standard ABIs like Ethereum
                result = None
            elif chain.lower() == "sui":
                # Sui doesn't have standard ABIs like Ethereum
                result = None
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            # Cache the result
            self.cache[cache_key] = result
            return result
        except Exception as e:
            logger.error(f"Failed to get ABI for contract {address} on {chain}: {str(e)}")
            raise
    
    async def _get_ethereum_abi(self, address: str) -> Optional[str]:
        """Get the ABI of an Ethereum contract from Etherscan."""
        if not self.etherscan_api_key:
            raise APIException("ETHERSCAN_API_KEY not set")
        
        params = {
            "module": "contract",
            "action": "getabi",
            "address": address,
            "apikey": self.etherscan_api_key
        }
        
        response = await self._make_request(self.base_urls["ethereum"], params, "ethereum")
        
        if response["status"] == "1":
            return response["result"]
        
        return None
    
    async def _get_bsc_abi(self, address: str) -> Optional[str]:
        """Get the ABI of a BSC contract from BscScan."""
        if not self.bscscan_api_key:
            raise APIException("BSCSCAN_API_KEY not set")
        
        params = {
            "module": "contract",
            "action": "getabi",
            "address": address,
            "apikey": self.bscscan_api_key
        }
        
        response = await self._make_request(self.base_urls["bsc"], params, "bsc")
        
        if response["status"] == "1":
            return response["result"]
        
        return None
    
    async def get_creator_address(self, address: str, chain: str) -> Optional[str]:
        """
        Get the creator address of a contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            Creator address, or None if not available
        """
        cache_key = self._get_cache_key("get_creator_address", address, chain)
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            if chain.lower() == "ethereum":
                result = await self._get_ethereum_creator(address)
            elif chain.lower() == "bsc":
                result = await self._get_bsc_creator(address)
            elif chain.lower() == "solana":
                # Solana doesn't have a direct way to get the creator address
                result = None
            elif chain.lower() == "sui":
                # Sui doesn't have a direct way to get the creator address
                result = None
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            # Cache the result
            self.cache[cache_key] = result
            return result
        except Exception as e:
            logger.error(f"Failed to get creator address for contract {address} on {chain}: {str(e)}")
            raise
    
    async def _get_ethereum_creator(self, address: str) -> Optional[str]:
        """Get the creator address of an Ethereum contract."""
        if not self.etherscan_api_key:
            raise APIException("ETHERSCAN_API_KEY not set")
        
        # First, get the transaction that created the contract
        params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": address,
            "apikey": self.etherscan_api_key
        }
        
        response = await self._make_request(self.base_urls["ethereum"], params, "ethereum")
        
        if response["status"] == "1" and response["result"]:
            creation_info = response["result"][0]
            return creation_info.get("contractCreator")
        
        return None
    
    async def _get_bsc_creator(self, address: str) -> Optional[str]:
        """Get the creator address of a BSC contract."""
        if not self.bscscan_api_key:
            raise APIException("BSCSCAN_API_KEY not set")
        
        # First, get the transaction that created the contract
        params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": address,
            "apikey": self.bscscan_api_key
        }
        
        response = await self._make_request(self.base_urls["bsc"], params, "bsc")
        
        if response["status"] == "1" and response["result"]:
            creation_info = response["result"][0]
            return creation_info.get("contractCreator")
        
        return None
