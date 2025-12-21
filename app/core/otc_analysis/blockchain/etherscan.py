import requests
import time
from typing import List, Dict, Optional
import os
import logging

logger = logging.getLogger(__name__)

class EtherscanAPI:
    """
    Interface to Etherscan API V2 (and BSCScan, Polygonscan, etc.)
    Used for fetching transaction history, token transfers, contract info.
    """
    
    def __init__(self, chain_id: int = 1):
        self.chain_id = chain_id
        self.api_key = self._get_api_key()
        self.base_url = self._get_base_url()
        self.rate_limit_delay = 0.2  # 5 requests per second max
        self.last_request_time = 0
    
    def _get_api_key(self) -> str:
        """Get appropriate API key based on chain."""
        if self.chain_id == 1:  # Ethereum
            key = os.getenv('ETHERSCAN_API_KEY')
        elif self.chain_id == 56:  # BSC
            key = os.getenv('BSCSCAN_API_KEY')
        elif self.chain_id == 137:  # Polygon
            key = os.getenv('POLYGONSCAN_API_KEY')
        else:
            key = os.getenv('ETHERSCAN_API_KEY')
        
        if not key:
            raise ValueError(f"No API key found for chain {self.chain_id}")
        
        return key
    
    def _get_base_url(self) -> str:
        """Get base URL for API based on chain - V2 for Ethereum."""
        urls = {
            1: "https://api.etherscan.io/v2/api",  # V2 for Ethereum
            56: "https://api.bscscan.com/api",
            137: "https://api.polygonscan.com/api",
            42161: "https://api.arbiscan.io/api",
            10: "https://api-optimistic.etherscan.io/api"
        }
        return urls.get(self.chain_id, urls[1])
    
    def _rate_limit(self):
        """Enforce rate limiting."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        
        self.last_request_time = time.time()
    
    def _make_request(self, params: Dict) -> Optional[Dict]:
        """Make API request with rate limiting."""
        self._rate_limit()
        
        params['apikey'] = self.api_key
        
        # V2 API needs chainid parameter for Ethereum
        if self.chain_id == 1:
            params['chainid'] = '1'
        
        try:
            logger.info(f"🔍 Etherscan request: {params.get('action')} for {params.get('address', 'N/A')[:10]}...")
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"📡 Etherscan response status: {data.get('status')} - {data.get('message')}")
            
            if data['status'] == '1':
                result = data['result']
                if isinstance(result, list):
                    logger.info(f"✅ Received {len(result)} items from Etherscan")
                return result
            else:
                error_msg = data.get('message', 'Unknown error')
                if error_msg not in ['No transactions found', 'NOTOK']:
                    logger.warning(f"⚠️  Etherscan API: {error_msg}")
                else:
                    logger.info(f"ℹ️  {error_msg}")
                return None
        except requests.exceptions.Timeout:
            logger.error("❌ Etherscan API timeout")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Etherscan request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Etherscan error: {e}")
            return None
    
    def get_normal_transactions(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
        page: int = 1,
        offset: int = 1000
    ) -> List[Dict]:
        """
        Get normal transactions for an address.
        
        Args:
            address: Wallet address
            start_block: Starting block number
            end_block: Ending block number
            page: Page number
            offset: Number of transactions per page (max 10000)
        """
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': start_block,
            'endblock': end_block,
            'page': page,
            'offset': offset,
            'sort': 'desc'
        }
        
        result = self._make_request(params)
        return result if result else []
    
    def get_internal_transactions(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999
    ) -> List[Dict]:
        """Get internal transactions (contract calls) for an address."""
        params = {
            'module': 'account',
            'action': 'txlistinternal',
            'address': address,
            'startblock': start_block,
            'endblock': end_block,
            'sort': 'desc'
        }
        
        result = self._make_request(params)
        return result if result else []
    
    def get_erc20_transfers(
        self,
        address: Optional[str] = None,
        contract_address: Optional[str] = None,
        start_block: int = 0,
        end_block: int = 99999999,
        page: int = 1,
        offset: int = 1000
    ) -> List[Dict]:
        """
        Get ERC20 token transfers.
        
        Can filter by address or contract address.
        """
        params = {
            'module': 'account',
            'action': 'tokentx',
            'startblock': start_block,
            'endblock': end_block,
            'page': page,
            'offset': offset,
            'sort': 'desc'
        }
        
        if address:
            params['address'] = address
        if contract_address:
            params['contractaddress'] = contract_address
        
        result = self._make_request(params)
        return result if result else []
    
    def get_block_by_timestamp(self, timestamp: int, closest: str = 'before') -> Optional[int]:
        """
        Get block number by timestamp.
        
        Args:
            timestamp: Unix timestamp
            closest: 'before' or 'after'
        """
        params = {
            'module': 'block',
            'action': 'getblocknobytime',
            'timestamp': timestamp,
            'closest': closest
        }
        
        result = self._make_request(params)
        return int(result) if result else None
    
    def get_contract_abi(self, contract_address: str) -> Optional[str]:
        """Get ABI for verified contract."""
        params = {
            'module': 'contract',
            'action': 'getabi',
            'address': contract_address
        }
        
        return self._make_request(params)
    
    def get_transaction_by_hash(self, tx_hash: str) -> Optional[Dict]:
        """Get transaction details by hash."""
        params = {
            'module': 'proxy',
            'action': 'eth_getTransactionByHash',
            'txhash': tx_hash
        }
        
        return self._make_request(params)
    
    def get_gas_oracle(self) -> Optional[Dict]:
        """Get current gas prices."""
        params = {
            'module': 'gastracker',
            'action': 'gasoracle'
        }
        
        return self._make_request(params)
