"""
Etherscan API Client
====================

Interface to Etherscan API V2 with live ETH price support.

✅ FIXED v2.2: V2 Price API Support + Detailed NOTOK Debugging
"""

import requests
import time
from typing import List, Dict, Optional
import os
import logging

logger = logging.getLogger(__name__)


class EtherscanAPI:
    """
    Interface to Etherscan API V2 (and BSCScan, Polygonscan, etc.)
    
    ✨ NEW: Added live ETH price fetching
    ✅ ENHANCED: V2 + V1 fallback for price API
    ✅ ENHANCED: Detailed error logging for debugging
    """
    
    def __init__(self, chain_id: int = 1):
        self.chain_id = chain_id
        self.api_key = self._get_api_key()
        self.base_url = self._get_base_url()
        self.rate_limit_delay = 0.2  # 5 requests per second max
        self.last_request_time = 0
        self.session = requests.Session()  # ✅ Add session for reuse
    
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
            
            response = self.session.get(self.base_url, params=params, timeout=10)
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
        """Get normal transactions for an address."""
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
        """Get ERC20 token transfers."""
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
        """Get block number by timestamp."""
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

    def get_balance(self, address: str) -> Optional[Dict]:
        """
        Get ETH balance for an address.
        
        Returns dict with balance_wei, balance_eth
        """
        params = {
            'module': 'account',
            'action': 'balance',
            'address': address,
            'tag': 'latest'
        }
        
        result = self._make_request(params)
        
        if result is None:
            return None
        
        try:
            balance_wei = int(result)
            balance_eth = balance_wei / 1e18
            
            # Sanity check
            if balance_eth > 1_000_000:
                logger.warning(f"⚠️ Suspicious balance: {balance_eth} ETH")
                return None
            
            logger.info(f"💰 Balance: {balance_eth:.4f} ETH")
            
            return {
                "balance_wei": balance_wei,
                "balance_eth": balance_eth
            }
        except (ValueError, TypeError) as e:
            logger.error(f"❌ Error parsing balance: {e}")
            return None
    
    def get_recent_transactions(
        self, 
        address: str, 
        limit: int = 100
    ) -> List[Dict]:
        """
        Get recent normal transactions for an address.
        Wrapper around get_normal_transactions with sensible defaults.
        """
        return self.get_normal_transactions(
            address=address,
            page=1,
            offset=limit
        )
    
    def get_transaction_count_simple(self, address: str) -> int:
        """Get total transaction count for address (proxy method)."""
        params = {
            'module': 'proxy',
            'action': 'eth_getTransactionCount',
            'address': address,
            'tag': 'latest'
        }
        
        result = self._make_request(params)
        
        if result is None:
            return 0
        
        try:
            # Result is hex string
            count = int(result, 16)
            return count
        except (ValueError, TypeError):
            return 0

    # ========================================================================
    # ✨ ETH PRICE METHODS - V2 + V1 FALLBACK
    # ========================================================================

    def get_eth_price_usd(self) -> Optional[float]:
        """
        Get current ETH price in USD from Etherscan.
        
        ✅ STRATEGY: Try V2 first (future-proof), fallback to V1 if needed
        ✅ ENHANCED: Detailed error logging for debugging NOTOK errors
        
        Returns:
            Current ETH price in USD or None
        
        Etherscan V2 Response:
        {
          "status": "1",
          "message": "OK",
          "result": {
            "ethbtc": "0.05297",
            "ethbtc_timestamp": "1703875234",
            "ethusd": "3421.42",
            "ethusd_timestamp": "1703875234"
          }
        }
        """
        # Mask API key for logging (show first/last 4 chars)
        masked_key = f"{self.api_key[:4]}...{self.api_key[-4:]}" if len(self.api_key) > 8 else "****"
        
        # ================================================================
        # STRATEGY 1: Try V2 API (Future-proof)
        # ================================================================
        
        logger.info(f"📡 Strategy 1: Trying Etherscan V2 Price API...")
        
        v2_price = self._try_v2_price_api(masked_key)
        if v2_price is not None:
            logger.info(f"✅ V2 API Success: ${v2_price:,.2f}")
            return v2_price
        
        logger.warning(f"⚠️ V2 API failed, trying V1 fallback...")
        
        # ================================================================
        # STRATEGY 2: Fallback to V1 API (Legacy, deprecated 2025-08-15)
        # ================================================================
        
        logger.info(f"📡 Strategy 2: Trying Etherscan V1 Price API (fallback)...")
        
        v1_price = self._try_v1_price_api(masked_key)
        if v1_price is not None:
            logger.info(f"✅ V1 API Success: ${v1_price:,.2f}")
            logger.warning(f"⚠️ Using deprecated V1 API - migrate to V2 before Aug 2025!")
            return v1_price
        
        # ================================================================
        # BOTH FAILED - Log detailed error
        # ================================================================
        
        logger.error(f"❌ ALL STRATEGIES FAILED - Cannot fetch ETH price")
        logger.error(f"   📊 API Plan: FREE (100k calls/day, 5 calls/sec)")
        logger.error(f"   🔑 API Key: {masked_key}")
        logger.error(f"   💡 Possible causes:")
        logger.error(f"      1. FREE plan doesn't include Price API")
        logger.error(f"      2. Rate limit exceeded (5 calls/sec)")
        logger.error(f"      3. API key invalid or expired")
        logger.error(f"      4. Price endpoint not available in V2 yet")
        logger.error(f"   🔗 Check: https://docs.etherscan.io/api-endpoints/stats")
        
        return None
    
    def _try_v2_price_api(self, masked_key: str) -> Optional[float]:
        """
        Try V2 Price API.
        
        V2 Endpoint: https://api.etherscan.io/v2/api
        Module: stats
        Action: ethprice
        ChainID: 1 (required for V2)
        """
        try:
            v2_url = "https://api.etherscan.io/v2/api"
            
            params = {
                'module': 'stats',
                'action': 'ethprice',
                'chainid': '1',  # Required for V2
                'apikey': self.api_key
            }
            
            # Rate limit
            self._rate_limit()
            
            # Make request
            response = self.session.get(v2_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Log response
            logger.info(f"   📋 V2 Response: status={data.get('status')}, msg={data.get('message')}")
            
            # Parse response
            if data.get('status') == '1' and data.get('result'):
                eth_price = float(data['result']['ethusd'])
                
                # Sanity check: ETH price should be $100-$10,000
                if 100 <= eth_price <= 10000:
                    return eth_price
                else:
                    logger.warning(f"   ⚠️ Suspicious price: ${eth_price:,.2f}")
                    return None
            else:
                # Log detailed error
                error_msg = data.get('message', 'Unknown')
                result_data = data.get('result', 'No result')
                
                logger.warning(f"   ❌ V2 Failed: {error_msg}")
                if error_msg == 'NOTOK':
                    logger.warning(f"      • Result: {result_data}")
                    logger.warning(f"      • This usually means V2 doesn't support ethprice yet")
                
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"   ⏱️  V2 API timeout")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"   ❌ V2 request failed: {e}")
            return None
        except (ValueError, KeyError) as e:
            logger.warning(f"   ❌ V2 parse error: {e}")
            return None
        except Exception as e:
            logger.warning(f"   ❌ V2 unexpected error: {e}")
            return None
    
    def _try_v1_price_api(self, masked_key: str) -> Optional[float]:
        """
        Try V1 Price API (fallback).
        
        V1 Endpoint: https://api.etherscan.io/api (NO v2!)
        Module: stats
        Action: ethprice
        No ChainID parameter
        """
        try:
            v1_url = "https://api.etherscan.io/api"  # V1 - no /v2/
            
            params = {
                'module': 'stats',
                'action': 'ethprice',
                'apikey': self.api_key
                # NO chainid for V1!
            }
            
            # Rate limit
            self._rate_limit()
            
            # Make request
            response = self.session.get(v1_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Log response
            logger.info(f"   📋 V1 Response: status={data.get('status')}, msg={data.get('message')}")
            
            # Parse response
            if data.get('status') == '1' and data.get('result'):
                eth_price = float(data['result']['ethusd'])
                
                # Sanity check
                if 100 <= eth_price <= 10000:
                    return eth_price
                else:
                    logger.warning(f"   ⚠️ Suspicious price: ${eth_price:,.2f}")
                    return None
            else:
                # Log detailed error
                error_msg = data.get('message', 'Unknown')
                result_data = data.get('result', 'No result')
                
                logger.warning(f"   ❌ V1 Failed: {error_msg}")
                if error_msg == 'NOTOK':
                    logger.warning(f"      • Result: {result_data}")
                    logger.warning(f"      • FREE plan may not have access to Price API")
                    logger.warning(f"      • Consider upgrading to PRO plan")
                
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"   ⏱️  V1 API timeout")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"   ❌ V1 request failed: {e}")
            return None
        except (ValueError, KeyError) as e:
            logger.warning(f"   ❌ V1 parse error: {e}")
            return None
        except Exception as e:
            logger.warning(f"   ❌ V1 unexpected error: {e}")
            return None
    
    def get_historical_eth_price(self, timestamp: int) -> Optional[float]:
        """
        Get historical ETH price at specific timestamp.
        
        Note: Etherscan doesn't provide historical prices directly.
        This would require an external service like CoinGecko or CryptoCompare.
        
        For now, returns current price as approximation.
        
        TODO: Implement with CryptoCompare or CoinGecko historical API
        """
        logger.warning(f"⚠️ Historical ETH price not available, using current price")
        return self.get_eth_price_usd()
