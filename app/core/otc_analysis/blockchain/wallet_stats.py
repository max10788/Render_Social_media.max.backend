"""
Wallet Stats API - CORRECTED VERSION
=====================================

Fixes:
1. Moralis: Correct response parsing (no total_networth_usd)
2. Covalent: Sum all holdings[].quote values
3. DeBank: Add AccessKey authentication
4. Etherscan: Upgrade to V2 API
5. ✅ FIXED: Use correct ApiErrorTracker method names (track_call instead of record_success/record_error)
6. ✅ FIXED: Pass full addresses (42 chars) instead of truncated (10 chars)

Environment Variables:
- MORALIS_API_KEY
- COVALENT_API_KEY  
- DEBANK_ACCESS_KEY (NEW!)
- ETHERSCAN_API_KEY
"""

import requests
import logging
from typing import Dict, Optional
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class WalletStatsAPI:
    """
    Multi-tier wallet stats API with proper response parsing.
    
    Priority:
    1. Moralis (best quality, fastest)
    2. Covalent (good quality, detailed)
    3. DeBank (requires API key now)
    4. Etherscan (fallback, TX count only)
    """
    
    def __init__(self, error_tracker=None, health_monitor=None):
        self.api_error_tracker = error_tracker
        self.api_health_monitor = health_monitor
        
        # API Keys from environment
        self.moralis_key = os.getenv('MORALIS_API_KEY')
        self.covalent_key = os.getenv('COVALENT_API_KEY')
        self.debank_key = os.getenv('DEBANK_ACCESS_KEY')
        self.etherscan_key = os.getenv('ETHERSCAN_API_KEY')
        
        # API availability
        self.moralis_available = bool(self.moralis_key)
        self.covalent_available = bool(self.covalent_key)
        self.debank_available = bool(self.debank_key)
        self.etherscan_available = bool(self.etherscan_key)
        
        logger.info(f"📊 WalletStatsAPI initialized:")
        logger.info(f"   • Moralis: {'✅' if self.moralis_available else '❌'}")
        logger.info(f"   • Covalent: {'✅' if self.covalent_available else '❌'}")
        logger.info(f"   • DeBank: {'✅' if self.debank_available else '❌'}")
        logger.info(f"   • Etherscan: {'✅' if self.etherscan_available else '❌'}")


    
    def get_quick_stats(self, address: str) -> Dict:
        """
        Get quick wallet statistics with multi-tier fallback.
        
        ✅ FIXED: Now passes full 42-character address through entire chain
        
        Returns:
            {
                'total_transactions': int,
                'total_value_usd': float,
                'source': 'moralis'|'covalent'|'debank'|'etherscan'|'none',
                'data_quality': 'high'|'medium'|'low',
                'timestamp': datetime
            }
        """
        # ✅ FIXED: Keep full address, only truncate for logging
        address = address.lower().strip()
        logger.info(f"📊 Getting quick stats for {address[:10]}... (full: {address})")
        
        # Priority 1: Moralis
        if self.moralis_available and self._is_api_healthy('moralis'):
            result = self._try_moralis(address)  # ✅ Pass full address
            if result:
                return result
        
        # Priority 2: Covalent
        if self.covalent_available and self._is_api_healthy('covalent'):
            result = self._try_covalent(address)  # ✅ Pass full address
            if result:
                return result
        
        # Priority 3: DeBank
        if self.debank_available and self._is_api_healthy('debank'):
            result = self._try_debank(address)  # ✅ Pass full address
            if result:
                return result
        
        # Priority 4: Etherscan (fallback)
        if self.etherscan_available and self._is_api_healthy('etherscan'):
            result = self._try_etherscan(address)  # ✅ Pass full address
            if result:
                return result
        
        # All failed
        logger.warning(f"   ❌ All APIs failed for {address[:10]}... (full: {address})")
        return {
            'total_transactions': 0,
            'total_value_usd': 0,
            'source': 'none',
            'data_quality': 'none',
            'timestamp': datetime.utcnow()
        }
    
    # ========================================================================
    # MORALIS - CORRECTED
    # ========================================================================
    
    def _try_moralis(self, address: str) -> Optional[Dict]:
        """
        Try Moralis API.
        
        ✅ FIXED: 
        - Uses correct track_call() method
        - Receives full 42-character address
        
        Response format:
        {
            "nfts": "237",
            "collections": "159",
            "transactions": {"total": "173343"},
            "nft_transfers": {"total": "360"},
            "token_transfers": {"total": "229528"}
        }
        """
        try:
            # ✅ Use full address in API call
            url = f"https://deep-index.moralis.io/api/v2.2/wallets/{address}/stats"
            
            response = requests.get(
                url,
                headers={
                    'X-API-Key': self.moralis_key,
                    'accept': 'application/json'
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Parse transaction counts
                tx_count = int(data.get('transactions', {}).get('total', 0))
                token_transfers = int(data.get('token_transfers', {}).get('total', 0))
                
                # Total transactions = normal TX + token transfers
                total_tx = tx_count + token_transfers
                
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('moralis', success=True)
                
                logger.info(f"   ✅ Got stats from moralis: {total_tx} transactions")
                
                return {
                    'total_transactions': total_tx,
                    'total_value_usd': 0,
                    'source': 'moralis',
                    'data_quality': 'high',
                    'timestamp': datetime.utcnow(),
                    'raw_data': {
                        'normal_tx': tx_count,
                        'token_transfers': token_transfers,
                        'nft_transfers': int(data.get('nft_transfers', {}).get('total', 0))
                    }
                }
            
            elif response.status_code == 429:
                logger.warning(f"   ⏱️  Moralis rate limit")
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('moralis', success=False, error='rate_limit')
            
            else:
                logger.warning(f"   ❌ moralis failed: HTTP {response.status_code}")
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('moralis', success=False, error=f'http_{response.status_code}')
            
            return None
            
        except requests.exceptions.Timeout:
            logger.warning(f"   ⏱️  Moralis timeout")
            # ✅ FIXED: Use correct method name
            if self.api_error_tracker:
                self.api_error_tracker.track_call('moralis', success=False, error='timeout')
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ moralis failed: {type(e).__name__}")
            # ✅ FIXED: Use correct method name
            if self.api_error_tracker:
                self.api_error_tracker.track_call('moralis', success=False, error=type(e).__name__)
            return None
    
    # ========================================================================
    # COVALENT - CORRECTED
    # ========================================================================
    
    def _try_covalent(self, address: str) -> Optional[Dict]:
        """
        Try Covalent API.
        
        ✅ FIXED: Uses correct track_call() method
        """
        try:
            # ✅ Use full address in API call
            url = f"https://api.covalenthq.com/v1/eth-mainnet/address/{address}/portfolio_v2/"
            
            response = requests.get(
                url,
                headers={
                    'Authorization': f'Bearer {self.covalent_key}'
                },
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Sum all holdings[].quote values
                items = data.get('data', {}).get('items', [])
                
                total_value = 0.0
                token_count = 0
                
                for item in items:
                    holdings = item.get('holdings', [])
                    for holding in holdings:
                        quote = holding.get('quote', 0)
                        if quote:
                            total_value += float(quote)
                            token_count += 1
                
                # Estimate transaction count from token diversity
                estimated_tx = min(token_count * 10, 10000)
                
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('covalent', success=True)
                
                logger.info(f"   ✅ Got stats from covalent: ${total_value:,.2f} across {len(items)} tokens")
                
                return {
                    'total_transactions': estimated_tx,
                    'total_value_usd': total_value,
                    'source': 'covalent',
                    'data_quality': 'high' if total_value > 0 else 'medium',
                    'timestamp': datetime.utcnow(),
                    'raw_data': {
                        'total_tokens': len(items),
                        'total_holdings': token_count
                    }
                }
            
            elif response.status_code == 429:
                logger.warning(f"   ⏱️  Covalent rate limit")
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('covalent', success=False, error='rate_limit')
            
            else:
                logger.warning(f"   ❌ covalent failed: HTTP {response.status_code}")
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('covalent', success=False, error=f'http_{response.status_code}')
            
            return None
            
        except requests.exceptions.Timeout:
            logger.warning(f"   ⏱️  Covalent timeout")
            # ✅ FIXED: Use correct method name
            if self.api_error_tracker:
                self.api_error_tracker.track_call('covalent', success=False, error='timeout')
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ Covalent error: {type(e).__name__}")
            # ✅ FIXED: Use correct method name
            if self.api_error_tracker:
                self.api_error_tracker.track_call('covalent', success=False, error=type(e).__name__)
            return None
    
    # ========================================================================
    # DEBANK - CORRECTED
    # ========================================================================
    
    def _try_debank(self, address: str) -> Optional[Dict]:
        """
        Try DeBank API.
        
        ✅ FIXED: Uses correct track_call() method
        """
        try:
            # ✅ Use full address in API call
            url = f"https://pro-openapi.debank.com/v1/user/total_balance?id={address}"
            
            response = requests.get(
                url,
                headers={
                    'AccessKey': self.debank_key
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                total_usd = float(data.get('total_usd_value', 0))
                
                # Estimate TX count from portfolio value
                if total_usd > 1000000:
                    estimated_tx = 1000
                elif total_usd > 100000:
                    estimated_tx = 500
                elif total_usd > 10000:
                    estimated_tx = 100
                else:
                    estimated_tx = 50
                
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('debank', success=True)
                
                logger.info(f"   ✅ Got stats from debank: ${total_usd:,.2f}")
                
                return {
                    'total_transactions': estimated_tx,
                    'total_value_usd': total_usd,
                    'source': 'debank',
                    'data_quality': 'medium',
                    'timestamp': datetime.utcnow()
                }
            
            elif response.status_code == 401:
                logger.warning(f"   ❌ DeBank error: 401 UNAUTHORIZED - Check DEBANK_ACCESS_KEY")
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('debank', success=False, error='unauthorized')
            
            elif response.status_code == 429:
                logger.warning(f"   ⏱️  DeBank rate limit")
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('debank', success=False, error='rate_limit')
            
            else:
                logger.warning(f"   ❌ DeBank error: {response.status_code}")
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('debank', success=False, error=f'http_{response.status_code}')
            
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ DeBank error: {type(e).__name__}")
            # ✅ FIXED: Use correct method name
            if self.api_error_tracker:
                self.api_error_tracker.track_call('debank', success=False, error=type(e).__name__)
            return None
    
    # ========================================================================
    # ETHERSCAN - CORRECTED
    # ========================================================================
    
    def _try_etherscan(self, address: str) -> Optional[Dict]:
        """
        Try Etherscan API (fallback).
        
        ✅ FIXED: Uses correct track_call() method
        """
        try:
            url = "https://api.etherscan.io/v2/api"
            
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': address,  # ✅ Use full address
                'startblock': 0,
                'endblock': 99999999,
                'page': 1,
                'offset': 1,
                'sort': 'desc',
                'apikey': self.etherscan_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == '1':
                    result = data.get('result', [])
                    tx_count = len(result) if isinstance(result, list) else 0
                    
                    # ✅ FIXED: Use correct method name
                    if self.api_error_tracker:
                        self.api_error_tracker.track_call('etherscan', success=True)
                    
                    logger.info(f"   ✅ Got stats from etherscan: {tx_count}+ transactions")
                    
                    return {
                        'total_transactions': tx_count,
                        'total_value_usd': 0,
                        'source': 'etherscan',
                        'data_quality': 'low',
                        'timestamp': datetime.utcnow()
                    }
                else:
                    logger.warning(f"   ❌ etherscan error: {data.get('message')}")
                    # ✅ FIXED: Use correct method name
                    if self.api_error_tracker:
                        self.api_error_tracker.track_call('etherscan', success=False, error='api_error')
            
            else:
                logger.warning(f"   ❌ etherscan failed: HTTP {response.status_code}")
                # ✅ FIXED: Use correct method name
                if self.api_error_tracker:
                    self.api_error_tracker.track_call('etherscan', success=False, error=f'http_{response.status_code}')
            
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ Etherscan error: {type(e).__name__}")
            # ✅ FIXED: Use correct method name
            if self.api_error_tracker:
                self.api_error_tracker.track_call('etherscan', success=False, error=type(e).__name__)
            return None

    # ========================================================================
    # MORALIS ERC20 TRANSFERS
    # ========================================================================
    
    def _get_moralis_erc20_transfers(self, address: str, limit: int = 100) -> Optional[list]:
        """
        Hole ERC20 Transfers via Moralis API.
        
        Args:
            address: Wallet address (42 chars)
            limit: Max transfers to fetch
            
        Returns:
            List of transfer dicts or None
        """
        if not self.wallet_stats_api or not self.wallet_stats_api.moralis_available:
            logger.warning(f"   ⚠️ Moralis API not available")
            return None
        
        try:
            url = f"https://deep-index.moralis.io/api/v2.2/{address}/erc20/transfers"
            
            response = requests.get(
                url,
                headers={
                    'X-API-Key': self.wallet_stats_api.moralis_key,
                    'accept': 'application/json'
                },
                params={'limit': limit},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                transfers = data.get('result', [])
                
                logger.info(f"   ✅ Fetched {len(transfers)} ERC20 transfers from Moralis")
                
                # Track success
                if self.wallet_stats_api.api_error_tracker:
                    self.wallet_stats_api.api_error_tracker.track_call('moralis', success=True)
                
                return transfers
            
            elif response.status_code == 429:
                logger.warning(f"   ⏱️  Moralis rate limit")
                if self.wallet_stats_api.api_error_tracker:
                    self.wallet_stats_api.api_error_tracker.track_call('moralis', success=False, error='rate_limit')
            
            else:
                logger.warning(f"   ❌ Moralis ERC20 API failed: HTTP {response.status_code}")
                if self.wallet_stats_api.api_error_tracker:
                    self.wallet_stats_api.api_error_tracker.track_call('moralis', success=False, error=f'http_{response.status_code}')
            
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ Moralis ERC20 error: {type(e).__name__}")
            if self.wallet_stats_api.api_error_tracker:
                self.wallet_stats_api.api_error_tracker.track_call('moralis', success=False, error=type(e).__name__)
            return None
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    
    def _is_api_healthy(self, api_name: str) -> bool:
        """Check if API is healthy (not circuit broken)."""
        if not self.api_health_monitor:
            return True
        
        try:
            is_healthy = self.api_health_monitor.is_healthy(api_name)
            
            if not is_healthy:
                logger.info(f"   ⏭️  Skipping {api_name} (unhealthy)")
            
            return is_healthy
        except AttributeError:
            # If health monitor doesn't have is_healthy method, default to True
            return True


# Export
__all__ = ['WalletStatsAPI']
