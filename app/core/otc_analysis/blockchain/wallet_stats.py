"""
Wallet Stats API - Multi-Tier Fallback System
==============================================

Fetches aggregated wallet statistics with intelligent fallback chain.
Optimized for quick stats without processing individual transactions.

Fallback Chain:
1. Moralis (best - rich data)
2. Covalent (good - portfolio data)
3. DeBank (free - basic stats)
4. Etherscan (fallback - transaction count only)

Version: 1.0
Date: 2025-01-04
"""

import os
import logging
import requests
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class WalletStatsAPI:
    """
    Fetches quick wallet statistics from multiple APIs with automatic fallback.
    
    Features:
    - Multi-tier fallback chain
    - Error tracking and reporting
    - Rate limit handling
    - Graceful degradation
    
    Usage:
        stats_api = WalletStatsAPI()
        stats = stats_api.get_quick_stats("0x123...")
        
        if stats['source'] != 'none':
            print(f"Got stats from {stats['source']}")
            print(f"Total transactions: {stats['total_transactions']}")
            print(f"Total value: ${stats['total_value_usd']:,.2f}")
    """
    
    def __init__(self, error_tracker=None, health_monitor=None):
        """
        Initialize with optional error tracking.
        
        Args:
            error_tracker: ApiErrorTracker instance
            health_monitor: ApiHealthMonitor instance
        """
        # API Keys from environment
        self.moralis_key = os.getenv('MORALIS_API_KEY')
        self.covalent_key = os.getenv('COVALENT_API_KEY')
        self.etherscan_key = os.getenv('ETHERSCAN_API_KEY')
        
        # Optional tracking
        self.error_tracker = error_tracker
        self.health_monitor = health_monitor
        
        # API endpoints
        self.moralis_base = "https://deep-index.moralis.io/api/v2.2"
        self.covalent_base = "https://api.covalenthq.com/v1"
        self.debank_base = "https://pro-openapi.debank.com/v1"
        self.etherscan_base = "https://api.etherscan.io/api"
        
        # Request timeout
        self.timeout = 10
        
        # Fallback enabled?
        self.fallback_enabled = os.getenv('API_FALLBACK_ENABLED', 'true').lower() == 'true'
        
        logger.info("🔧 WalletStatsAPI initialized")
        logger.info(f"   • Moralis: {'✅' if self.moralis_key else '❌'}")
        logger.info(f"   • Covalent: {'✅' if self.covalent_key else '❌'}")
        logger.info(f"   • Etherscan: {'✅' if self.etherscan_key else '❌'}")
        logger.info(f"   • Fallback: {'✅ Enabled' if self.fallback_enabled else '❌ Disabled'}")
    
    def get_quick_stats(self, address: str) -> Dict:
        """
        Get quick wallet stats with automatic fallback.
        
        Args:
            address: Ethereum address
            
        Returns:
            Dict with:
                - total_transactions: int
                - total_value_usd: float
                - balance_usd: float (current balance)
                - source: str (which API provided data)
                - data_quality: str (high/medium/low/none)
        """
        logger.info(f"📊 Getting quick stats for {address[:10]}...")
        
        # Try APIs in order
        apis_to_try = [
            ('moralis', self._get_moralis_stats),
            ('covalent', self._get_covalent_stats),
            ('debank', self._get_debank_stats),
            ('etherscan', self._get_etherscan_stats)
        ]
        
        for api_name, api_func in apis_to_try:
            # Check health if monitor available
            if self.health_monitor and not self.health_monitor.is_api_healthy(api_name):
                logger.info(f"   ⏭️  Skipping {api_name} (unhealthy)")
                continue
            
            # Try API
            try:
                stats = api_func(address)
                
                if stats and stats.get('total_transactions', 0) > 0:
                    # Success!
                    logger.info(f"   ✅ Got stats from {api_name}")
                    
                    if self.error_tracker:
                        self.error_tracker.track_call(api_name, success=True)
                    if self.health_monitor:
                        self.health_monitor.mark_success(api_name)
                    
                    return stats
                else:
                    # No data returned
                    logger.debug(f"   ⚠️  {api_name} returned no data")
                    
                    if self.error_tracker:
                        self.error_tracker.track_call(api_name, success=False, error='no_data')
                    
            except Exception as e:
                # API call failed
                error_type = type(e).__name__
                logger.warning(f"   ❌ {api_name} failed: {error_type}")
                
                if self.error_tracker:
                    self.error_tracker.track_call(api_name, success=False, error=error_type)
                if self.health_monitor:
                    self.health_monitor.mark_failure(api_name, error_type)
            
            # If fallback disabled, stop after first attempt
            if not self.fallback_enabled:
                break
        
        # All APIs failed
        logger.warning(f"   ❌ All APIs failed for {address[:10]}")
        return self._empty_stats()
    
    # ========================================================================
    # API IMPLEMENTATIONS
    # ========================================================================
    
    def _get_moralis_stats(self, address: str) -> Optional[Dict]:
        """
        Get stats from Moralis.
        
        Endpoint: /wallets/{address}/stats
        Returns: Transaction count, total volume, balance
        """
        if not self.moralis_key:
            logger.debug("   ⚠️  Moralis API key not configured")
            return None
        
        url = f"{self.moralis_base}/wallets/{address}/stats"
        headers = {
            'X-API-Key': self.moralis_key,
            'accept': 'application/json'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=self.timeout)
            
            # Handle rate limiting
            if response.status_code == 429:
                logger.warning("   ⚠️  Moralis rate limited")
                if self.error_tracker:
                    self.error_tracker.track_call('moralis', success=False, error='rate_limit')
                return None
            
            # Handle invalid key
            if response.status_code == 401:
                logger.error("   ❌ Moralis invalid API key")
                if self.error_tracker:
                    self.error_tracker.track_call('moralis', success=False, error='invalid_key')
                if self.health_monitor:
                    self.health_monitor.disable_permanently('moralis')
                return None
            
            response.raise_for_status()
            data = response.json()
            
            # Parse Moralis response
            return {
                'total_transactions': data.get('nft_transfers', 0) + data.get('token_transfers', 0),
                'total_value_usd': data.get('total_networth_usd', 0),
                'balance_usd': data.get('total_networth_usd', 0),
                'source': 'moralis',
                'data_quality': 'high'
            }
            
        except requests.exceptions.Timeout:
            logger.warning("   ⏱️  Moralis timeout")
            if self.error_tracker:
                self.error_tracker.track_call('moralis', success=False, error='timeout')
            return None
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"   ❌ Moralis error: {e}")
            if self.error_tracker:
                self.error_tracker.track_call('moralis', success=False, error='request_error')
            return None
    
    def _get_covalent_stats(self, address: str) -> Optional[Dict]:
        """
        Get stats from Covalent.
        
        Endpoint: /v1/eth-mainnet/address/{address}/portfolio_v2/
        Returns: Portfolio balance and transaction count
        """
        if not self.covalent_key:
            logger.debug("   ⚠️  Covalent API key not configured")
            return None
        
        url = f"{self.covalent_base}/eth-mainnet/address/{address}/portfolio_v2/"
        headers = {
            'Authorization': f'Bearer {self.covalent_key}'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=self.timeout)
            
            if response.status_code == 429:
                logger.warning("   ⚠️  Covalent rate limited")
                if self.error_tracker:
                    self.error_tracker.track_call('covalent', success=False, error='rate_limit')
                return None
            
            if response.status_code == 401:
                logger.error("   ❌ Covalent invalid API key")
                if self.error_tracker:
                    self.error_tracker.track_call('covalent', success=False, error='invalid_key')
                if self.health_monitor:
                    self.health_monitor.disable_permanently('covalent')
                return None
            
            response.raise_for_status()
            data = response.json()
            
            # Parse Covalent response
            items = data.get('data', {}).get('items', [])
            total_value = sum(item.get('quote', 0) for item in items)
            
            return {
                'total_transactions': len(items) * 10,  # Estimate
                'total_value_usd': total_value,
                'balance_usd': total_value,
                'source': 'covalent',
                'data_quality': 'medium'
            }
            
        except requests.exceptions.Timeout:
            logger.warning("   ⏱️  Covalent timeout")
            if self.error_tracker:
                self.error_tracker.track_call('covalent', success=False, error='timeout')
            return None
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"   ❌ Covalent error: {e}")
            if self.error_tracker:
                self.error_tracker.track_call('covalent', success=False, error='request_error')
            return None
    
    def _get_debank_stats(self, address: str) -> Optional[Dict]:
        """
        Get stats from DeBank (FREE API).
        
        Endpoint: /v1/user/total_balance
        Returns: Total balance in USD
        """
        url = f"{self.debank_base}/user/total_balance"
        params = {
            'id': address
        }
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            
            if response.status_code == 429:
                logger.warning("   ⚠️  DeBank rate limited")
                if self.error_tracker:
                    self.error_tracker.track_call('debank', success=False, error='rate_limit')
                return None
            
            response.raise_for_status()
            data = response.json()
            
            total_usd = data.get('total_usd_value', 0)
            
            if total_usd == 0:
                return None
            
            return {
                'total_transactions': 0,  # DeBank doesn't provide this
                'total_value_usd': total_usd,
                'balance_usd': total_usd,
                'source': 'debank',
                'data_quality': 'low'
            }
            
        except requests.exceptions.Timeout:
            logger.warning("   ⏱️  DeBank timeout")
            if self.error_tracker:
                self.error_tracker.track_call('debank', success=False, error='timeout')
            return None
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"   ❌ DeBank error: {e}")
            if self.error_tracker:
                self.error_tracker.track_call('debank', success=False, error='request_error')
            return None
    
    def _get_etherscan_stats(self, address: str) -> Optional[Dict]:
        """
        Get stats from Etherscan (FALLBACK).
        
        Endpoint: /api?module=account&action=txlist
        Returns: Transaction count only
        """
        if not self.etherscan_key:
            logger.debug("   ⚠️  Etherscan API key not configured")
            return None
        
        url = self.etherscan_base
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': 0,
            'endblock': 99999999,
            'page': 1,
            'offset': 1,  # Only get 1 transaction to check if any exist
            'sort': 'desc',
            'apikey': self.etherscan_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            
            if response.status_code == 429:
                logger.warning("   ⚠️  Etherscan rate limited")
                if self.error_tracker:
                    self.error_tracker.track_call('etherscan', success=False, error='rate_limit')
                return None
            
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') != '1':
                return None
            
            # Etherscan only provides transaction count
            return {
                'total_transactions': len(data.get('result', [])),
                'total_value_usd': 0,  # Not available
                'balance_usd': 0,
                'source': 'etherscan',
                'data_quality': 'low'
            }
            
        except requests.exceptions.Timeout:
            logger.warning("   ⏱️  Etherscan timeout")
            if self.error_tracker:
                self.error_tracker.track_call('etherscan', success=False, error='timeout')
            return None
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"   ❌ Etherscan error: {e}")
            if self.error_tracker:
                self.error_tracker.track_call('etherscan', success=False, error='request_error')
            return None
    
    def _empty_stats(self) -> Dict:
        """Return empty stats when all APIs fail."""
        return {
            'total_transactions': 0,
            'total_value_usd': 0,
            'balance_usd': 0,
            'source': 'none',
            'data_quality': 'none'
        }


# Export
__all__ = ['WalletStatsAPI']
