"""
Wallet Stats API - CORRECTED VERSION
=====================================

Fixes:
1. Moralis: Correct response parsing (no total_networth_usd)
2. Covalent: Sum all holdings[].quote values
3. DeBank: Add AccessKey authentication
4. Etherscan: Upgrade to V2 API

Environment Variables:
- MORALIS_API_KEY
- COVALENT_API_KEY  
- DEBANK_ACCESS_KEY (NEW!)
- ETHERSCAN_API_KEY
"""

import requests
import logging
from typing import Dict, Optional
import os
from datetime import datetime

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
        self.debank_key = os.getenv('DEBANK_ACCESS_KEY')  # NEW!
        self.etherscan_key = os.getenv('ETHERSCAN_API_KEY')
        
        # API availability
        self.moralis_available = bool(self.moralis_key)
        self.covalent_available = bool(self.covalent_key)
        self.debank_available = bool(self.debank_key)  # NEW!
        self.etherscan_available = bool(self.etherscan_key)
        
        logger.info(f"📊 WalletStatsAPI initialized:")
        logger.info(f"   • Moralis: {'✅' if self.moralis_available else '❌'}")
        logger.info(f"   • Covalent: {'✅' if self.covalent_available else '❌'}")
        logger.info(f"   • DeBank: {'✅' if self.debank_available else '❌'}")
        logger.info(f"   • Etherscan: {'✅' if self.etherscan_available else '❌'}")
    
    def get_quick_stats(self, address: str) -> Dict:
        """
        Get quick wallet statistics with multi-tier fallback.
        
        Returns:
            {
                'total_transactions': int,
                'total_value_usd': float,
                'source': 'moralis'|'covalent'|'debank'|'etherscan'|'none',
                'data_quality': 'high'|'medium'|'low',
                'timestamp': datetime
            }
        """
        address = address.lower().strip()
        logger.info(f"📊 Getting quick stats for {address[:10]}...")
        
        # Priority 1: Moralis
        if self.moralis_available and self._is_api_healthy('moralis'):
            result = self._try_moralis(address)
            if result:
                return result
        
        # Priority 2: Covalent
        if self.covalent_available and self._is_api_healthy('covalent'):
            result = self._try_covalent(address)
            if result:
                return result
        
        # Priority 3: DeBank
        if self.debank_available and self._is_api_healthy('debank'):
            result = self._try_debank(address)
            if result:
                return result
        
        # Priority 4: Etherscan (fallback)
        if self.etherscan_available and self._is_api_healthy('etherscan'):
            result = self._try_etherscan(address)
            if result:
                return result
        
        # All failed
        logger.warning(f"   ❌ All APIs failed for {address[:10]}")
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
        
        CORRECTED: Moralis gives transaction COUNTS, not USD values!
        
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
                
                # ✅ CORRECTED: Parse transaction counts
                tx_count = int(data.get('transactions', {}).get('total', 0))
                token_transfers = int(data.get('token_transfers', {}).get('total', 0))
                
                # Total transactions = normal TX + token transfers
                total_tx = tx_count + token_transfers
                
                if self.api_error_tracker:
                    self.api_error_tracker.record_success('moralis')
                
                logger.info(f"   ✅ Got stats from moralis: {total_tx} transactions")
                
                return {
                    'total_transactions': total_tx,
                    'total_value_usd': 0,  # ❌ Moralis doesn't provide USD value in this endpoint
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
                if self.api_error_tracker:
                    self.api_error_tracker.record_error('moralis', 'rate_limit')
            
            else:
                logger.warning(f"   ❌ moralis failed: HTTP {response.status_code}")
                if self.api_error_tracker:
                    self.api_error_tracker.record_error('moralis', f'http_{response.status_code}')
            
            return None
            
        except requests.exceptions.Timeout:
            logger.warning(f"   ⏱️  Moralis timeout")
            if self.api_error_tracker:
                self.api_error_tracker.record_error('moralis', 'timeout')
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ moralis failed: {type(e).__name__}")
            if self.api_error_tracker:
                self.api_error_tracker.record_error('moralis', type(e).__name__)
            return None
    
    # ========================================================================
    # COVALENT - CORRECTED
    # ========================================================================
    
    def _try_covalent(self, address: str) -> Optional[Dict]:
        """
        Try Covalent API.
        
        CORRECTED: Must sum all holdings[].quote values!
        
        Response format:
        {
            "data": {
                "items": [
                    {
                        "contract_name": "USD Coin",
                        "holdings": [
                            {"quote": 1234.56, ...},
                            {"quote": 789.01, ...}
                        ]
                    },
                    ...
                ]
            }
        }
        """
        try:
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
                
                # ✅ CORRECTED: Sum all holdings[].quote values
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
                # More tokens usually means more transactions
                estimated_tx = min(token_count * 10, 10000)  # Rough estimate
                
                if self.api_error_tracker:
                    self.api_error_tracker.record_success('covalent')
                
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
                if self.api_error_tracker:
                    self.api_error_tracker.record_error('covalent', 'rate_limit')
            
            else:
                logger.warning(f"   ❌ covalent failed: HTTP {response.status_code}")
                if self.api_error_tracker:
                    self.api_error_tracker.record_error('covalent', f'http_{response.status_code}')
            
            return None
            
        except requests.exceptions.Timeout:
            logger.warning(f"   ⏱️  Covalent timeout")
            if self.api_error_tracker:
                self.api_error_tracker.record_error('covalent', 'timeout')
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ Covalent error: {type(e).__name__}")
            if self.api_error_tracker:
                self.api_error_tracker.record_error('covalent', type(e).__name__)
            return None
    
    # ========================================================================
    # DEBANK - CORRECTED (NOW REQUIRES API KEY)
    # ========================================================================
    
    def _try_debank(self, address: str) -> Optional[Dict]:
        """
        Try DeBank API.
        
        CORRECTED: Now requires AccessKey authentication!
        
        Endpoint: https://pro-openapi.debank.com/v1/user/total_balance
        Header: AccessKey: {your_key}
        
        Response format:
        {
            "total_usd_value": 1234567.89,
            "chain_list": [
                {
                    "id": "eth",
                    "usd_value": 1234567.89
                }
            ]
        }
        """
        try:
            url = f"https://pro-openapi.debank.com/v1/user/total_balance?id={address}"
            
            response = requests.get(
                url,
                headers={
                    'AccessKey': self.debank_key  # ✅ CORRECTED: Use AccessKey header
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Parse response
                total_usd = float(data.get('total_usd_value', 0))
                
                # Estimate TX count from portfolio value
                # Rough heuristic: higher value = more transactions
                if total_usd > 1000000:
                    estimated_tx = 1000
                elif total_usd > 100000:
                    estimated_tx = 500
                elif total_usd > 10000:
                    estimated_tx = 100
                else:
                    estimated_tx = 50
                
                if self.api_error_tracker:
                    self.api_error_tracker.record_success('debank')
                
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
                if self.api_error_tracker:
                    self.api_error_tracker.record_error('debank', 'unauthorized')
            
            elif response.status_code == 429:
                logger.warning(f"   ⏱️  DeBank rate limit")
                if self.api_error_tracker:
                    self.api_error_tracker.record_error('debank', 'rate_limit')
            
            else:
                logger.warning(f"   ❌ DeBank error: {response.status_code}")
                if self.api_error_tracker:
                    self.api_error_tracker.record_error('debank', f'http_{response.status_code}')
            
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ DeBank error: {type(e).__name__}")
            if self.api_error_tracker:
                self.api_error_tracker.record_error('debank', type(e).__name__)
            return None
    
    # ========================================================================
    # ETHERSCAN - CORRECTED (UPGRADE TO V2)
    # ========================================================================
    
    def _try_etherscan(self, address: str) -> Optional[Dict]:
        """
        Try Etherscan API (fallback).
        
        CORRECTED: Use V2 API endpoint!
        
        V2 Endpoint: https://api.etherscan.io/v2/api
        
        Response format:
        {
            "status": "1",
            "message": "OK",
            "result": [...]
        }
        """
        try:
            # ✅ CORRECTED: Use V2 API
            url = "https://api.etherscan.io/v2/api"
            
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': address,
                'startblock': 0,
                'endblock': 99999999,
                'page': 1,
                'offset': 1,  # Just get count, not full list
                'sort': 'desc',
                'apikey': self.etherscan_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == '1':
                    # Get actual count from message or result
                    result = data.get('result', [])
                    
                    # Etherscan returns total count in separate call
                    # For now, use result length as proxy
                    tx_count = len(result) if isinstance(result, list) else 0
                    
                    if self.api_error_tracker:
                        self.api_error_tracker.record_success('etherscan')
                    
                    logger.info(f"   ✅ Got stats from etherscan: {tx_count}+ transactions")
                    
                    return {
                        'total_transactions': tx_count,
                        'total_value_usd': 0,  # Etherscan doesn't provide USD value
                        'source': 'etherscan',
                        'data_quality': 'low',
                        'timestamp': datetime.utcnow()
                    }
                else:
                    logger.warning(f"   ❌ etherscan error: {data.get('message')}")
                    if self.api_error_tracker:
                        self.api_error_tracker.record_error('etherscan', 'api_error')
            
            else:
                logger.warning(f"   ❌ etherscan failed: HTTP {response.status_code}")
                if self.api_error_tracker:
                    self.api_error_tracker.record_error('etherscan', f'http_{response.status_code}')
            
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ Etherscan error: {type(e).__name__}")
            if self.api_error_tracker:
                self.api_error_tracker.record_error('etherscan', type(e).__name__)
            return None
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    
    def _is_api_healthy(self, api_name: str) -> bool:
        """Check if API is healthy (not circuit broken)."""
        if not self.api_health_monitor:
            return True
        
        is_healthy = self.api_health_monitor.is_healthy(api_name)
        
        if not is_healthy:
            logger.info(f"   ⏭️  Skipping {api_name} (unhealthy)")
        
        return is_healthy


# Export
__all__ = ['WalletStatsAPI']
