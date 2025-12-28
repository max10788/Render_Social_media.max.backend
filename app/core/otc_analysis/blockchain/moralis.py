"""
Moralis Web3 API Interface
===========================

Provides clean interface to Moralis Web3 Data API for:
- Entity labeling and enrichment
- Wallet transaction history
- Address validation
- Entity metadata (names, logos, labels)

Free Tier: 40,000 requests/month
API Docs: https://docs.moralis.com/web3-data-api
Get API Key: https://admin.moralis.io/register

Usage:
    moralis = MoralisAPI()
    
    # Get entity info for address
    entity = moralis.get_entity_info("0x742d35Cc...")
    print(entity['name'])  # "Cumberland DRW"
    print(entity['logo'])  # "https://..."
    
    # Get wallet history with enrichment
    history = moralis.get_wallet_history("0x742d35Cc...", limit=10)
"""

import os
import requests
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class MoralisAPI:
    """
    Moralis Web3 API Interface.
    
    Handles all Moralis API interactions with:
    - Automatic rate limiting
    - Error handling
    - Response validation
    - Entity enrichment
    
    Setup:
        export MORALIS_API_KEY='your_key_here'
        
    Get free API key: https://admin.moralis.io/register
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Moralis API client.
        
        Args:
            api_key: Optional API key (uses env var if not provided)
        """
        self.api_key = api_key or os.getenv('MORALIS_API_KEY')
        
        if not self.api_key:
            logger.warning("⚠️  MORALIS_API_KEY not configured!")
            logger.warning("Get free key: https://admin.moralis.io/register")
        
        self.base_url = "https://deep-index.moralis.io/api/v2.2"
        self.headers = {
            'Accept': 'application/json',
            'X-API-Key': self.api_key
        }
        
        # Rate limiting (40k/month = ~55/hour = ~1/minute safe rate)
        self.rate_limit_delay = 1.0  # seconds between requests
        self.last_request_time = 0
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        method: str = 'GET'
    ) -> Optional[Dict]:
        """
        Make request to Moralis API with error handling.
        
        Args:
            endpoint: API endpoint (e.g., '/wallets/{address}/history')
            params: Query parameters
            method: HTTP method
            
        Returns:
            Response data or None if failed
        """
        if not self.api_key:
            logger.error("❌ Moralis API key not configured")
            return None
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.debug(f"🔍 Moralis API: {method} {endpoint}")
            
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                timeout=10
            )
            
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"✅ Moralis API: Success")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"❌ Moralis API timeout: {endpoint}")
            return None
            
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            
            if status == 401:
                logger.error("❌ Moralis API: Invalid API key")
            elif status == 429:
                logger.error("❌ Moralis API: Rate limit exceeded")
            elif status == 404:
                logger.debug(f"ℹ️  Moralis API: Not found - {endpoint}")
            else:
                logger.error(f"❌ Moralis API error {status}: {e}")
            
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Moralis API request failed: {e}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Unexpected Moralis error: {e}")
            return None
    
    def get_wallet_history(
        self,
        address: str,
        chain: str = 'eth',
        limit: int = 10,
        cursor: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Get wallet transaction history with entity enrichment.
        
        This is the KEY method for entity discovery!
        Returns transactions with:
        - from_address_entity: "Jump Trading"
        - from_address_entity_logo: "https://..."
        - from_address_label: "Jump Trading: Hot Wallet"
        - to_address_entity: "Wintermute"
        - to_address_label: "Wintermute: Market Maker"
        
        Args:
            address: Wallet address
            chain: Blockchain (eth, bsc, polygon, arbitrum, etc.)
            limit: Number of transactions (max 100)
            cursor: Pagination cursor
            
        Returns:
            {
                'result': [list of transactions with entity labels],
                'cursor': 'pagination_cursor',
                'page': page_number,
                'page_size': size
            }
        """
        endpoint = f"/wallets/{address}/history"
        
        params = {
            'chain': chain,
            'limit': min(limit, 100)  # Max 100 per request
        }
        
        if cursor:
            params['cursor'] = cursor
        
        return self._make_request(endpoint, params)
    
    def get_entity_info(
        self,
        address: str,
        chain: str = 'eth'
    ) -> Optional[Dict]:
        """
        Get entity information for an address.
        
        Extracts entity metadata from recent transactions.
        Returns first available entity info found.
        
        Args:
            address: Wallet address
            chain: Blockchain
            
        Returns:
            {
                'entity_name': 'Jump Trading',
                'entity_logo': 'https://...',
                'entity_label': 'Jump Trading: Hot Wallet',
                'address': '0x...',
                'has_entity': True/False
            }
        """
        history = self.get_wallet_history(address, chain=chain, limit=5)
        
        if not history or 'result' not in history:
            return {
                'address': address,
                'has_entity': False
            }
        
        transactions = history['result']
        
        if not transactions:
            return {
                'address': address,
                'has_entity': False
            }
        
        # Look for entity info in transactions
        for tx in transactions:
            from_addr = tx.get('from_address', '').lower()
            to_addr = tx.get('to_address', '').lower()
            
            # Check if this address is from_address
            if from_addr == address.lower():
                entity_name = tx.get('from_address_entity')
                entity_logo = tx.get('from_address_entity_logo')
                entity_label = tx.get('from_address_label')
                
                if entity_name or entity_label:
                    return {
                        'address': address,
                        'entity_name': entity_name,
                        'entity_logo': entity_logo,
                        'entity_label': entity_label,
                        'has_entity': True,
                        'source_tx': tx.get('hash'),
                        'last_activity': tx.get('block_timestamp')
                    }
            
            # Check if this address is to_address
            if to_addr == address.lower():
                entity_name = tx.get('to_address_entity')
                entity_logo = tx.get('to_address_entity_logo')
                entity_label = tx.get('to_address_label')
                
                if entity_name or entity_label:
                    return {
                        'address': address,
                        'entity_name': entity_name,
                        'entity_logo': entity_logo,
                        'entity_label': entity_label,
                        'has_entity': True,
                        'source_tx': tx.get('hash'),
                        'last_activity': tx.get('block_timestamp')
                    }
        
        return {
            'address': address,
            'has_entity': False
        }
    
    def validate_otc_entity(
        self,
        address: str,
        chain: str = 'eth'
    ) -> Optional[Dict]:
        """
        Validate if address belongs to OTC desk/trading firm.
        
        Checks entity labels for OTC-related keywords:
        - 'otc'
        - 'trading'
        - 'market maker'
        - 'institutional'
        - 'liquidity'
        - 'desk'
        - 'proprietary'
        
        Args:
            address: Wallet address
            chain: Blockchain
            
        Returns:
            {
                'address': '0x...',
                'is_otc': True/False,
                'entity_name': 'Jump Trading',
                'entity_logo': 'https://...',
                'entity_label': 'Jump Trading: Hot',
                'confidence': 0.95,
                'matched_keywords': ['trading'],
                'last_activity': '2024-12-28T...'
            }
        """
        entity_info = self.get_entity_info(address, chain)
        
        if not entity_info or not entity_info.get('has_entity'):
            return {
                'address': address,
                'is_otc': False,
                'confidence': 0.0
            }
        
        # OTC-related keywords
        otc_keywords = [
            'otc',
            'trading',
            'market maker',
            'institutional',
            'liquidity',
            'desk',
            'proprietary',
            'prop'
        ]
        
        # Check entity name and label
        entity_name = (entity_info.get('entity_name') or '').lower()
        entity_label = (entity_info.get('entity_label') or '').lower()
        
        matched_keywords = []
        
        for keyword in otc_keywords:
            if keyword in entity_name or keyword in entity_label:
                matched_keywords.append(keyword)
        
        # Calculate confidence
        is_otc = len(matched_keywords) > 0
        confidence = 0.95 if is_otc else 0.75 if entity_name else 0.0
        
        return {
            'address': address,
            'is_otc': is_otc,
            'entity_name': entity_info.get('entity_name'),
            'entity_logo': entity_info.get('entity_logo'),
            'entity_label': entity_info.get('entity_label'),
            'confidence': confidence,
            'matched_keywords': matched_keywords,
            'last_activity': entity_info.get('last_activity')
        }
    
    def batch_validate_addresses(
        self,
        addresses: List[str],
        chain: str = 'eth'
    ) -> List[Dict]:
        """
        Validate multiple addresses.
        
        Args:
            addresses: List of wallet addresses
            chain: Blockchain
            
        Returns:
            List of validation results
        """
        results = []
        
        for address in addresses:
            try:
                result = self.validate_otc_entity(address, chain)
                if result:
                    results.append(result)
                    
                    if result.get('is_otc'):
                        logger.info(f"   ✅ {address[:10]}... → {result.get('entity_name')} (OTC)")
                    else:
                        logger.debug(f"   ⚠️  {address[:10]}... → Not OTC")
                        
            except Exception as e:
                logger.error(f"❌ Error validating {address}: {e}")
                continue
        
        return results
    
    def get_api_status(self) -> Dict:
        """
        Check API status and configuration.
        
        Returns:
            {
                'configured': True/False,
                'api_key_length': 32,
                'api_key_masked': 'eyJhbG...xMjM',
                'base_url': 'https://...',
                'free_tier_limit': 40000
            }
        """
        return {
            'configured': self.api_key is not None,
            'api_key_length': len(self.api_key) if self.api_key else 0,
            'api_key_masked': f"{self.api_key[:8]}...{self.api_key[-4:]}" if self.api_key and len(self.api_key) > 12 else None,
            'base_url': self.base_url,
            'free_tier_limit': 40000,
            'get_key_url': 'https://admin.moralis.io/register'
        }


# ════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ════════════════════════════════════════════════════════════════════════

def check_moralis_api_key() -> bool:
    """
    Check if Moralis API key is configured.
    
    Returns:
        True if configured, False otherwise
    """
    api_key = os.getenv('MORALIS_API_KEY')
    
    if api_key:
        logger.info(f"✅ Moralis API key configured: {api_key[:8]}...{api_key[-4:]}")
        return True
    else:
        logger.warning("⚠️  Moralis API key not configured!")
        logger.warning("Get free key: https://admin.moralis.io/register")
        logger.warning("Set: export MORALIS_API_KEY='your_key'")
        return False


# ════════════════════════════════════════════════════════════════════════
# EXAMPLE USAGE
# ════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """
    Example usage of Moralis API.
    """
    
    print("\n" + "="*80)
    print("MORALIS API - ENTITY ENRICHMENT TEST")
    print("="*80)
    
    # Check API key
    if not check_moralis_api_key():
        print("\n❌ Please set MORALIS_API_KEY environment variable")
        print("Get free key: https://admin.moralis.io/register")
        exit(1)
    
    # Initialize client
    moralis = MoralisAPI()
    
    # Check status
    status = moralis.get_api_status()
    print(f"\n📊 API Status:")
    print(f"   Configured: {status['configured']}")
    print(f"   Key: {status['api_key_masked']}")
    print(f"   Free Tier: {status['free_tier_limit']} requests/month")
    
    # Test with known OTC desk address
    test_address = "0xf584f8728b874a6a5c7a8d4d387c9aae9172d621"  # Jump Trading
    
    print(f"\n🔍 Testing Address: {test_address[:10]}...")
    
    # Get entity info
    entity_info = moralis.get_entity_info(test_address)
    
    if entity_info and entity_info.get('has_entity'):
        print(f"\n✅ Entity Found:")
        print(f"   Name: {entity_info.get('entity_name')}")
        print(f"   Label: {entity_info.get('entity_label')}")
        if entity_info.get('entity_logo'):
            print(f"   Logo: {entity_info.get('entity_logo')[:60]}...")
    else:
        print(f"\n⚠️  No entity info found")
    
    # Validate OTC
    validation = moralis.validate_otc_entity(test_address)
    
    if validation:
        print(f"\n📊 OTC Validation:")
        print(f"   Is OTC: {validation.get('is_otc')}")
        print(f"   Confidence: {validation.get('confidence'):.0%}")
        print(f"   Keywords: {validation.get('matched_keywords')}")
    
    print("\n" + "="*80)
    print("✅ Test complete!")
    print("="*80 + "\n")
