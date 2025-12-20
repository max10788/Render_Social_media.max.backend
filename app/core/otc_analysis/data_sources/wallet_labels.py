import requests
import time
from typing import Optional, Dict, List, Set
from otc_analysis.utils.cache import CacheManager
import os

class WalletLabelingService:
    """
    Fetches wallet labels from various services:
    - Nansen (subscription required)
    - Arkham Intelligence API
    - Manual labels from known exchanges/OTC desks
    """
    
    def __init__(self, cache_manager: Optional[CacheManager] = None):
        self.cache = cache_manager
        self.nansen_api_key = os.getenv('NANSEN_API_KEY')
        self.arkham_api_key = os.getenv('ARKHAM_API_KEY')
        
        # Known exchange addresses (manually maintained)
        self.known_exchanges = self._load_known_exchanges()
        
        # Rate limiting
        self.rate_limit_delay = 1.0
        self.last_request_time = 0
    
    def _load_known_exchanges(self) -> Dict[str, Dict]:
        """
        Load manually curated list of known exchange addresses.
        In production, this would be loaded from database or config file.
        """
        return {
            # Binance
            '0x28c6c06298d514db089934071355e5743bf21d60': {
                'entity_type': 'exchange',
                'entity_name': 'Binance',
                'wallet_type': 'hot_wallet',
                'labels': ['exchange', 'cex', 'binance']
            },
            '0x21a31ee1afc51d94c2efccaa2092ad1028285549': {
                'entity_type': 'exchange',
                'entity_name': 'Binance',
                'wallet_type': 'hot_wallet',
                'labels': ['exchange', 'cex', 'binance']
            },
            
            # Coinbase
            '0x71660c4005ba85c37ccec55d0c4493e66fe775d3': {
                'entity_type': 'exchange',
                'entity_name': 'Coinbase',
                'wallet_type': 'hot_wallet',
                'labels': ['exchange', 'cex', 'coinbase']
            },
            '0x503828976d22510aad0201ac7ec88293211d23da': {
                'entity_type': 'exchange',
                'entity_name': 'Coinbase',
                'wallet_type': 'cold_wallet',
                'labels': ['exchange', 'cex', 'coinbase', 'cold_storage']
            },
            
            # Kraken
            '0x2910543af39aba0cd09dbb2d50200b3e800a63d2': {
                'entity_type': 'exchange',
                'entity_name': 'Kraken',
                'wallet_type': 'hot_wallet',
                'labels': ['exchange', 'cex', 'kraken']
            },
            
            # Add more as needed...
        }
    
    def _rate_limit(self):
        """Enforce rate limiting."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        
        self.last_request_time = time.time()
    
    def get_wallet_labels(self, address: str) -> Optional[Dict]:
        """
        Get comprehensive labels for a wallet address.
        Checks cache first, then APIs, then known exchanges.
        
        Returns:
            Dict with entity_type, entity_name, labels, etc.
        """
        address_lower = address.lower()
        
        # Check cache
        if self.cache:
            cached = self.cache.get(address_lower, prefix='wallet_label')
            if cached:
                return cached
        
        # Check known exchanges first (fastest)
        if address_lower in self.known_exchanges:
            labels = self.known_exchanges[address_lower]
            self._cache_labels(address_lower, labels)
            return labels
        
        # Try Arkham API
        arkham_labels = self._fetch_arkham_labels(address)
        if arkham_labels:
            self._cache_labels(address_lower, arkham_labels)
            return arkham_labels
        
        # Try Nansen API
        if self.nansen_api_key:
            nansen_labels = self._fetch_nansen_labels(address)
            if nansen_labels:
                self._cache_labels(address_lower, nansen_labels)
                return nansen_labels
        
        # No labels found
        return {
            'entity_type': 'unknown',
            'entity_name': None,
            'labels': [],
            'source': 'none'
        }
    
    def _fetch_arkham_labels(self, address: str) -> Optional[Dict]:
        """Fetch labels from Arkham Intelligence API."""
        if not self.arkham_api_key:
            return None
        
        self._rate_limit()
        
        url = f"https://api.arkhamintelligence.com/intelligence/address/{address}"
        headers = {
            'API-Key': self.arkham_api_key
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            data = response.json()
            
            if not data:
                return None
            
            return {
                'entity_type': data.get('arkhamEntity', {}).get('type', 'unknown'),
                'entity_name': data.get('arkhamEntity', {}).get('name'),
                'labels': data.get('labels', []),
                'source': 'arkham',
                'confidence': data.get('confidence', 0)
            }
        except Exception as e:
            print(f"Arkham API error for {address}: {e}")
            return None
    
    def _fetch_nansen_labels(self, address: str) -> Optional[Dict]:
        """
        Fetch labels from Nansen API.
        Note: Nansen requires paid subscription.
        """
        if not self.nansen_api_key:
            return None
        
        self._rate_limit()
        
        url = f"https://api.nansen.ai/v1/addresses/{address}"
        headers = {
            'X-API-KEY': self.nansen_api_key
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            data = response.json()
            
            if not data:
                return None
            
            return {
                'entity_type': data.get('entity_type', 'unknown'),
                'entity_name': data.get('entity_name'),
                'labels': data.get('labels', []),
                'source': 'nansen',
                'wallet_type': data.get('wallet_type')
            }
        except Exception as e:
            print(f"Nansen API error for {address}: {e}")
            return None
    
    def _cache_labels(self, address: str, labels: Dict):
        """Cache wallet labels."""
        if self.cache:
            self.cache.set(address, labels, ttl=86400, prefix='wallet_label')  # 24h TTL
    
    def is_exchange(self, address: str) -> bool:
        """Quick check if address belongs to a known exchange."""
        labels = self.get_wallet_labels(address)
        return labels and labels.get('entity_type') == 'exchange'
    
    def is_known_otc_desk(self, address: str) -> bool:
        """Check if address belongs to a known OTC desk."""
        labels = self.get_wallet_labels(address)
        if not labels:
            return False
        
        # Check entity type
        if labels.get('entity_type') == 'otc_desk':
            return True
        
        # Check labels
        otc_keywords = ['otc', 'desk', 'otc desk', 'market maker']
        labels_list = labels.get('labels', [])
        
        return any(keyword in ' '.join(labels_list).lower() for keyword in otc_keywords)
    
    def batch_get_labels(self, addresses: List[str]) -> Dict[str, Dict]:
        """
        Get labels for multiple addresses.
        More efficient for bulk operations.
        """
        results = {}
        
        for address in addresses:
            labels = self.get_wallet_labels(address)
            results[address] = labels
            
            # Small delay to avoid rate limits
            time.sleep(0.1)
        
        return results
    
    def get_entity_addresses(self, entity_name: str) -> Set[str]:
        """
        Get all known addresses for an entity (e.g., all Binance wallets).
        """
        addresses = set()
        
        for addr, info in self.known_exchanges.items():
            if info['entity_name'].lower() == entity_name.lower():
                addresses.add(addr)
        
        return addresses
    
    def add_manual_label(
        self,
        address: str,
        entity_type: str,
        entity_name: str,
        labels: List[str]
    ):
        """
        Manually add or update a wallet label.
        Useful for maintaining custom labels.
        """
        address_lower = address.lower()
        
        label_data = {
            'entity_type': entity_type,
            'entity_name': entity_name,
            'labels': labels,
            'source': 'manual',
            'wallet_type': 'unknown'
        }
        
        self.known_exchanges[address_lower] = label_data
        
        # Update cache
        if self.cache:
            self._cache_labels(address_lower, label_data)
