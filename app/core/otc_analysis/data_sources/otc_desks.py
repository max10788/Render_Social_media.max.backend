from typing import List, Dict, Optional, Set
from otc_analysis.utils.cache import CacheManager
from otc_analysis.data_sources.wallet_labels import WalletLabelingService

class OTCDeskRegistry:
    """
    Registry of known OTC desks and their addresses.
    Combines manual curation with labeling services.
    
    As mentioned in doc: Wintermute, Cumberland, Galaxy Digital, etc.
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        labeling_service: Optional[WalletLabelingService] = None
    ):
        self.cache = cache_manager
        self.labeling_service = labeling_service
        
        # Manually curated OTC desk registry
        self.known_desks = self._initialize_known_desks()
    
    def _initialize_known_desks(self) -> Dict[str, Dict]:
        """
        Initialize registry of known OTC desks.
        
        Structure:
        {
            'desk_name': {
                'addresses': [...],
                'type': 'otc_desk',
                'description': '...',
                'confidence': 1.0  # 1.0 for verified, <1.0 for suspected
            }
        }
        """
        return {
            'wintermute': {
                'addresses': [
                    '0x00000000ae347930bd1e7b0f35588b92280f9e75',
                    '0x0000000070f4a47e7c9be9be8f857b3b91a86d5f',
                    # Add more Wintermute addresses
                ],
                'type': 'otc_desk',
                'description': 'Wintermute Trading - Major crypto market maker and OTC desk',
                'confidence': 1.0,
                'website': 'https://wintermute.com',
                'active': True
            },
            
            'cumberland': {
                'addresses': [
                    '0x742d35cc6634c0532925a3b844bc454e4438f44e',
                    # Add more Cumberland addresses
                ],
                'type': 'otc_desk',
                'description': 'Cumberland DRW - OTC trading desk',
                'confidence': 1.0,
                'website': 'https://cumberland.io',
                'active': True
            },
            
            'galaxy_digital': {
                'addresses': [
                    '0x1111111254eeb25477b68fb85ed929f73a960582',
                    # Add more Galaxy Digital addresses
                ],
                'type': 'otc_desk',
                'description': 'Galaxy Digital - Institutional crypto services',
                'confidence': 1.0,
                'website': 'https://galaxydigital.io',
                'active': True
            },
            
            'jump_trading': {
                'addresses': [
                    '0x7891b20c690605f4e370d6944c8a5dbfac5a451c',
                    # Add more Jump Trading addresses
                ],
                'type': 'otc_desk',
                'description': 'Jump Trading - Quantitative trading firm',
                'confidence': 0.9,
                'active': True
            },
            
            'b2c2': {
                'addresses': [
                    '0xc098b2a3aa256d2140208c3de6543aaef5cd3a94',
                    # Add more B2C2 addresses
                ],
                'type': 'otc_desk',
                'description': 'B2C2 - Institutional liquidity provider',
                'confidence': 1.0,
                'active': True
            },
            
            'genesis_trading': {
                'addresses': [
                    # Add Genesis Trading addresses if known
                ],
                'type': 'otc_desk',
                'description': 'Genesis Global Trading - OTC desk',
                'confidence': 0.8,
                'active': False  # Note: Genesis filed for bankruptcy in 2023
            },
            
            'circle_trade': {
                'addresses': [
                    # Add Circle Trade addresses
                ],
                'type': 'otc_desk',
                'description': 'Circle Trade - OTC trading desk',
                'confidence': 0.9,
                'active': True
            }
        }
    
    def get_all_otc_addresses(self) -> Set[str]:
        """Get set of all known OTC desk addresses."""
        addresses = set()
        
        for desk_info in self.known_desks.values():
            addresses.update(desk_info['addresses'])
        
        return addresses
    
    def is_otc_desk(self, address: str) -> bool:
        """
        Check if address belongs to a known OTC desk.
        Checks both registry and labeling service.
        """
        address_lower = address.lower()
        
        # Check cache first
        if self.cache:
            cached = self.cache.get(f"is_otc:{address_lower}", prefix='otc')
            if cached is not None:
                return cached
        
        # Check known desks registry
        for desk_info in self.known_desks.values():
            if address_lower in [addr.lower() for addr in desk_info['addresses']]:
                self._cache_result(address_lower, True)
                return True
        
        # Check labeling service if available
        if self.labeling_service:
            is_otc = self.labeling_service.is_known_otc_desk(address)
            self._cache_result(address_lower, is_otc)
            return is_otc
        
        self._cache_result(address_lower, False)
        return False
    
    def get_desk_info(self, address: str) -> Optional[Dict]:
        """Get detailed information about the OTC desk associated with an address."""
        address_lower = address.lower()
        
        for desk_name, desk_info in self.known_desks.items():
            if address_lower in [addr.lower() for addr in desk_info['addresses']]:
                return {
                    'desk_name': desk_name,
                    'type': desk_info['type'],
                    'description': desk_info['description'],
                    'confidence': desk_info['confidence'],
                    'active': desk_info.get('active', True),
                    'all_addresses': desk_info['addresses']
                }
        
        return None
    
    def get_desk_by_name(self, desk_name: str) -> Optional[Dict]:
        """Get OTC desk info by name."""
        desk_name_lower = desk_name.lower()
        
        if desk_name_lower in self.known_desks:
            return self.known_desks[desk_name_lower]
        
        return None
    
    def get_all_desks(self, active_only: bool = True) -> Dict[str, Dict]:
        """
        Get all registered OTC desks.
        
        Args:
            active_only: Only return active desks
        """
        if active_only:
            return {
                name: info
                for name, info in self.known_desks.items()
                if info.get('active', True)
            }
        
        return self.known_desks
    
    def add_desk(
        self,
        desk_name: str,
        addresses: List[str],
        description: str,
        confidence: float = 0.8
    ):
        """
        Manually add a new OTC desk to the registry.
        
        Args:
            desk_name: Name of the desk
            addresses: List of wallet addresses
            description: Description of the desk
            confidence: Confidence score (0-1)
        """
        self.known_desks[desk_name.lower()] = {
            'addresses': addresses,
            'type': 'otc_desk',
            'description': description,
            'confidence': confidence,
            'active': True,
            'manually_added': True
        }
        
        # Invalidate cache for these addresses
        if self.cache:
            for addr in addresses:
                self.cache.delete(f"is_otc:{addr.lower()}", prefix='otc')
    
    def add_address_to_desk(self, desk_name: str, address: str):
        """Add an address to an existing desk."""
        desk_name_lower = desk_name.lower()
        
        if desk_name_lower not in self.known_desks:
            raise ValueError(f"Desk '{desk_name}' not found in registry")
        
        if address not in self.known_desks[desk_name_lower]['addresses']:
            self.known_desks[desk_name_lower]['addresses'].append(address)
            
            # Invalidate cache
            if self.cache:
                self.cache.delete(f"is_otc:{address.lower()}", prefix='otc')
    
    def get_desk_list(self) -> List[Dict]:
        """
        Get cached list of all OTC desks.
        Used for frontend display and quick lookups.
        """
        # Check cache
        if self.cache:
            cached = self.cache.get_otc_desk_list()
            if cached:
                return cached
        
        # Build list
        desk_list = []
        for name, info in self.known_desks.items():
            desk_list.append({
                'name': name,
                'display_name': name.replace('_', ' ').title(),
                'address_count': len(info['addresses']),
                'confidence': info['confidence'],
                'active': info.get('active', True),
                'description': info['description']
            })
        
        # Cache it
        if self.cache:
            self.cache.cache_otc_desk_list(desk_list)
        
        return desk_list
    
    def _cache_result(self, address: str, is_otc: bool):
        """Cache OTC desk check result."""
        if self.cache:
            self.cache.set(f"is_otc:{address}", is_otc, ttl=3600, prefix='otc')
    
    def search_desks(self, query: str) -> List[Dict]:
        """
        Search for OTC desks by name or description.
        
        Args:
            query: Search query
        
        Returns:
            List of matching desks
        """
        query_lower = query.lower()
        results = []
        
        for name, info in self.known_desks.items():
            # Search in name
            if query_lower in name:
                results.append({'name': name, **info})
                continue
            
            # Search in description
            if query_lower in info.get('description', '').lower():
                results.append({'name': name, **info})
        
        return results
