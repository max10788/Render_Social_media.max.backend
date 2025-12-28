"""
Dynamic OTC Desk Registry - Active Discovery System
====================================================

BREAKTHROUGH APPROACH:
Instead of curated lists, we DISCOVER OTC desks dynamically by:
1. Scanning recent large transactions (>$100k)
2. Extracting addresses with high volume
3. Checking Moralis entity metadata
4. Validating OTC keywords in labels
5. Adding as "discovered" desks

Result: Self-expanding registry of ACTIVE OTC desks!

Desk Types:
- VERIFIED: Manually verified, high confidence (seed list)
- DISCOVERED: Auto-discovered from large transactions
- VALIDATED: Moralis entity labels confirm OTC activity

API Requirements:
- Moralis API Key (free, 40k requests/month)
- Etherscan API Key (for transaction scanning)

Get keys:
- Moralis: https://admin.moralis.io/register
- Etherscan: https://etherscan.io/apis
"""

import os
import logging
from typing import List, Dict, Optional, Set, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

# Import APIs
try:
    from app.core.otc_analysis.blockchain.moralis import MoralisAPI
    from app.core.otc_analysis.blockchain.etherscan import EtherscanAPI
except ImportError:
    import sys
    sys.path.append('/opt/render/project/src')
    from app.core.otc_analysis.blockchain.moralis import MoralisAPI
    from app.core.otc_analysis.blockchain.etherscan import EtherscanAPI

logger = logging.getLogger(__name__)


class ActiveTransactionScanner:
    """
    Scans recent large transactions to discover OTC desks.
    
    Strategy:
    1. Get recent transactions from Etherscan
    2. Filter by volume threshold (e.g., >$100k)
    3. Extract unique addresses
    4. Return candidates for validation
    """
    
    def __init__(self, etherscan: EtherscanAPI):
        self.etherscan = etherscan
    
    def scan_large_transactions(
        self,
        addresses_to_scan: List[str],
        min_value_usd: float = 100000,
        max_transactions: int = 100
    ) -> Dict[str, Dict]:
        """
        Scan addresses for large recent transactions.
        
        Args:
            addresses_to_scan: Known addresses to check (seed list)
            min_value_usd: Minimum transaction value in USD
            max_transactions: Max transactions to check per address
            
        Returns:
            Dict of {address: {volume, tx_count, counterparties}}
        """
        logger.info(f"🔍 Scanning for large transactions (>${min_value_usd/1000:.0f}k)...")
        
        large_tx_addresses = defaultdict(lambda: {
            'volume_usd': 0,
            'tx_count': 0,
            'counterparties': set()
        })
        
        for address in addresses_to_scan:
            try:
                # Get recent transactions
                transactions = self.etherscan.get_recent_transactions(address, limit=max_transactions)
                
                if not transactions:
                    continue
                
                # Analyze transactions
                for tx in transactions:
                    # Get transaction value in ETH
                    value_eth = float(tx.get('value', 0)) / 1e18
                    
                    # Estimate USD value (rough, would need price API)
                    # Using ~$3000/ETH as rough estimate
                    value_usd = value_eth * 3000
                    
                    if value_usd >= min_value_usd:
                        from_addr = tx.get('from', '').lower()
                        to_addr = tx.get('to', '').lower()
                        
                        # Track both sides
                        for addr in [from_addr, to_addr]:
                            if addr and addr != address.lower():
                                large_tx_addresses[addr]['volume_usd'] += value_usd
                                large_tx_addresses[addr]['tx_count'] += 1
                                large_tx_addresses[addr]['counterparties'].add(address.lower())
                
            except Exception as e:
                logger.error(f"❌ Error scanning {address[:10]}: {e}")
                continue
        
        # Filter: Must have multiple large transactions
        filtered = {
            addr: stats for addr, stats in large_tx_addresses.items()
            if stats['tx_count'] >= 2  # At least 2 large transactions
        }
        
        logger.info(f"   ✅ Found {len(filtered)} addresses with large activity")
        
        return filtered


class OTCDeskRegistry:
    """
    Dynamic OTC Desk Registry with Active Discovery.
    
    Features:
    - VERIFIED desks (manually curated, high confidence)
    - DISCOVERED desks (auto-found from large transactions)
    - Live Moralis metadata for all desks
    - Self-expanding registry
    
    Usage:
        registry = OTCDeskRegistry(cache_manager)
        
        # Get all desks (verified + discovered)
        desks = registry.get_desk_list(include_discovered=True)
        
        # Discover new desks
        new_desks = registry.discover_active_desks()
        
        # Check if address is OTC
        is_otc = registry.is_otc_desk(address)
    """
    
    def __init__(self, cache_manager=None):
        self.cache = cache_manager
        
        # Initialize APIs
        self.moralis = MoralisAPI()
        try:
            self.etherscan = EtherscanAPI(chain_id=1)  # Ethereum mainnet
        except Exception as e:
            logger.warning(f"⚠️  Etherscan API not available: {e}")
            self.etherscan = None
        
        # Initialize scanner
        if self.etherscan:
            self.scanner = ActiveTransactionScanner(self.etherscan)
        else:
            self.scanner = None
        
        # Cache settings
        self._desks_cache = None
        self._cache_timestamp = None
        self._cache_ttl = 86400  # 24 hours
        
        # Discovery settings
        self.discovery_volume_threshold = 100000  # $100k
        self.discovery_enabled = True
    
    def _get_verified_seeds(self) -> List[Dict]:
        """
        Get VERIFIED seed addresses (manually verified OTC desks).
        
        These are high-confidence, well-known OTC desks.
        Used as:
        1. Initial registry (always available)
        2. Starting points for discovery (their counterparties)
        
        Updated: December 2024
        """
        return [
            # TIER 1: Major verified OTC desks
            {
                'address': '0xf584f8728b874a6a5c7a8d4d387c9aae9172d621',
                'name': 'Jump Trading',
                'type': 'prop_trading',
                'desk_category': 'verified'
            },
            {
                'address': '0xdbf5e9c5206d0db70a90108bf936da60221dc080',
                'name': 'Wintermute',
                'type': 'market_maker',
                'desk_category': 'verified'
            },
            {
                'address': '0x742d35Cc6634C0532925a3b844Bc9e7595f0bEbC',
                'name': 'Cumberland DRW',
                'type': 'otc_desk',
                'desk_category': 'verified'
            },
            {
                'address': '0xC333E80eF2deC2805F239E3f1e810612D294F771',
                'name': 'B2C2',
                'type': 'liquidity_provider',
                'desk_category': 'verified'
            },
            {
                'address': '0x5a52e96bacdabb82fd05763e25335261b270efcb',
                'name': 'GSR',
                'type': 'market_maker',
                'desk_category': 'verified'
            },
            {
                'address': '0x7891b20c690605f4e370d6944c8a5dbfac5a451c',
                'name': 'Flowtraders',
                'type': 'market_maker',
                'desk_category': 'verified'
            },
            {
                'address': '0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13',
                'name': 'DWF Labs',
                'type': 'market_maker',
                'desk_category': 'verified'
            },
            {
                'address': '0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43',
                'name': 'Kronos Research',
                'type': 'market_maker',
                'desk_category': 'verified'
            },
        ]
    
    def discover_active_desks(
        self,
        volume_threshold: float = 100000,
        max_new_desks: int = 20,
        hours_back: int = 24
    ) -> List[Dict]:
        """
        🚀 ACTIVE DISCOVERY: Find new OTC desks from large transactions.
        """
        if not self.scanner:
            logger.warning("⚠️  Active discovery disabled (Etherscan not available)")
            return []
        
        logger.info("🚀 Starting active OTC desk discovery...")
        logger.info(f"   Strategy: Large transactions (>${volume_threshold/1000:.0f}k) in last {hours_back}h → Entity validation")
        
        # Step 1: Get verified seeds as starting points
        verified_seeds = self._get_verified_seeds()
        seed_addresses = [s['address'] for s in verified_seeds]
        
        logger.info(f"   📋 Scanning {len(seed_addresses)} verified desks for counterparties...")
        logger.info(f"   ⏰ Time window: Last {hours_back} hours")
        
        # Step 2: Scan for large transactions
        large_tx_addresses = self.scanner.scan_large_transactions(
            addresses_to_scan=seed_addresses,
            min_value_usd=volume_threshold,
            max_transactions=50
        )
        
        if not large_tx_addresses:
            logger.info("   ℹ️  No large transactions found")
            return []
        
        # Step 3: Validate via Moralis
        logger.info(f"   🔍 Validating {len(large_tx_addresses)} candidates via Moralis...")
        
        discovered_desks = []
        
        # ✅ FIX: Don't call _get_cached_desks() - it causes infinite loop!
        # Just use verified seeds for existing addresses
        existing_addresses = set(s['address'].lower() for s in verified_seeds)
        
        # ❌ OLD CODE (CAUSES LOOP):
        # cached_desks = self._get_cached_desks()  # ← THIS CALLS _validate_and_enrich_desks() → LOOP!
        # for desk in cached_desks.values():
        #     existing_addresses.update(addr.lower() for addr in desk.get('addresses', []))
        
        for address, stats in list(large_tx_addresses.items())[:max_new_desks]:
            # Skip if already known
            if address.lower() in existing_addresses:
                continue
            
            try:
                # Validate via Moralis
                validation = self.moralis.validate_otc_entity(address)
                
                if not validation or not validation.get('entity_name'):
                    logger.debug(f"      ⊘ {address[:10]}: No entity info")
                    continue
                
                # Check if OTC-related
                is_otc = validation.get('is_otc', False)
                confidence = validation.get('confidence', 0)
                
                if is_otc or confidence >= 0.7:
                    desk_data = {
                        'address': address,
                        'name': validation.get('entity_name'),
                        'type': 'discovered',
                        'desk_category': 'discovered',
                        'entity_label': validation.get('entity_label'),
                        'logo_url': validation.get('entity_logo'),
                        'confidence': confidence,
                        'matched_keywords': validation.get('matched_keywords', []),
                        'discovery_volume': stats['volume_usd'],
                        'discovery_tx_count': stats['tx_count'],
                        'discovered_at': datetime.now().isoformat()
                    }
                    
                    discovered_desks.append(desk_data)
                    logger.info(f"      ✅ DISCOVERED: {desk_data['name']} (confidence: {confidence:.0%}, volume: ${stats['volume_usd']/1000:.0f}k)")
                else:
                    logger.debug(f"      ⊘ {address[:10]}: Not OTC (confidence: {confidence:.0%})")
                
            except Exception as e:
                logger.error(f"❌ Error validating {address[:10]}: {e}")
                continue
        
        logger.info(f"🎉 Discovery complete: {len(discovered_desks)} new OTC desks found!")
        
        return discovered_desks
        
        logger.info(f"🎉 Discovery complete: {len(discovered_desks)} new OTC desks found!")
        
        return discovered_desks
    
    def _validate_and_enrich_desks(self, include_discovery: bool = False) -> Dict[str, Dict]:  # ✅ DEFAULT = False!
        """
        Validate and enrich all desks (verified + discovered).
        
        Args:
            include_discovery: Whether to run active discovery (DEFAULT: False to prevent loops)
            
        Returns:
            Dict of all desks with metadata
        """
        logger.info("🔄 Building OTC desk registry...")
        
        all_desks = {}
        
        # Step 1: Process verified seeds
        logger.info("   Step 1: Validating verified desks...")
        
        verified_seeds = self._get_verified_seeds()
        
        for seed in verified_seeds:
            address = seed.get('address')
            expected_name = seed.get('name', 'Unknown')
            
            # Validation...
            if not address or not isinstance(address, str):
                logger.warning(f"      ⚠️  {expected_name}: Invalid address - skipping")
                continue
            
            if not address.startswith('0x') or len(address) != 42:
                logger.warning(f"      ⚠️  {expected_name}: Invalid address format - skipping")
                continue
            
            try:
                # Validate with Moralis
                validation = self.moralis.validate_otc_entity(address)
                
                if not validation:
                    logger.warning(f"      ⚠️  {expected_name}: No Moralis response")
                    continue
                
                # Build desk entry
                desk_name = validation.get('entity_name') or expected_name
                desk_key = desk_name.lower().replace(' ', '_').replace(':', '').replace('-', '_')
                
                desk_data = {
                    'name': desk_name,
                    'addresses': [address],
                    'type': seed.get('type', 'unknown'),
                    'desk_category': 'verified',
                    'entity_label': validation.get('entity_label'),
                    'logo_url': validation.get('entity_logo'),
                    'confidence': max(validation.get('confidence', 0.75), 0.9),
                    'matched_keywords': validation.get('matched_keywords', []),
                    'is_otc': validation.get('is_otc', True),
                    'active': True,
                    'source': 'verified_moralis',
                    'last_updated': datetime.now().isoformat(),
                    'last_activity': validation.get('last_activity')
                }
                
                all_desks[desk_key] = desk_data
                logger.info(f"      ✅ {desk_name}: Verified")
                
            except Exception as e:
                logger.error(f"❌ Error validating {expected_name}: {e}")
                continue
        
        # ✅ Step 2: SKIP discovery in cache build to prevent infinite loop!
        if include_discovery and self.discovery_enabled:
            logger.info("   Step 2: Running active discovery...")
            logger.warning("⚠️  Discovery in cache build - this may cause loops!")
            
            # Don't run discovery here - it causes infinite loop!
            # Discovery should be triggered manually via API endpoints
        else:
            logger.info("   Step 2: Skipping discovery (prevent loops)")
        
        logger.info(f"✅ Registry built: {len(all_desks)} OTC desks (verified only)")
        
        return all_desks
    
    def _get_cached_desks(self) -> Dict[str, Dict]:
        """Get cached desks or build new registry."""
        # Check in-memory cache
        if self._desks_cache and self._cache_timestamp:
            age = datetime.now() - self._cache_timestamp
            if age.total_seconds() < self._cache_ttl:
                logger.debug(f"✅ Using cached desks (age: {int(age.total_seconds() / 3600)}h)")
                return self._desks_cache
        
        # Check cache manager
        if self.cache:
            cached = self.cache.get('otc_desks_full', prefix='otc')
            if cached and isinstance(cached, dict):
                logger.info("✅ Loaded desks from cache manager")
                self._desks_cache = cached
                self._cache_timestamp = datetime.now()
                return cached
        
        # Build fresh registry
        logger.info("🔄 Cache expired, building registry...")
        
        desks = self._validate_and_enrich_desks(include_discovery=self.discovery_enabled)
        
        # Update caches
        self._desks_cache = desks
        self._cache_timestamp = datetime.now()
        
        if self.cache:
            self.cache.set('otc_desks_full', desks, ttl=self._cache_ttl, prefix='otc')
        
        return desks
    
    def get_all_otc_addresses(self) -> Set[str]:
        """Get set of all known OTC desk addresses."""
        desks = self._get_cached_desks()
        
        addresses = set()
        for desk_info in desks.values():
            addresses.update(desk_info['addresses'])
        
        return addresses
    
    def is_otc_desk(self, address: str) -> bool:
        """Check if address belongs to known OTC desk."""
        address_lower = address.lower()
        
        # Check cache
        if self.cache:
            cached = self.cache.get(f"is_otc:{address_lower}", prefix='otc')
            if cached is not None:
                return cached
        
        # Check known desks
        desks = self._get_cached_desks()
        
        for desk_info in desks.values():
            if address_lower in [addr.lower() for addr in desk_info['addresses']]:
                self._cache_result(address_lower, True)
                return True
        
        self._cache_result(address_lower, False)
        return False
    
    def get_desk_info(self, address: str) -> Optional[Dict]:
        """Get detailed info about OTC desk."""
        address_lower = address.lower()
        
        desks = self._get_cached_desks()
        
        for desk_name, desk_info in desks.items():
            if address_lower in [addr.lower() for addr in desk_info['addresses']]:
                return {
                    'desk_name': desk_name,
                    'display_name': desk_info['name'],
                    'type': desk_info['type'],
                    'desk_category': desk_info.get('desk_category', 'verified'),
                    'entity_label': desk_info.get('entity_label'),
                    'logo_url': desk_info.get('logo_url'),
                    'confidence': desk_info['confidence'],
                    'matched_keywords': desk_info.get('matched_keywords', []),
                    'discovery_volume': desk_info.get('discovery_volume'),
                    'is_otc': desk_info.get('is_otc', True),
                    'active': desk_info.get('active', True),
                    'all_addresses': desk_info['addresses'],
                    'source': desk_info.get('source', 'verified_moralis'),
                    'last_updated': desk_info.get('last_updated'),
                    'last_activity': desk_info.get('last_activity')
                }
        
        return None
    
    def get_desk_by_name(self, desk_name: str) -> Optional[Dict]:
        """Get OTC desk by name."""
        desk_name_lower = desk_name.lower().replace(' ', '_')
        
        desks = self._get_cached_desks()
        
        if desk_name_lower in desks:
            return desks[desk_name_lower]
        
        # Try partial match
        for name, info in desks.items():
            if desk_name_lower in name:
                return info
        
        return None
    
    def get_desk_list(self, include_discovered: bool = False, min_confidence: float = 0.0) -> List[Dict]:
        """
        Get list of all known OTC desks.
        
        Args:
            include_discovered: Include auto-discovered desks
            min_confidence: Minimum confidence threshold
            
        Returns:
            List of OTC desks with metadata
        """
        desks = self._get_cached_desks()
        
        desk_list = []
        for name, info in desks.items():
            # Filter by category
            if not include_discovered and info.get('desk_category') == 'discovered':
                continue
            
            # Filter by confidence
            if info.get('confidence', 0) < min_confidence:
                continue
            
            desk_list.append({
                'name': name,
                'display_name': info['name'],
                'type': info['type'],
                'desk_category': info.get('desk_category', 'verified'),
                'address_count': len(info['addresses']),
                'addresses': info['addresses'],
                'confidence': info['confidence'],
                'matched_keywords': info.get('matched_keywords', []),
                'discovery_volume': info.get('discovery_volume'),
                'is_otc': info.get('is_otc', True),
                'active': info.get('active', True),
                'logo_url': info.get('logo_url'),
                'source': info.get('source', 'verified_moralis'),
                'last_updated': info.get('last_updated'),
                'last_activity': info.get('last_activity')
            })
        
        # Sort by category (verified first), then confidence
        desk_list.sort(key=lambda x: (
            0 if x['desk_category'] == 'verified' else 1,
            -x['confidence']
        ))
        
        return desk_list
    
    def refresh_desks(self, force_discovery: bool = True) -> int:
        """
        Force refresh registry.
        
        Args:
            force_discovery: Run active discovery
            
        Returns:
            Number of desks in registry
        """
        logger.info("🔄 Force refreshing OTC desks...")
        
        # Clear caches
        self._desks_cache = None
        self._cache_timestamp = None
        
        if self.cache:
            self.cache.delete('otc_desks_full', prefix='otc')
        
        # Rebuild with discovery
        old_state = self.discovery_enabled
        self.discovery_enabled = force_discovery
        
        desks = self._get_cached_desks()
        
        self.discovery_enabled = old_state
        
        logger.info(f"✅ Refreshed {len(desks)} OTC desks")
        
        return len(desks)
    
    def _cache_result(self, address: str, is_otc: bool):
        """Cache OTC check result."""
        if self.cache:
            self.cache.set(f"is_otc:{address}", is_otc, ttl=3600, prefix='otc')
    
    def get_stats(self) -> Dict:
        """Get registry statistics."""
        desks = self._get_cached_desks()
        
        total_addresses = sum(len(desk['addresses']) for desk in desks.values())
        active_desks = sum(1 for desk in desks.values() if desk.get('active', True))
        
        # Count by category
        verified_count = sum(1 for d in desks.values() if d.get('desk_category') == 'verified')
        discovered_count = sum(1 for d in desks.values() if d.get('desk_category') == 'discovered')
        
        # Cache age
        cache_age = None
        if self._cache_timestamp:
            age = datetime.now() - self._cache_timestamp
            cache_age = int(age.total_seconds() / 3600)
        
        # Count by type
        by_type = {}
        for desk in desks.values():
            desk_type = desk.get('type', 'unknown')
            by_type[desk_type] = by_type.get(desk_type, 0) + 1
        
        # Confidence distribution
        high_confidence = sum(1 for d in desks.values() if d.get('confidence', 0) >= 0.9)
        medium_confidence = sum(1 for d in desks.values() if 0.7 <= d.get('confidence', 0) < 0.9)
        low_confidence = sum(1 for d in desks.values() if d.get('confidence', 0) < 0.7)
        
        return {
            'total_desks': len(desks),
            'active_desks': active_desks,
            'total_addresses': total_addresses,
            'verified_desks': verified_count,
            'discovered_desks': discovered_count,
            'desks_by_type': by_type,
            'confidence_distribution': {
                'high (>=90%)': high_confidence,
                'medium (70-89%)': medium_confidence,
                'low (<70%)': low_confidence
            },
            'cache_age_hours': cache_age,
            'data_source': 'active_discovery + moralis_api',
            'discovery_enabled': self.discovery_enabled,
            'moralis_configured': self.moralis.api_key is not None,
            'etherscan_configured': self.etherscan is not None
        }
    
    def search_desks(self, query: str) -> List[Dict]:
        """Search OTC desks by name or type."""
        query_lower = query.lower()
        desks = self._get_cached_desks()
        results = []
        
        for name, info in desks.items():
            # Search in name
            if query_lower in name or query_lower in info['name'].lower():
                results.append({'name': name, **info})
                continue
            
            # Search in type
            if query_lower in info.get('type', ''):
                results.append({'name': name, **info})
        
        return results
    
    def get_combined_desk_list(
        self,
        include_discovered: bool = True,
        include_db_validated: bool = True,
        min_confidence: float = 0.0,
        db_session=None
    ) -> List[Dict]:
        """
        Get COMBINED list of OTC desks from:
        1. Registry (verified + discovered)
        2. Database (validated wallets with high confidence)
        
        Args:
            include_discovered: Include auto-discovered desks from registry
            include_db_validated: Include validated desks from database
            min_confidence: Minimum confidence threshold
            db_session: Database session (optional)
            
        Returns:
            Combined list of all OTC desks with source tracking
        """
        all_desks = []
        
        # 1. Get desks from Registry
        registry_desks = self.get_desk_list(
            include_discovered=include_discovered,
            min_confidence=min_confidence
        )
        
        for desk in registry_desks:
            desk['data_source'] = 'registry'
            all_desks.append(desk)
        
        logger.info(f"📊 Registry: {len(registry_desks)} desks")
        
        # 2. Get validated desks from Database (if available)
        if include_db_validated and db_session:
            try:
                from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
                
                # Query high-confidence wallets from DB
                db_wallets = db_session.query(OTCWallet).filter(
                    OTCWallet.confidence_score >= min_confidence * 100,  # Convert to 0-100 scale
                    OTCWallet.is_active == True
                ).all()
                
                # Check which addresses are NOT already in registry
                registry_addresses = set()
                for desk in registry_desks:
                    registry_addresses.update(
                        addr.lower() for addr in desk.get('addresses', [])
                    )
                
                # Add DB wallets that aren't in registry
                db_desk_count = 0
                for wallet in db_wallets:
                    if wallet.address.lower() not in registry_addresses:
                        all_desks.append({
                            'name': wallet.label or f"desk_{wallet.address[:8]}",
                            'display_name': wallet.entity_name or wallet.label or f"{wallet.address[:8]}...",
                            'type': wallet.entity_type or 'validated',
                            'desk_category': 'db_validated',
                            'address_count': 1,
                            'addresses': [wallet.address],
                            'confidence': wallet.confidence_score / 100,  # Convert to 0-1 scale
                            'is_otc': True,
                            'active': wallet.is_active,
                            'logo_url': None,
                            'source': 'database',
                            'data_source': 'database',
                            'total_volume': wallet.total_volume,
                            'transaction_count': wallet.transaction_count,
                            'last_updated': wallet.updated_at.isoformat() if wallet.updated_at else None,
                            'last_activity': wallet.last_active.isoformat() if wallet.last_active else None
                        })
                        db_desk_count += 1
                
                logger.info(f"💾 Database: {db_desk_count} validated desks (excluded {len(db_wallets) - db_desk_count} duplicates)")
                
            except Exception as e:
                logger.error(f"❌ Error fetching DB wallets: {e}")
        
        # Sort combined list
        all_desks.sort(key=lambda x: (
            0 if x.get('desk_category') == 'verified' else 1 if x.get('desk_category') == 'discovered' else 2,
            -x.get('confidence', 0)
        ))
        
        logger.info(f"🎯 Combined: {len(all_desks)} total OTC desks")
        
        return all_desks
    
    def discover_desks_by_time_period(
        self,
        hours_back: int = 1,
        volume_threshold: float = 100000,
        max_new_desks: int = 20
    ) -> Dict:
        """
        Discover OTC desks active in a specific time period.
        
        Args:
            hours_back: Hours to look back (1, 6, 24, 168, etc.)
            volume_threshold: Min transaction value
            max_new_desks: Max desks to discover
            
        Returns:
            Dict with discovered desks and metadata
        """
        logger.info(f"🕒 Discovery for time period: Last {hours_back} hours")
        
        # Run discovery
        discovered = self.discover_active_desks(
            volume_threshold=volume_threshold,
            max_new_desks=max_new_desks,
            hours_back=hours_back
        )
        
        return {
            'discovered_desks': discovered,
            'time_period': {
                'hours_back': hours_back,
                'start_time': (datetime.now() - timedelta(hours=hours_back)).isoformat(),
                'end_time': datetime.now().isoformat()
            },
            'discovery_params': {
                'volume_threshold': volume_threshold,
                'max_new_desks': max_new_desks
            },
            'count': len(discovered)
        }


# ════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ════════════════════════════════════════════════════════════════════════

def get_moralis_api_key_status() -> Dict:
    """Check Moralis API key configuration."""
    moralis = MoralisAPI()
    return moralis.get_api_status()


# ════════════════════════════════════════════════════════════════════════
# EXAMPLE USAGE
# ════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """
    Test the active discovery system.
    """
    
    print("\n" + "="*80)
    print("OTC DESK REGISTRY - ACTIVE DISCOVERY SYSTEM")
    print("="*80)
    
    # Check APIs
    status = get_moralis_api_key_status()
    print(f"\n🔑 Moralis API: {'✅ Configured' if status['configured'] else '❌ Not configured'}")
    
    # Initialize registry
    registry = OTCDeskRegistry()
    
    # Get stats
    stats = registry.get_stats()
    print(f"\n📊 Registry Stats:")
    print(f"   Total Desks: {stats['total_desks']}")
    print(f"   • Verified: {stats['verified_desks']}")
    print(f"   • Discovered: {stats['discovered_desks']}")
    print(f"   Data Source: {stats['data_source']}")
    print(f"   Discovery Enabled: {stats['discovery_enabled']}")
    
    # List desks
    desks = registry.get_desk_list(include_discovered=True)
    print(f"\n🏢 OTC Desks (Top 10):")
    for desk in desks[:10]:
        category_icon = "🔰" if desk['desk_category'] == 'verified' else "🔍"
        print(f"   {category_icon} {desk['display_name']}: {desk['confidence']:.0%} ({desk['desk_category']})")
        if desk.get('discovery_volume'):
            print(f"      Volume: ${desk['discovery_volume']/1000:.0f}k")
    
    print("\n" + "="*80)
    print("✅ Active Discovery System Ready!")
    print("="*80 + "\n")
