
"""
Shared Dependencies & Services - ALWAYS USE QUICK STATS FIRST
===============================================================

✨ IMPROVED VERSION - Quick Stats API als PRIMARY Strategy:
- IMMER zuerst Quick Stats API versuchen (unabhängig von TX Count)
- Nur bei Fehler/Unavailable → Fallback auf Transaction Processing
- 15x schneller für ALLE Registry Wallets

✨ NEW: LinkBuilder Service für schnelle Link-Generierung

Version: 6.1 - Always Quick Stats First + LinkBuilder
Date: 2025-01-06
"""

import os
import time
import logging
from typing import Dict, Optional, Any, List, Set
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

# Database
from app.core.backend_crypto_tracker.config.database import get_db
from app.core.otc_analysis.models.wallet_link import WalletLink

# ✨ API infrastructure
from app.core.otc_analysis.blockchain.wallet_stats import WalletStatsAPI
from app.core.otc_analysis.utils.api_error_tracker import api_error_tracker
from app.core.otc_analysis.utils.api_health import ApiHealthMonitor

# Detection Services
from app.core.otc_analysis.detection.otc_detector import OTCDetector
from app.core.otc_analysis.detection.wallet_profiler import WalletProfiler
from app.core.otc_analysis.detection.flow_tracer import FlowTracer

# Blockchain Services
from app.core.otc_analysis.blockchain.node_provider import NodeProvider
from app.core.otc_analysis.blockchain.block_scanner import BlockScanner
from app.core.otc_analysis.blockchain.transaction_extractor import TransactionExtractor
from app.core.otc_analysis.blockchain.balance_fetcher import BalanceFetcher  # ✨ NEW
from app.core.otc_analysis.blockchain.etherscan import EtherscanAPI

# ✨ NEW: Activity & Balance Analysis
from app.core.otc_analysis.discovery.activity_analyzer import ActivityAnalyzer
from app.core.otc_analysis.discovery.balance_scorer import BalanceScorer

# Data Sources
from app.core.otc_analysis.data_sources.price_oracle import PriceOracle
from app.core.otc_analysis.data_sources.otc_desks import OTCDeskRegistry
from app.core.otc_analysis.data_sources.wallet_labels import WalletLabelingService
from app.core.otc_analysis.discovery.discovery_scorer import DiscoveryScorer
from app.core.otc_analysis.discovery.volume_scorer import VolumeScorer

# Analysis Services
from app.core.otc_analysis.analysis.statistics_service import StatisticsService
from app.core.otc_analysis.analysis.graph_builder import GraphBuilderService
from app.core.otc_analysis.analysis.network_graph import NetworkAnalysisService
from app.core.otc_analysis.analysis.link_builder import LinkBuilder  # ✨ NEW
from app.core.otc_analysis.discovery.high_volume_analyzer import HighVolumeAnalyzer

# Database Models
from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
from app.core.otc_analysis.models.watchlist import WatchlistItem as OTCWatchlist
from app.core.otc_analysis.models.alert import Alert as OTCAlert
from app.core.otc_analysis.models.wallet_link import WalletLink

# Utils
from app.core.otc_analysis.utils.cache import CacheManager

# Validators
from app.core.otc_analysis.api.validators import validate_ethereum_address

logger = logging.getLogger(__name__)

# ============================================================================
# SERVICE INITIALIZATION
# ============================================================================

# Core services
cache_manager = CacheManager()

# Blockchain services
node_provider = NodeProvider(chain_id=1)
etherscan = EtherscanAPI(chain_id=1)

# PriceOracle WITH Etherscan injected
price_oracle = PriceOracle(
    cache_manager,
    etherscan,
    moralis_api_key=os.getenv('MORALIS_API_KEY')  # ✨ NEW
)
    
# API infrastructure
api_health_monitor = ApiHealthMonitor(cooldown_minutes=5, error_threshold=0.5)
wallet_stats_api = WalletStatsAPI(
    error_tracker=api_error_tracker,
    health_monitor=api_health_monitor
)

# WalletProfiler WITH PriceOracle AND WalletStatsAPI injected
wallet_profiler = WalletProfiler(
    price_oracle=price_oracle,
    wallet_stats_api=wallet_stats_api
)

# TransactionExtractor WITH Moralis support
transaction_extractor = TransactionExtractor(
    node_provider, 
    etherscan,
    use_moralis=True
)

# ✨ NEW: BalanceFetcher for current wallet balances
balance_fetcher = BalanceFetcher(
    cache_manager=cache_manager,
    price_oracle=price_oracle,
    chain="eth",
    cache_ttl=300  # 5 minutes
)

# ✨ NEW: Activity & Balance Analysis Services
activity_analyzer = ActivityAnalyzer(
    dormancy_threshold_days=90
)

balance_scorer = BalanceScorer(
    min_active_balance_usd=10_000
)

# Other services
otc_registry = OTCDeskRegistry(cache_manager)
labeling_service = WalletLabelingService(cache_manager)
otc_detector = OTCDetector(cache_manager, otc_registry, labeling_service)
flow_tracer = FlowTracer()
block_scanner = BlockScanner(node_provider, chain_id=1)

# Analysis services
statistics_service = StatisticsService(cache_manager)
graph_builder = GraphBuilderService(cache_manager)

# ✨ NEW: LinkBuilder service for fast link/edge generation
link_builder = LinkBuilder(cache_manager, transaction_extractor)

logger.info("✅ All OTC services initialized successfully")
logger.info(f"   • Strategy: ALWAYS Quick Stats First (15x faster)")
logger.info(f"   • WalletProfiler: with PriceOracle + WalletStatsAPI")
logger.info(f"   • WalletStatsAPI: Multi-tier fallback")
logger.info(f"   • TransactionExtractor: Moralis enabled")
logger.info(f"   • LinkBuilder: Fast link generation with caching")
logger.info(f"   • BalanceFetcher: Current balance tracking (5min cache)")  # ✨ NEW
logger.info(f"   • ActivityAnalyzer: Temporal pattern analysis (90d threshold)")  # ✨ NEW
logger.info(f"   • BalanceScorer: Combined balance + activity scoring")  # ✨ NEW

# ============================================================================
# DEPENDENCY FUNCTIONS
# ============================================================================

def get_current_user():
    """Get current authenticated user."""
    return "dev_user_123"


def get_otc_registry():
    """Dependency: Get OTC desk registry."""
    return otc_registry


def get_cache_manager():
    """Dependency: Get cache manager."""
    return cache_manager


def get_labeling_service():
    """Dependency: Get wallet labeling service."""
    return labeling_service


def get_transaction_extractor():
    """Dependency: Get transaction extractor."""
    return transaction_extractor


def get_price_oracle():
    """Dependency: Get price oracle."""
    return price_oracle


def get_wallet_profiler():
    """Dependency: Get wallet profiler."""
    return wallet_profiler


def get_otc_detector():
    """Dependency: Get OTC detector."""
    return otc_detector


def get_link_builder():
    """Dependency: Get link builder service."""
    return link_builder

def get_balance_fetcher():
    """Dependency: Get balance fetcher service."""
    return balance_fetcher


def get_activity_analyzer():
    """Dependency: Get activity analyzer service."""
    return activity_analyzer


def get_balance_scorer():
    """Dependency: Get balance scorer service."""
    return balance_scorer



# ============================================================================
# ✨ IMPROVED: ALWAYS USE QUICK STATS FIRST
# ============================================================================

async def ensure_registry_wallets_in_db(
    db: Session,
    max_to_fetch: int = 1,
    skip_if_recent: bool = True
):
    """
    Ensures registry wallets are in database.
    
    ✨ NEW STRATEGY - ALWAYS QUICK STATS FIRST:
    - PRIORITY 1: Try Quick Stats API (ALL wallets, any TX count)
    - PRIORITY 2: Fallback to Transaction Processing (only if Quick Stats fails)
    - 15x faster for ALL wallets
    
    ✅ FEATURES:
    - 12h cache
    - Multi-tier API fallback
    - Error tracking with summary at end
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from datetime import datetime, timedelta
    from sqlalchemy.exc import IntegrityError
    
    # ✅ 12h Cache check
    if skip_if_recent:
        cache_threshold = datetime.now() - timedelta(hours=12)
        
        recent_count = db.query(OTCWallet).filter(
            OTCWallet.updated_at >= cache_threshold
        ).count()
        
        if recent_count >= 3:
            logger.info(f"⚡ Fast path: {recent_count} wallets cached (12h)")
            return {"cached": True, "count": recent_count}
    
    stats = {
        "fetched": 0, 
        "kept": 0, 
        "skipped": 0, 
        "updated": 0,
        "quick_stats_used": 0,
        "transaction_processing_used": 0
    }
    
    try:
        desks = otc_registry.get_desk_list()
        all_addresses = []
        address_to_desk = {}
        
        logger.info(f"🔄 Auto-sync: Checking {len(desks)} OTC desks...")
        logger.info(f"   🚀 Strategy: ALWAYS Quick Stats First (15x faster)")
        
        # Collect all addresses
        for desk in desks:
            try:
                desk_name = desk.get('name', 'unknown')
                desk_info = otc_registry.get_desk_by_name(desk_name)
                
                if desk_info and 'addresses' in desk_info:
                    addresses = desk_info['addresses']
                    
                    if isinstance(addresses, list):
                        for addr in addresses:
                            all_addresses.append(addr)
                            address_to_desk[addr.lower()] = desk_info
                        logger.info(f"   ✅ {desk_name}: {len(addresses)} addresses")
                    elif isinstance(addresses, str):
                        all_addresses.append(addresses)
                        address_to_desk[addresses.lower()] = desk_info
                        logger.info(f"   ✅ {desk_name}: 1 address")
                    
            except Exception as e:
                logger.error(f"❌ Error processing desk {desk_name}: {e}")
                continue
        
        logger.info(f"📊 Found {len(all_addresses)} total addresses")
        
        if len(all_addresses) == 0:
            logger.warning(f"⚠️  No addresses found in registry!")
            return stats
        
        # Filter addresses (check cache)
        addresses_to_fetch = []
        cache_threshold = datetime.now() - timedelta(hours=12)
        
        for address in all_addresses:
            try:
                address_str = str(address).strip()
                
                if not address_str.startswith('0x') or len(address_str) != 42:
                    logger.warning(f"⚠️  Invalid address: {address_str}")
                    continue
                
                validated = validate_ethereum_address(address_str)
                
                wallet = db.query(OTCWallet).filter(
                    OTCWallet.address == validated
                ).first()
                
                if wallet and wallet.updated_at >= cache_threshold:
                    if wallet.confidence_score >= 40.0:
                        logger.info(
                            f"   ⚡ {validated[:10]}... cached "
                            f"(updated {wallet.updated_at.strftime('%H:%M')})"
                        )
                        stats["kept"] += 1
                        continue
                
                addresses_to_fetch.append(validated)
                    
            except Exception as e:
                logger.error(f"❌ Error validating address {address}: {e}")
                continue
        
        logger.info(
            f"🎯 Need to fetch: {len(addresses_to_fetch)} wallets "
            f"(kept {stats['kept']})"
        )
        
        # ====================================================================
        # ✨ NEW STRATEGY: ALWAYS TRY QUICK STATS FIRST
        # ====================================================================
        
        for address in addresses_to_fetch[:max_to_fetch]:
            try:
                logger.info(f"🔄 Auto-fetching {address[:10]}...")
                
                # ====================================================================
                # ✨ PRIORITY 1: ALWAYS TRY QUICK STATS FIRST (NO TX COUNT CHECK)
                # ====================================================================
                
                logger.info(f"   🚀 PRIORITY 1: Trying Quick Stats API (ALWAYS preferred)")
                
                quick_stats = wallet_stats_api.get_quick_stats(address)
                tx_count = quick_stats.get('total_transactions', 0)
                
                logger.info(f"   📊 Quick stats result: {tx_count} transactions")
                
                # ====================================================================
                # ✨ STRATEGY A: Use Quick Stats (if available)
                # ====================================================================
                
                if quick_stats.get('source') != 'none':
                    # SUCCESS: Quick Stats available - USE IT!
                    logger.info(f"   ✅ Quick Stats available from {quick_stats['source']}")
                    logger.info(f"   ⚡ Using aggregated data (NO transaction processing)")
                    
                    stats["quick_stats_used"] += 1
                    
                    # Get labels
                    desk_info = address_to_desk.get(address.lower())
                    
                    if desk_info:
                        labels = {
                            'entity_type': 'otc_desk',
                            'entity_name': desk_info.get('name'),
                            'labels': ['verified_otc_desk', 'registry', desk_info.get('type', 'otc')],
                            'source': 'registry',
                            'confidence': 1.0
                        }
                    else:
                        labels = {
                            'entity_type': 'unknown',
                            'entity_name': None,
                            'labels': [],
                            'source': 'none'
                        }
                    
                    # Create profile from quick stats (no TX processing)
                    profile = wallet_profiler._create_profile_from_quick_stats(
                        address, quick_stats, labels, tx_count
                    )
                    
                    otc_probability = wallet_profiler.calculate_otc_probability(profile)
                    confidence = otc_probability * 100
                    total_volume = quick_stats.get('total_value_usd', 0)
                    data_quality = 'high'  # Quick Stats = high quality
                    
                else:
                    # ====================================================================
                    # ✨ STRATEGY B: Fallback to Transaction Processing
                    # ====================================================================
                    
                    logger.warning(f"   ⚠️  Quick Stats unavailable from all APIs")
                    logger.warning(f"   ⚠️  FALLBACK: Will process transactions manually")
                    
                    stats["transaction_processing_used"] += 1
                    
                    # Fetch transactions
                    transactions = transaction_extractor.extract_wallet_transactions(
                        address,
                        include_internal=True,
                        include_tokens=True
                    )
                    
                    if not transactions:
                        logger.info(f"⚠️  No transactions found - skipping")
                        stats["skipped"] += 1
                        continue
                    
                    # Sample if too many
                    if len(transactions) > 50:
                        import random
                        logger.info(f"   📊 Sampling 50 of {len(transactions)} transactions")
                        transactions = random.sample(transactions, 50)
                    
                    # Enrich with USD
                    transactions = transaction_extractor.enrich_with_usd_value(
                        transactions,
                        price_oracle,
                        max_transactions=50
                    )
                    
                    # Get labels
                    desk_info = address_to_desk.get(address.lower())
                    
                    if desk_info:
                        labels = {
                            'entity_type': 'otc_desk',
                            'entity_name': desk_info.get('name'),
                            'labels': ['verified_otc_desk', 'registry'],
                            'source': 'registry',
                            'confidence': 1.0
                        }
                    else:
                        labels = labeling_service.get_wallet_labels(address)
                    
                    # Create profile
                    profile = wallet_profiler.create_profile(address, transactions, labels)
                    otc_probability = wallet_profiler.calculate_otc_probability(profile)
                    confidence = otc_probability * 100
                    total_volume = profile.get('total_volume_usd', 0)
                    data_quality = profile.get('data_quality', 'unknown')
                
                # ====================================================================
                # SAVE TO DATABASE
                # ====================================================================
                
                logger.info(f"📊 Profile complete:")
                logger.info(f"   • Method: {profile.get('profile_method', 'unknown')}")
                logger.info(f"   • Confidence: {confidence:.1f}%")
                logger.info(f"   • Volume: ${total_volume:,.0f}")
                logger.info(f"   • Data quality: {data_quality}")
                
                # Determine threshold
                if labels.get('source') == 'registry':
                    min_confidence = 40.0
                else:
                    min_confidence = 60.0
                
                if confidence >= min_confidence:
                    if labels.get('source') == 'registry':
                        entity_type = 'otc_desk'
                    else:
                        entity_type = labels.get('entity_type', 'unknown')
                    
                    existing_wallet = db.query(OTCWallet).filter(
                        OTCWallet.address == address
                    ).first()
                    
                    if existing_wallet:
                        # Update existing
                        existing_wallet.entity_type = entity_type
                        existing_wallet.entity_name = labels.get('entity_name')
                        existing_wallet.confidence_score = confidence
                        existing_wallet.total_volume = total_volume
                        existing_wallet.transaction_count = tx_count or len(transactions) if 'transactions' in locals() else 0
                        existing_wallet.updated_at = datetime.now()
                        
                        db.commit()
                        logger.info(f"✅ Updated {address[:10]}...")
                        stats["updated"] += 1
                    else:
                        # Insert new
                        wallet = OTCWallet(
                            address=address,
                            label=labels.get('entity_name') or f"{address[:8]}...",
                            entity_type=entity_type,
                            entity_name=labels.get('entity_name'),
                            confidence_score=confidence,
                            total_volume=total_volume,
                            transaction_count=tx_count or len(transactions) if 'transactions' in locals() else 0,
                            first_seen=datetime.now() - timedelta(days=365),
                            last_active=datetime.now(),
                            is_active=True,
                            tags=labels.get('labels', []),
                            created_at=datetime.now(),
                            updated_at=datetime.now()
                        )
                        
                        db.add(wallet)
                        db.commit()
                        logger.info(f"✅ Inserted {address[:10]}...")
                        stats["fetched"] += 1
                else:
                    logger.info(f"⚠️  Low confidence ({confidence:.1f}%) - not saving")
                    stats["skipped"] += 1
                
                time.sleep(0.3)
                
            except Exception as e:
                logger.error(f"❌ Error auto-fetching {address}: {e}", exc_info=True)
                db.rollback()
                stats["skipped"] += 1
                continue
        
        # ====================================================================
        # ✨ PRINT API ERROR SUMMARY & STRATEGY STATS
        # ====================================================================
        logger.info("\n" + "="*60)
        logger.info("EXECUTION COMPLETE - Printing Summary")
        logger.info("="*60)
        
        # Strategy stats
        logger.info(f"\n📊 STRATEGY USAGE:")
        logger.info(f"   • Quick Stats used: {stats['quick_stats_used']} (15x faster)")
        logger.info(f"   • Transaction Processing used: {stats['transaction_processing_used']} (fallback)")
        
        # API summary
        api_error_tracker.print_summary()
        
        if stats["fetched"] > 0 or stats["updated"] > 0:
            logger.info(
                f"\n✅ Auto-sync complete: "
                f"Fetched {stats['fetched']}, "
                f"Updated {stats['updated']}, "
                f"Kept {stats['kept']}, "
                f"Skipped {stats['skipped']}"
            )
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Auto-sync failed: {e}", exc_info=True)
        db.rollback()
        
        # Still print summary
        logger.info("\n" + "="*60)
        logger.info("EXECUTION FAILED - Printing Summary")
        logger.info("="*60)
        api_error_tracker.print_summary()
        
        return stats

# ============================================================================
# DISCOVERY FUNCTIONS
# ============================================================================

async def discover_new_otc_desks(
    db: Session,
    max_discoveries: int = 5
) -> List[Dict]:
    """🕵️ Discover new OTC desks through counterparty analysis."""
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from app.core.otc_analysis.discovery.counterparty_analyzer import CounterpartyAnalyzer
    
    logger.info("🕵️ Starting OTC discovery...")
    
    try:
        known_otc = db.query(OTCWallet).filter(
            OTCWallet.entity_type == 'otc_desk',
            OTCWallet.confidence_score >= 70.0
        ).all()
        
        if len(known_otc) < 2:
            logger.warning("⚠️ Need 2+ known OTC desks")
            return []
        
        known_addresses = [w.address for w in known_otc]
        logger.info(f"📊 Analyzing {len(known_addresses)} OTC desks...")
        
        analyzer = CounterpartyAnalyzer(
            db=db,
            transaction_extractor=transaction_extractor,
            wallet_profiler=wallet_profiler
        )
        
        candidates = analyzer.discover_counterparties(
            known_otc_addresses=known_addresses,
            min_interactions=2,
            min_volume=1_000_000,
            max_candidates=max_discoveries
        )
        
        if not candidates:
            logger.info("ℹ️ No candidates found")
            return []
        
        logger.info(f"🎯 Found {len(candidates)} candidates")
        
        discovered = []
        
        for candidate in candidates:
            address = candidate['address']
            
            existing = db.query(OTCWallet).filter(
                OTCWallet.address == address
            ).first()
            
            if existing:
                logger.info(f"   ⚠️ {address[:10]}... exists")
                continue
            
            logger.info(
                f"   🆕 {address[:10]}... - "
                f"Score: {candidate['discovery_score']}/100, "
                f"OTC links: {candidate['otc_interaction_count']}, "
                f"Volume: ${candidate['total_volume']:,.0f}"
            )
            
            try:
                # ✨ Try Quick Stats first
                quick_stats = wallet_stats_api.get_quick_stats(address)
                
                if quick_stats.get('source') != 'none':
                    logger.info(f"      ✅ Using Quick Stats for discovery")
                    
                    labels = {
                        'entity_type': 'otc_desk',
                        'entity_name': f"Discovered {address[:8]}",
                        'labels': ['discovered', 'counterparty_analysis'],
                        'source': 'discovery',
                        'confidence': candidate['discovery_score'] / 100
                    }
                    
                    profile = wallet_profiler._create_profile_from_quick_stats(
                        address, quick_stats, labels, quick_stats.get('total_transactions', 0)
                    )
                else:
                    # Fallback to transaction processing
                    transactions = transaction_extractor.extract_wallet_transactions(
                        address,
                        include_internal=True,
                        include_tokens=True
                    )
                    
                    if not transactions:
                        continue
                    
                    transactions = transaction_extractor.enrich_with_usd_value(
                        transactions,
                        price_oracle,
                        max_transactions=30
                    )
                    
                    labels = {
                        'entity_type': 'otc_desk',
                        'entity_name': f"Discovered {address[:8]}",
                        'labels': ['discovered', 'counterparty_analysis'],
                        'source': 'discovery',
                        'confidence': candidate['discovery_score'] / 100
                    }
                    
                    profile = wallet_profiler.create_profile(address, transactions, labels)
                
                otc_prob = wallet_profiler.calculate_otc_probability(profile)
                confidence = otc_prob * 100
                combined = (confidence + candidate['discovery_score']) / 2
                
                logger.info(f"      Profile: {combined:.1f}% confidence")
                
                if combined >= 60.0:
                    wallet = OTCWallet(
                        address=address,
                        label=f"Discovered {address[:8]}",
                        entity_type='otc_desk',
                        entity_name=f"Discovered OTC {address[:8]}",
                        confidence_score=combined,
                        total_volume=profile.get('total_volume_usd', 0),
                        transaction_count=profile.get('total_transactions', 0),
                        first_seen=candidate['first_seen'],
                        last_active=candidate['last_seen'],
                        is_active=True,
                        tags=['discovered', 'counterparty'],
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    
                    db.add(wallet)
                    db.commit()
                    
                    discovered.append({
                        'address': address,
                        'confidence': combined,
                        'discovery_score': candidate['discovery_score'],
                        'otc_interactions': candidate['otc_interaction_count']
                    })
                    
                    logger.info(f"      ✅ Saved to DB")
                else:
                    logger.info(f"      ⚠️ Low confidence ({combined:.1f}%)")
                
            except Exception as e:
                logger.error(f"❌ Error profiling {address[:10]}: {e}")
                continue
        
        logger.info(f"✅ Discovery complete: {len(discovered)} new OTC desks")
        return discovered
        
    except Exception as e:
        logger.error(f"❌ Discovery failed: {e}", exc_info=True)
        db.rollback()
        return []


async def discover_from_last_5_transactions(
    db: Session,
    otc_address: str,
    num_transactions: int = 5,
    filter_known_entities: bool = True
) -> List[Dict]:
    """
    🔍 Analyze counterparties from last N transactions.
    
    ✅ IMPROVED VERSION:
    - Holt nur num_transactions * 2 TXs (statt alle)
    - Source OTC Desk wird in Scoring berücksichtigt
    - Niedrigerer Save Threshold (40% statt 50%)
    - Adaptive Final Score Berechnung
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from app.core.otc_analysis.discovery.simple_analyzer import SimpleLastTxAnalyzer
    
    logger.info(f"🔍 Simple Discovery: Last {num_transactions} TXs from {otc_address[:10]}...")
    
    if filter_known_entities:
        logger.info("   🏷️ Moralis label filtering: ENABLED")
    else:
        logger.info("   🏷️ Moralis label filtering: DISABLED")
    
    try:
        # Get known OTC desks
        known_otc = db.query(OTCWallet).filter(
            OTCWallet.entity_type == 'otc_desk',
            OTCWallet.confidence_score >= 70.0
        ).all()
        
        known_addresses = [w.address for w in known_otc]
        
        # ✅ ADD: Source OTC Desk to known addresses
        if otc_address.lower() not in [addr.lower() for addr in known_addresses]:
            known_addresses.append(otc_address)
            logger.info(f"   ✅ Added source OTC desk {otc_address[:10]}... to known list")
        
        # Initialize analyzer
        analyzer = SimpleLastTxAnalyzer(
            db=db,
            transaction_extractor=transaction_extractor,
            wallet_profiler=wallet_profiler,
            price_oracle=price_oracle,
            wallet_stats_api=wallet_stats_api
        )
        
        # Initialize scorer (WITH known addresses INCLUDING source)
        discovery_scorer = DiscoveryScorer(known_addresses)
        
        # ====================================================================
        # ✅ FIX: LIMIT TRANSACTIONS IMMEDIATELY
        # ====================================================================
        # Get transactions (MIT LIMIT!)
        transactions = transaction_extractor.extract_wallet_transactions(
            otc_address,
            include_internal=True,
            include_tokens=True
        )[:num_transactions * 2]  # ✅ Begrenze SOFORT nach Fetch
        
        if not transactions:
            logger.info("ℹ️ No transactions found")
            return []
        
        # Sort and take recent N
        recent_txs = sorted(
            transactions, 
            key=lambda x: x.get('timestamp', datetime.min), 
            reverse=True
        )[:num_transactions]
        
        logger.info(f"📊 Analyzing {len(recent_txs)} recent transactions...")
        
        # Extract counterparties
        counterparties_data = {}
        filtered_count = 0
        
        for tx in recent_txs:
            # Determine counterparty
            if tx['from_address'].lower() == otc_address.lower():
                counterparty = tx['to_address']
                label = tx.get('to_address_label')
                entity = tx.get('to_address_entity')
                is_known = tx.get('to_is_known_entity', False)
            else:
                counterparty = tx['from_address']
                label = tx.get('from_address_label')
                entity = tx.get('from_address_entity')
                is_known = tx.get('from_is_known_entity', False)
            
            # Filter known entities
            if filter_known_entities and is_known:
                filtered_count += 1
                logger.info(
                    f"   ⏭️  Skipping {counterparty[:10]}... "
                    f"(known entity: {label or entity})"
                )
                continue
            
            # Track counterparty
            if counterparty not in counterparties_data:
                counterparties_data[counterparty] = {
                    'address': counterparty,
                    'tx_count': 0,
                    'total_volume': 0,
                    'first_seen': tx['timestamp'],
                    'last_seen': tx['timestamp'],
                    'moralis_label': label,
                    'moralis_entity': entity,
                    'is_known_entity': is_known
                }
            
            # Update stats
            cp_data = counterparties_data[counterparty]
            cp_data['tx_count'] += 1
            
            if tx.get('usd_value'):
                cp_data['total_volume'] += tx['usd_value']
            
            if tx['timestamp'] < cp_data['first_seen']:
                cp_data['first_seen'] = tx['timestamp']
            if tx['timestamp'] > cp_data['last_seen']:
                cp_data['last_seen'] = tx['timestamp']
        
        logger.info(
            f"📊 Found {len(counterparties_data)} unique counterparties "
            f"(filtered {filtered_count} known entities)"
        )
        
        if not counterparties_data:
            logger.info("ℹ️ No valid counterparties after filtering")
            return []
        
        # Analyze each counterparty
        discovered = []
        
        for address, cp_data in counterparties_data.items():
            # Skip if already in DB
            existing = db.query(OTCWallet).filter(
                OTCWallet.address == address
            ).first()
            
            if existing:
                logger.info(
                    f"   ⚠️ {address[:10]}... already exists "
                    f"(score: {existing.confidence_score:.1f}%)"
                )
                continue
            
            logger.info(f"   🔍 Analyzing {address[:10]}...")
            
            # Analyze
            analysis = analyzer.analyze_counterparty(address)
            
            if not analysis:
                logger.info(f"      ⚠️ Analysis failed")
                continue
            
            # Get full transactions for scoring
            cp_transactions = transaction_extractor.extract_wallet_transactions(
                address,
                include_internal=True,
                include_tokens=True
            )
            
            # ====================================================================
            # ✅ FIX: INJECT SOURCE OTC DESK IN SCORER
            # ====================================================================
            discovery_result = discovery_scorer.score_discovered_wallet(
                address=address,
                transactions=cp_transactions,
                counterparty_data=cp_data,
                profile=analysis.get('profile', {}),
                source_otc_desk=otc_address  # ✅ WICHTIG!
            )
            
            # ====================================================================
            # ✅ FIX: ADAPTIVE FINAL SCORE BERECHNUNG
            # ====================================================================
            base_confidence = analysis['confidence']
            discovery_score = discovery_result['score']
            
            # Adaptive Gewichtung basierend auf Datenmenge
            if len(cp_transactions) >= 50:
                # Viele TXs → Discovery Score wichtiger
                final_confidence = (base_confidence * 0.4) + (discovery_score * 0.6)
            else:
                # Wenige TXs → Base Score wichtiger
                final_confidence = (base_confidence * 0.6) + (discovery_score * 0.4)
            
            logger.info(
                f"      📊 Scores: Base={base_confidence:.1f}%, "
                f"Discovery={discovery_score:.0f}, "
                f"Final={final_confidence:.1f}%"
            )
            
            if cp_data.get('moralis_label'):
                logger.info(
                    f"      🏷️ Moralis: {cp_data['moralis_label']} "
                    f"(Entity: {cp_data.get('moralis_entity', 'N/A')})"
                )
            
            logger.info(f"      💡 Recommendation: {discovery_result['recommendation']}")
            
            # ====================================================================
            # ✅ FIX: NIEDRIGERER THRESHOLD (40% statt 50%)
            # ====================================================================
            if final_confidence >= 40.0:  # ✅ Runter von 50%!
                tags = ['discovered', 'last_tx_analysis', discovery_result['recommendation']]
                
                if cp_data.get('moralis_label'):
                    tags.append(f"moralis:{cp_data['moralis_label'][:30]}")
                
                wallet = OTCWallet(
                    address=address,
                    label=f"Discovered {address[:8]}",
                    entity_type='otc_desk',
                    entity_name=cp_data.get('moralis_label') or f"Discovered OTC {address[:8]}",
                    confidence_score=final_confidence,
                    total_volume=analysis['total_volume'],
                    transaction_count=analysis['transaction_count'],
                    first_seen=analysis['first_seen'],
                    last_active=analysis['last_seen'],
                    is_active=True,
                    tags=tags,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                
                db.add(wallet)
                db.commit()
                
                discovered.append({
                    'address': address,
                    'confidence': final_confidence,
                    'volume': analysis['total_volume'],
                    'tx_count': analysis['transaction_count'],
                    'counterparty_data': cp_data,
                    'discovery_breakdown': discovery_result['breakdown'],
                    'moralis_label': cp_data.get('moralis_label'),
                    'moralis_entity': cp_data.get('moralis_entity')
                })
                
                logger.info(
                    f"      ✅ Saved to DB "
                    f"(threshold: 40%, confidence: {final_confidence:.1f}%)"
                )
            else:
                logger.info(f"      ⚠️ Below threshold (40%)")
        
        logger.info(
            f"✅ Discovery complete: {len(discovered)} new wallets found "
            f"(filtered {filtered_count} known entities)"
        )
        
        return discovered
        
    except Exception as e:
        logger.error(f"❌ Discovery failed: {e}", exc_info=True)
        db.rollback()
        return []

async def discover_high_volume_from_transactions(
    db: Session,
    source_address: str,
    num_transactions: int = 5,
    min_volume_threshold: float = 1_000_000,
    filter_known_entities: bool = True,
    auto_sync_transactions: bool = True,
    save_links: bool = True  # ✨ NEW: Enable link saving
) -> List[Dict]:
    """
    🔍 Discover high-volume wallets from last N transactions.
    
    ✨ ENHANCED v3: BALANCE + ACTIVITY INTEGRATION
    - Auto-sync transactions (wenn enabled)
    - Current balance analysis via BalanceFetcher
    - Activity pattern analysis via ActivityAnalyzer
    - Combined scoring via BalanceScorer
    - Accurate classification (active vs dormant)
    
    Args:
        db: Database session
        source_address: Source wallet to analyze
        num_transactions: Number of recent transactions to check
        min_volume_threshold: Minimum USD volume to save (default: $1M)
        filter_known_entities: Filter out known exchanges/protocols
        auto_sync_transactions: Auto-sync transactions for discovered wallets
        
    Returns:
        List of discovered high-volume wallets with enhanced metadata
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from datetime import datetime
    
    logger.info(f"🔍 High Volume Discovery v3: Last {num_transactions} TXs from {source_address[:10]}...")
    logger.info(f"   💰 Min volume threshold: ${min_volume_threshold:,.0f}")
    logger.info(f"   🔄 Auto-sync TXs: {auto_sync_transactions}")
    logger.info(f"   🎯 Balance + Activity analysis: ENABLED")
    
    if filter_known_entities:
        logger.info("   🏷️ Moralis label filtering: ENABLED")
    else:
        logger.info("   🏷️ Moralis label filtering: DISABLED")
    
    try:
        # ====================================================================
        # ✨ INITIALIZE ANALYZER WITH NEW SERVICES
        # ====================================================================
        
        analyzer = HighVolumeAnalyzer(
            db=db,
            transaction_extractor=transaction_extractor,
            wallet_profiler=wallet_profiler,
            price_oracle=price_oracle,
            wallet_stats_api=wallet_stats_api,
            balance_fetcher=balance_fetcher,      # ✨ NEW
            activity_analyzer=activity_analyzer,  # ✨ NEW
            balance_scorer=balance_scorer         # ✨ NEW
        )
        
        # Initialize scorer
        volume_scorer = VolumeScorer(min_volume_threshold=min_volume_threshold)
        
        # Get counterparties from last N transactions
        counterparties_data = analyzer.discover_high_volume_counterparties(
            source_address=source_address,
            num_transactions=num_transactions,
            filter_known_entities=filter_known_entities
        )
        
        if not counterparties_data:
            logger.info("ℹ️ No valid counterparties found")
            return []
        
        logger.info(f"📊 Analyzing {len(counterparties_data)} counterparties...")
        
        # Analyze each counterparty
        discovered = []
        
        for cp_data in counterparties_data:
            address = cp_data['address']
            
            # Skip if already in DB
            existing = db.query(OTCWallet).filter(
                OTCWallet.address == address
            ).first()
            
            if existing:
                logger.info(
                    f"   ⚠️ {address[:10]}... already exists "
                    f"(type: {existing.entity_type}, score: {existing.confidence_score:.1f}%)"
                )
                continue
            
            logger.info(f"   🔍 Analyzing {address[:10]}...")
            
            # ================================================================
            # ✨ ANALYZE WITH BALANCE + ACTIVITY (NEW!)
            # ================================================================
            
            analysis = analyzer.analyze_volume_profile(
                address,
                include_balance_analysis=True,   # ✨ NEW
                include_activity_analysis=True   # ✨ NEW
            )
            
            if not analysis:
                logger.info(f"      ⚠️ Analysis failed")
                continue
            
            # Get full transactions for scoring
            cp_transactions = transaction_extractor.extract_wallet_transactions(
                address,
                include_internal=True,
                include_tokens=True
            )
            
            # ================================================================
            # ✨ SCORE WITH BALANCE + ACTIVITY (NEW!)
            # ================================================================
            
            scoring_result = volume_scorer.score_high_volume_wallet(
                address=address,
                transactions=cp_transactions,
                counterparty_data=cp_data,
                profile=analysis.get('profile', {}),
                balance_analysis=analysis.get('balance_analysis'),      # ✨ NEW
                activity_analysis=analysis.get('activity_analysis'),    # ✨ NEW
                combined_scoring=analysis.get('combined_scoring')       # ✨ NEW
            )
            
            volume_score = scoring_result['score']
            base_score = scoring_result.get('base_score', volume_score)
            classification = scoring_result['classification']
            total_volume = analysis['total_volume']
            
            # ================================================================
            # LOG RESULTS
            # ================================================================
            
            logger.info(
                f"      📊 Scores: Final={volume_score:.1f}/100, "
                f"Base={base_score}/100, "
                f"Volume=${total_volume:,.0f}"
            )
            
            # Log balance info
            if analysis.get('balance_analysis'):
                balance = analysis['balance_analysis']['total_balance_usd']
                logger.info(f"      💰 Balance: ${balance:,.2f}")
            
            # Log activity info
            if analysis.get('activity_analysis'):
                pattern = analysis['activity_analysis']['pattern']['pattern']
                logger.info(f"      📅 Activity: {pattern} pattern")
            
            if cp_data.get('moralis_label'):
                logger.info(
                    f"      🏷️ Moralis: {cp_data['moralis_label']} "
                    f"(Entity: {cp_data.get('moralis_entity', 'N/A')})"
                )
            
            logger.info(f"      💡 Classification: {classification['classification']}")
            
            # ================================================================
            # CHECK THRESHOLDS
            # ================================================================
            
            # Check volume threshold
            if not scoring_result['meets_threshold']:
                logger.info(
                    f"      ⚠️ Below volume threshold "
                    f"(${total_volume:,.0f} < ${min_volume_threshold:,.0f})"
                )
                continue
            
            # ✨ ADJUSTED MINIMUM SCORE (accounts for modifiers)
            min_score = 30  # Lower threshold due to modifiers
            
            if volume_score < min_score:
                logger.info(f"      ⚠️ Score too low ({volume_score:.1f}/100, min: {min_score})")
                continue
            
            # ================================================================
            # PREPARE TAGS
            # ================================================================
            
            tags = [
                'discovered',
                'high_volume',
                'volume_analysis'
            ] + classification.get('tags', [])
            
            if cp_data.get('moralis_label'):
                tags.append(f"moralis:{cp_data['moralis_label'][:30]}")
            
            # ✨ Add enhanced tags
            if analysis.get('balance_analysis'):
                balance_ratio = (
                    analysis['balance_analysis']['total_balance_usd'] / total_volume 
                    if total_volume > 0 else 0
                )
                if balance_ratio < 0.01:
                    tags.append('depleted')
                elif balance_ratio >= 1.0:
                    tags.append('accumulating')
            
            if analysis.get('activity_analysis'):
                pattern = analysis['activity_analysis']['pattern']['pattern']
                if pattern == 'dormant':
                    tags.append('dormant')
                elif pattern in ['sustained', 'active']:
                    tags.append('active')
            
# ================================================================
            # ✅ STEP 1: Save Wallet to DB
            # ================================================================
            
            wallet = OTCWallet(
                address=address,
                label=cp_data.get('moralis_label') or f"High Volume {address[:8]}",
                entity_type='high_volume_wallet',
                entity_name=cp_data.get('moralis_label') or f"High Volume {address[:8]}",
                confidence_score=volume_score,
                total_volume=total_volume,
                transaction_count=analysis['transaction_count'],
                first_seen=analysis['first_seen'],
                last_active=analysis['last_seen'],
                is_active=True,
                tags=tags,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            db.add(wallet)
            db.commit()
            
            logger.info(
                f"      ✅ Saved to DB "
                f"(type: high_volume_wallet, score: {volume_score:.1f}/100)"
            )
            
            # ================================================================
            # ✨ STEP 1.5: AUTO-CREATE WALLET LINKS
            # ================================================================
            
            if save_links:
                logger.info(f"      🔗 Creating wallet links...")
                
                try:
                    from app.core.otc_analysis.models.wallet_link import WalletLink
                    
                    links_created = 0
                    
                    # ============================================================
                    # LINK 1: Discovered Wallet ↔ Source Address
                    # ============================================================
                    
                    # Check if source is in our wallets table
                    source_wallet = db.query(OTCWallet).filter(
                        OTCWallet.address == source_address.lower()
                    ).first()
                    
                    if source_wallet:
                        # Calculate link from counterparty data
                        link_volume = cp_data.get('total_volume', 0)
                        link_tx_count = cp_data.get('tx_count', 0)
                        
                        # Link strength
                        volume_score_link = min(link_volume / 10_000_000, 1.0) * 40
                        frequency_score_link = min(link_tx_count / 100, 1.0) * 30
                        recency_score_link = 30  # Recent discovery
                        link_strength = volume_score_link + frequency_score_link + recency_score_link
                        
                        # OTC confidence
                        otc_conf = 0.0
                        if link_volume > 100000:
                            otc_conf += 40
                        if link_tx_count > 10:
                            otc_conf += 30
                        if source_wallet.entity_type == 'otc_desk':
                            otc_conf += 30
                        
                        # Check if link already exists
                        existing_link = db.query(WalletLink).filter(
                            WalletLink.from_address == source_address.lower(),
                            WalletLink.to_address == address.lower()
                        ).first()
                        
                        if not existing_link:
                            link = WalletLink(
                                from_address=source_address.lower(),
                                to_address=address.lower(),
                                from_wallet_type=source_wallet.entity_type,
                                to_wallet_type='high_volume_wallet',
                                from_wallet_label=source_wallet.label,
                                to_wallet_label=wallet.label,
                                transaction_count=link_tx_count,
                                total_volume_usd=link_volume,
                                avg_transaction_usd=link_volume / link_tx_count if link_tx_count > 0 else 0,
                                first_transaction=cp_data.get('first_seen'),
                                last_transaction=cp_data.get('last_seen'),
                                analysis_start=datetime.now() - timedelta(days=30),
                                analysis_end=datetime.now(),
                                link_strength=link_strength,
                                is_suspected_otc=otc_conf >= 60,
                                otc_confidence=otc_conf,
                                volume_score=volume_score_link,
                                frequency_score=frequency_score_link,
                                recency_score=recency_score_link,
                                detected_patterns=['discovered_via_analysis'],
                                flow_type='discovery',
                                data_source='discovery',
                                data_quality='high',
                                is_active=True,
                                created_at=datetime.now(),
                                last_calculated=datetime.now()
                            )
                            
                            db.add(link)
                            links_created += 1
                            
                            logger.info(
                                f"         ✅ Link: {source_address[:10]}...↔{address[:10]}... "
                                f"(${link_volume:,.0f}, {link_tx_count} TXs)"
                            )
                    
                    # ============================================================
                    # LINK 2: Discovered Wallet ↔ Other OTC Desks (from TXs)
                    # ============================================================
                    
                    if auto_sync_transactions and cp_data.get('tx_sync'):
                        # We have synced TXs - find links to OTC desks
                        
                        otc_desks = db.query(OTCWallet).filter(
                            OTCWallet.entity_type.in_(['otc_desk', 'exchange']),
                            OTCWallet.is_active == True
                        ).all()
                        
                        otc_addresses = [w.address.lower() for w in otc_desks]
                        
                        if len(otc_addresses) > 0:
                            # Query transactions between discovered wallet and OTC desks
                            otc_links = db.query(
                                Transaction.from_address,
                                Transaction.to_address,
                                func.count(Transaction.tx_hash).label('tx_count'),
                                func.sum(Transaction.usd_value).label('total_volume'),
                                func.min(Transaction.timestamp).label('first_tx'),
                                func.max(Transaction.timestamp).label('last_tx')
                            ).filter(
                                or_(
                                    and_(
                                        Transaction.from_address == address.lower(),
                                        Transaction.to_address.in_(otc_addresses)
                                    ),
                                    and_(
                                        Transaction.from_address.in_(otc_addresses),
                                        Transaction.to_address == address.lower()
                                    )
                                )
                            ).group_by(
                                Transaction.from_address,
                                Transaction.to_address
                            ).having(
                                func.coalesce(func.sum(Transaction.usd_value), 0) >= 100000
                            ).all()
                            
                            for otc_link in otc_links:
                                try:
                                    # Get wallet metadata
                                    from_wallet = db.query(OTCWallet).filter(
                                        OTCWallet.address == otc_link.from_address
                                    ).first()
                                    
                                    to_wallet = db.query(OTCWallet).filter(
                                        OTCWallet.address == otc_link.to_address
                                    ).first()
                                    
                                    if not from_wallet or not to_wallet:
                                        continue
                                    
                                    # Check if link already exists
                                    existing_link = db.query(WalletLink).filter(
                                        WalletLink.from_address == otc_link.from_address,
                                        WalletLink.to_address == otc_link.to_address
                                    ).first()
                                    
                                    if existing_link:
                                        continue
                                    
                                    link_vol = float(otc_link.total_volume or 0)
                                    link_txs = int(otc_link.tx_count or 0)
                                    
                                    # Calculate scores
                                    vol_score = min(link_vol / 10_000_000, 1.0) * 40
                                    freq_score = min(link_txs / 100, 1.0) * 30
                                    rec_score = 30
                                    strength = vol_score + freq_score + rec_score
                                    
                                    otc_conf = 70 if 'otc_desk' in [from_wallet.entity_type, to_wallet.entity_type] else 50
                                    
                                    link = WalletLink(
                                        from_address=otc_link.from_address,
                                        to_address=otc_link.to_address,
                                        from_wallet_type=from_wallet.entity_type,
                                        to_wallet_type=to_wallet.entity_type,
                                        from_wallet_label=from_wallet.label,
                                        to_wallet_label=to_wallet.label,
                                        transaction_count=link_txs,
                                        total_volume_usd=link_vol,
                                        avg_transaction_usd=link_vol / link_txs if link_txs > 0 else 0,
                                        first_transaction=otc_link.first_tx,
                                        last_transaction=otc_link.last_tx,
                                        analysis_start=datetime.now() - timedelta(days=30),
                                        analysis_end=datetime.now(),
                                        link_strength=strength,
                                        is_suspected_otc=otc_conf >= 60,
                                        otc_confidence=otc_conf,
                                        volume_score=vol_score,
                                        frequency_score=freq_score,
                                        recency_score=rec_score,
                                        detected_patterns=['auto_discovered'],
                                        flow_type='transaction_based',
                                        data_source='transactions',
                                        data_quality='high',
                                        is_active=True,
                                        created_at=datetime.now(),
                                        last_calculated=datetime.now()
                                    )
                                    
                                    db.add(link)
                                    links_created += 1
                                    
                                    logger.info(
                                        f"         ✅ Link: {otc_link.from_address[:10]}...↔"
                                        f"{otc_link.to_address[:10]}... "
                                        f"(${link_vol:,.0f}, {link_txs} TXs)"
                                    )
                                    
                                except Exception as link_error:
                                    logger.warning(f"         ⚠️ Error creating OTC link: {link_error}")
                                    continue
                    
                    # Commit all links
                    if links_created > 0:
                        db.commit()
                        logger.info(f"      💾 Saved {links_created} wallet links")
                        
                        # Add to response
                        cp_data['links_created'] = links_created
                    
                except Exception as link_error:
                    logger.error(f"      ❌ Error creating links: {link_error}")
                    db.rollback()
                    cp_data['links_created'] = 0
            
            # ================================================================
            # ✅ STEP 2: AUTO-SYNC TRANSACTIONS
            # ================================================================
            
            if auto_sync_transactions:
                logger.info(f"      🔄 Auto-syncing transactions for {address[:10]}...")
                
                try:
                    sync_stats = await sync_wallet_transactions_to_db(
                        db=db,
                        wallet_address=address,
                        max_transactions=100,
                        force_refresh=False,
                        skip_enrichment=False
                    )
                    
                    logger.info(
                        f"         ✅ Synced {sync_stats['saved_count']} transactions "
                        f"({sync_stats['updated_count']} updated, "
                        f"{sync_stats['skipped_count']} skipped)"
                    )
                    
                    # Add sync stats to response
                    cp_data['tx_sync'] = sync_stats
                    
                except Exception as sync_error:
                    logger.error(f"         ⚠️ TX sync failed: {sync_error}")
                    cp_data['tx_sync'] = {"error": str(sync_error)}
            
            # ================================================================
            # Add to discovered list
            # ================================================================
            
            discovered.append({
                'address': address,
                'volume_score': volume_score,
                'base_score': base_score,
                'total_volume': total_volume,
                'tx_count': analysis['transaction_count'],
                'avg_transaction': analysis['avg_transaction'],
                'classification': classification['classification'],
                'tags': tags,
                'counterparty_data': cp_data,
                'volume_breakdown': scoring_result.get('breakdown', {}),
                'score_modifiers': scoring_result.get('modifiers', {}),
                'moralis_label': cp_data.get('moralis_label'),
                'moralis_entity': cp_data.get('moralis_entity'),
                'has_balance_data': analysis.get('balance_analysis') is not None,
                'has_activity_data': analysis.get('activity_analysis') is not None,
                'enhanced_classification': analysis.get('enhanced_classification'),
                'links_created': cp_data.get('links_created', 0)  # ✨ NEW
            })
        
        logger.info(
            f"✅ High Volume Discovery complete: {len(discovered)} wallets found "
            f"(threshold: ${min_volume_threshold:,.0f})"
        )
        
        if auto_sync_transactions and len(discovered) > 0:
            logger.info(
                f"   💾 Transactions synced for all {len(discovered)} discovered wallets"
            )
        
        if save_links:
            total_links = sum(w.get('links_created', 0) for w in discovered)
            logger.info(f"   🔗 Created {total_links} wallet links")
        
        # ✨ Log enhancement stats
        balance_count = sum(1 for w in discovered if w['has_balance_data'])
        activity_count = sum(1 for w in discovered if w['has_activity_data'])
        
        logger.info(
            f"   🎯 Enhanced analysis: "
            f"{balance_count} with balance, "
            f"{activity_count} with activity"
        )
        
        return discovered
        
    except Exception as e:
        logger.error(f"❌ High volume discovery failed: {e}", exc_info=True)
        db.rollback()
        return []
async def save_wallet_links_to_db(
    db: Session,
    start_date: datetime,
    end_date: datetime,
    min_flow_size: float = 100000,
    max_links: int = 1000,
    include_high_volume: bool = True,
    use_transactions: bool = True,
    force_refresh: bool = False
) -> Dict[str, Any]:
    """
    Speichert Wallet-Links in die Datenbank.
    
    ✨ FEATURES:
    - Aggregiert Transaktionen zwischen Wallets
    - Berechnet Link-Strength & OTC-Confidence
    - Erkennt Patterns
    - Vermeidet Duplikate
    
    Args:
        db: Database session
        start_date: Start of analysis window
        end_date: End of analysis window
        min_flow_size: Minimum USD volume for a link
        max_links: Maximum links to save
        include_high_volume: Include high-volume wallets
        use_transactions: Use transaction data
        force_refresh: Overwrite existing links
        
    Returns:
        Stats dictionary
    """
    from sqlalchemy import func, or_, and_
    from app.core.otc_analysis.models.transaction import Transaction
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from datetime import datetime
    
    logger.info("="*80)
    logger.info("💾 SAVING WALLET LINKS TO DATABASE")
    logger.info("="*80)
    logger.info(f"📅 Analysis window: {start_date.date()} → {end_date.date()}")
    logger.info(f"💰 Min flow size: ${min_flow_size:,.0f}")
    logger.info(f"🔄 Force refresh: {force_refresh}")
    
    stats = {
        "wallets_analyzed": 0,
        "links_found": 0,
        "links_saved": 0,
        "links_updated": 0,
        "links_skipped": 0,
        "errors": 0,
        "start_time": datetime.now()
    }
    
    try:
        # ================================================================
        # STEP 1: GET WALLETS TO ANALYZE
        # ================================================================
        
        logger.info("🔍 Step 1: Loading wallets...")
        
        # Get OTC desks
        otc_wallets = db.query(OTCWallet).filter(
            OTCWallet.entity_type != 'high_volume_wallet',
            OTCWallet.is_active == True
        ).all()
        
        logger.info(f"   ✅ Found {len(otc_wallets)} OTC desks")
        
        # Get high-volume wallets
        high_volume_wallets = []
        if include_high_volume:
            high_volume_wallets = db.query(OTCWallet).filter(
                OTCWallet.entity_type == 'high_volume_wallet',
                OTCWallet.is_active == True
            ).all()
            logger.info(f"   ✅ Found {len(high_volume_wallets)} high-volume wallets")
        
        all_wallets = otc_wallets + high_volume_wallets
        stats["wallets_analyzed"] = len(all_wallets)
        
        if len(all_wallets) == 0:
            logger.warning("⚠️ No wallets found")
            return stats
        
        # Create lookup for wallet metadata
        wallet_lookup = {
            w.address.lower(): {
                'type': w.entity_type,
                'label': w.label or f"{w.address[:8]}...",
                'cluster_id': None  # TODO: Add clustering
            }
            for w in all_wallets
        }
        
        wallet_addresses = list(wallet_lookup.keys())
        logger.info(f"📋 Prepared {len(wallet_addresses)} addresses")
        
        # ================================================================
        # STEP 2: AGGREGATE TRANSACTIONS BETWEEN WALLETS
        # ================================================================
        
        logger.info("🔍 Step 2: Aggregating transactions...")
        
        if not use_transactions:
            logger.info("   ⏭️ Skipping transaction aggregation (use_transactions=False)")
            return stats
        
        # Query: Aggregate transactions between wallets
        link_data = db.query(
            Transaction.from_address,
            Transaction.to_address,
            func.count(Transaction.tx_hash).label('tx_count'),
            func.sum(Transaction.usd_value).label('total_volume'),
            func.avg(Transaction.usd_value).label('avg_volume'),
            func.min(Transaction.usd_value).label('min_volume'),
            func.max(Transaction.usd_value).label('max_volume'),
            func.min(Transaction.timestamp).label('first_tx'),
            func.max(Transaction.timestamp).label('last_tx'),
            func.array_agg(Transaction.tx_hash).label('tx_hashes')
        ).filter(
            Transaction.timestamp >= start_date,
            Transaction.timestamp <= end_date,
            or_(
                # Both addresses in our wallet list
                and_(
                    Transaction.from_address.in_(wallet_addresses),
                    Transaction.to_address.in_(wallet_addresses)
                )
            )
        ).group_by(
            Transaction.from_address,
            Transaction.to_address
        ).having(
            func.coalesce(func.sum(Transaction.usd_value), 0) >= min_flow_size
        ).limit(max_links).all()
        
        logger.info(f"   ✅ Found {len(link_data)} potential links")
        stats["links_found"] = len(link_data)
        
        if len(link_data) == 0:
            logger.info("   ℹ️ No links above threshold")
            return stats
        
        # ================================================================
        # STEP 3: SAVE/UPDATE LINKS
        # ================================================================
        
        logger.info("💾 Step 3: Saving links to database...")
        
        for idx, link in enumerate(link_data):
            try:
                from_addr = link.from_address.lower()
                to_addr = link.to_address.lower()
                
                # Get wallet metadata
                from_meta = wallet_lookup.get(from_addr, {})
                to_meta = wallet_lookup.get(to_addr, {})
                
                # Calculate scores
                total_volume = float(link.total_volume or 0)
                tx_count = int(link.tx_count or 0)
                avg_volume = float(link.avg_volume or 0)
                
                # Link strength (0-100)
                volume_score = min(total_volume / 10_000_000, 1.0) * 40  # Max 40 points
                frequency_score = min(tx_count / 100, 1.0) * 30  # Max 30 points
                
                # Recency score
                days_since_last = (datetime.now() - link.last_tx).days if link.last_tx else 999
                recency_score = max(0, (30 - days_since_last) / 30) * 30  # Max 30 points
                
                link_strength = volume_score + frequency_score + recency_score
                
                # OTC confidence
                otc_confidence = 0.0
                if avg_volume > 100000:
                    otc_confidence += 40
                if tx_count > 10:
                    otc_confidence += 30
                if from_meta.get('type') == 'otc_desk' or to_meta.get('type') == 'otc_desk':
                    otc_confidence += 30
                
                is_suspected_otc = otc_confidence >= 60
                
                # Detect patterns
                patterns = []
                if avg_volume > 1_000_000:
                    patterns.append('large_transfers')
                if tx_count > 50:
                    patterns.append('high_frequency')
                if link.max_volume and link.max_volume > avg_volume * 5:
                    patterns.append('size_outliers')
                
                # Sample hashes (max 10)
                sample_hashes = (link.tx_hashes or [])[:10]
                
                # Flow type
                flow_type = 'outbound'  # from → to
                is_bidirectional = False  # TODO: Check reverse direction
                
                # ============================================================
                # CHECK IF LINK EXISTS
                # ============================================================
                
                existing_link = db.query(WalletLink).filter(
                    WalletLink.from_address == from_addr,
                    WalletLink.to_address == to_addr,
                    WalletLink.analysis_start == start_date
                ).first()
                
                if existing_link and not force_refresh:
                    stats["links_skipped"] += 1
                    continue
                
                if existing_link:
                    # UPDATE
                    existing_link.transaction_count = tx_count
                    existing_link.total_volume_usd = total_volume
                    existing_link.avg_transaction_usd = avg_volume
                    existing_link.min_transaction_usd = float(link.min_volume) if link.min_volume else None
                    existing_link.max_transaction_usd = float(link.max_volume) if link.max_volume else None
                    existing_link.first_transaction = link.first_tx
                    existing_link.last_transaction = link.last_tx
                    existing_link.link_strength = link_strength
                    existing_link.is_suspected_otc = is_suspected_otc
                    existing_link.otc_confidence = otc_confidence
                    existing_link.volume_score = volume_score
                    existing_link.frequency_score = frequency_score
                    existing_link.recency_score = recency_score
                    existing_link.detected_patterns = patterns
                    existing_link.sample_tx_hashes = sample_hashes
                    existing_link.updated_at = datetime.now()
                    existing_link.last_calculated = datetime.now()
                    
                    stats["links_updated"] += 1
                    
                    if stats["links_updated"] <= 5:
                        logger.info(
                            f"      🔄 Link {idx+1}: {from_addr[:10]}...→{to_addr[:10]}... "
                            f"(${total_volume:,.0f}, {tx_count} TXs) - UPDATED"
                        )
                    
                else:
                    # INSERT
                    new_link = WalletLink(
                        from_address=from_addr,
                        to_address=to_addr,
                        from_wallet_type=from_meta.get('type'),
                        to_wallet_type=to_meta.get('type'),
                        from_wallet_label=from_meta.get('label'),
                        to_wallet_label=to_meta.get('label'),
                        transaction_count=tx_count,
                        total_volume_usd=total_volume,
                        avg_transaction_usd=avg_volume,
                        min_transaction_usd=float(link.min_volume) if link.min_volume else None,
                        max_transaction_usd=float(link.max_volume) if link.max_volume else None,
                        first_transaction=link.first_tx,
                        last_transaction=link.last_tx,
                        analysis_start=start_date,
                        analysis_end=end_date,
                        link_strength=link_strength,
                        is_suspected_otc=is_suspected_otc,
                        otc_confidence=otc_confidence,
                        volume_score=volume_score,
                        frequency_score=frequency_score,
                        recency_score=recency_score,
                        detected_patterns=patterns,
                        flow_type=flow_type,
                        is_bidirectional=is_bidirectional,
                        data_source='transactions',
                        data_quality='high',
                        sample_tx_hashes=sample_hashes,
                        is_active=True,
                        created_at=datetime.now(),
                        last_calculated=datetime.now()
                    )
                    
                    db.add(new_link)
                    stats["links_saved"] += 1
                    
                    if stats["links_saved"] <= 5:
                        logger.info(
                            f"      ➕ Link {idx+1}: {from_addr[:10]}...→{to_addr[:10]}... "
                            f"(${total_volume:,.0f}, {tx_count} TXs, strength:{link_strength:.1f})"
                        )
                
                # Commit in batches
                if (stats["links_saved"] + stats["links_updated"]) % 50 == 0:
                    db.commit()
                    logger.info(
                        f"      ✅ Committed {stats['links_saved']} new + "
                        f"{stats['links_updated']} updated links..."
                    )
                
            except Exception as link_error:
                logger.error(f"      ⚠️ Error processing link {idx+1}: {link_error}")
                stats["errors"] += 1
                continue
        
        # Final commit
        db.commit()
        
        stats["end_time"] = datetime.now()
        stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
        
        logger.info("="*80)
        logger.info("✅ WALLET LINKS SAVED")
        logger.info("="*80)
        logger.info(f"Wallets analyzed: {stats['wallets_analyzed']}")
        logger.info(f"Links found: {stats['links_found']}")
        logger.info(f"Links saved: {stats['links_saved']}")
        logger.info(f"Links updated: {stats['links_updated']}")
        logger.info(f"Links skipped: {stats['links_skipped']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"Duration: {stats['duration_seconds']:.1f}s")
        logger.info("="*80)
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error saving wallet links: {e}", exc_info=True)
        db.rollback()
        stats["errors"] += 1
        return stats
        
async def sync_wallet_transactions_to_db(
    db: Session,
    wallet_address: str,
    max_transactions: int = 100,
    force_refresh: bool = False,
    skip_enrichment: bool = False
) -> Dict[str, Any]:
    """
    Synchronisiert Transaktionen eines Wallets in die Datenbank.
    
    ✅ FIXED VERSION v3.1 FINAL:
    - REMOVED 'needs_enrichment' field (doesn't exist in DB)
    - Speichert ALLE neuen TXs (mit oder ohne USD value)
    - Updates existierende TXs wenn neuer USD value verfügbar
    - Robustes Logging für Debugging
    - NoneType-safe überall
    
    Args:
        db: Database session
        wallet_address: Wallet address to sync
        max_transactions: Max number of transactions to fetch
        force_refresh: Force refresh even if recent data exists
        skip_enrichment: Skip USD enrichment step
        
    Returns:
        Dict with sync statistics
    """
    from app.core.otc_analysis.models.transaction import Transaction
    from datetime import datetime, timedelta
    from sqlalchemy import and_, or_
    
    logger.info(f"🔄 Syncing transactions for {wallet_address[:10]}...")
    
    stats = {
        "address": wallet_address,
        "existing_count": 0,
        "fetched_count": 0,
        "saved_count": 0,
        "updated_count": 0,
        "skipped_count": 0,
        "enrichment_failed": 0,
        "source": "unknown",
        "errors": []
    }
    
    try:
        # ====================================================================
        # STEP 1: CHECK EXISTING TRANSACTIONS IN DB (Optional Cache)
        # ====================================================================
        
        if not force_refresh:
            cutoff_time = datetime.now() - timedelta(hours=6)
            
            existing_count = db.query(Transaction).filter(
                or_(
                    Transaction.from_address == wallet_address.lower(),
                    Transaction.to_address == wallet_address.lower()
                ),
                Transaction.created_at >= cutoff_time
            ).count()
            
            stats["existing_count"] = existing_count
            
            if existing_count >= max_transactions:
                logger.info(
                    f"   ✅ Found {existing_count} cached transactions (< 6h old), "
                    f"using cache"
                )
                stats["source"] = "cache"
                return stats
            elif existing_count > 0:
                logger.info(
                    f"   ℹ️ Found {existing_count} cached transactions, "
                    f"but fetching more from blockchain..."
                )
        else:
            logger.info(f"   🔄 Force refresh enabled, fetching from blockchain...")
        
        # ====================================================================
        # STEP 2: FETCH FROM BLOCKCHAIN API
        # ====================================================================
        
        logger.info(f"   📡 Fetching transactions via TransactionExtractor...")
        
        transactions = transaction_extractor.extract_wallet_transactions(
            wallet_address,
            include_internal=True,
            include_tokens=True
        )
        
        if not transactions:
            logger.info(f"   ⚠️ No transactions returned from API")
            stats["source"] = "blockchain"
            return stats
        
        stats["fetched_count"] = len(transactions)
        logger.info(f"   ✅ Fetched {len(transactions)} transactions from blockchain")
        
        # Limit to max_transactions
        if len(transactions) > max_transactions:
            logger.info(f"   📊 Limiting to {max_transactions} most recent transactions")
            transactions = sorted(
                transactions,
                key=lambda x: x.get('timestamp', datetime.min),
                reverse=True
            )[:max_transactions]
        
        # ====================================================================
        # STEP 3: OPTIONAL USD ENRICHMENT
        # ====================================================================
        
        enriched_transactions = transactions
        
        if not skip_enrichment:
            logger.info(f"   💰 Enriching {len(transactions)} transactions with USD values...")
            
            try:
                enriched_transactions = transaction_extractor.enrich_with_usd_value(
                    transactions,
                    price_oracle,
                    max_transactions=len(transactions)
                )
                
                # Count successfully enriched
                enriched_count = 0
                for tx in enriched_transactions:
                    val = tx.get('value_usd') or tx.get('valueUSD') or tx.get('usd_value')
                    if val is not None and val > 0:
                        enriched_count += 1
                
                failed_count = len(enriched_transactions) - enriched_count
                stats["enrichment_failed"] = failed_count
                
                logger.info(
                    f"   💵 Enrichment result: "
                    f"{enriched_count} successful, "
                    f"{failed_count} failed/skipped"
                )
                
            except Exception as enrich_error:
                logger.warning(f"   ⚠️ Enrichment error: {enrich_error}")
                logger.info(f"   ℹ️ Continuing with raw transaction data...")
                enriched_transactions = transactions
                stats["enrichment_failed"] = len(transactions)
        else:
            logger.info(f"   ⏭️ Skipping USD enrichment (skip_enrichment=True)")
            stats["enrichment_failed"] = len(transactions)
        
        # ====================================================================
        # STEP 4: SAVE TO DATABASE (IMPROVED LOGIC)
        # ====================================================================
        
        logger.info(f"   💾 Saving transactions to database...")
        
        insert_count = 0
        update_count = 0
        skip_count = 0
        error_count = 0
        
        for idx, tx in enumerate(enriched_transactions):
            try:
                # ============================================================
                # PARSE TRANSACTION DATA
                # ============================================================
                
                # Parse timestamp
                tx_timestamp = tx.get('timestamp')
                if isinstance(tx_timestamp, str):
                    try:
                        tx_timestamp = datetime.fromisoformat(tx_timestamp.replace('Z', '+00:00'))
                    except:
                        tx_timestamp = datetime.now()
                elif isinstance(tx_timestamp, int):
                    tx_timestamp = datetime.fromtimestamp(tx_timestamp)
                elif not isinstance(tx_timestamp, datetime):
                    tx_timestamp = datetime.now()
                
                # Extract USD value (check multiple keys)
                usd_value = None
                for key in ['value_usd', 'valueUSD', 'usd_value']:
                    val = tx.get(key)
                    if val is not None:
                        try:
                            usd_value = float(val)
                            if usd_value > 0:
                                break
                        except (ValueError, TypeError):
                            continue
                
                # Get ETH value
                value_wei = str(tx.get('value', '0'))
                try:
                    raw_value = float(tx.get('value', 0))
                    value_decimal = raw_value / 1e18 if tx.get('value') else 0

                    # ✅ Skip spam tokens with absurd values (>10^27)
                    if raw_value > 1e27:
                        logger.debug(f"      🗑️  Skipping spam token TX {tx.get('hash', '')[:10]}: value={raw_value:.2e}")
                        skip_count += 1
                        continue

                    # ✅ Cap value_decimal to prevent database overflow (NUMERIC(36,18) max = 10^18)
                    MAX_DECIMAL = 999999999999999999.0  # 10^18 - 1
                    if abs(value_decimal) > MAX_DECIMAL:
                        logger.debug(f"      ⚠️ Capping oversized value_decimal: {value_decimal:.2e} -> {MAX_DECIMAL:.2e}")
                        value_decimal = MAX_DECIMAL if value_decimal > 0 else -MAX_DECIMAL
                except:
                    value_decimal = 0
                
                # Calculate OTC score
                otc_score = 0.0
                if usd_value and usd_value > 0:
                    if usd_value > 100000:
                        otc_score = min(usd_value / 1000000, 1.0)
                
                # Get transaction hash
                tx_hash = tx.get('hash') or tx.get('tx_hash')
                if not tx_hash:
                    logger.debug(f"      ⚠️ TX {idx+1}: Missing tx_hash, skipping")
                    skip_count += 1
                    continue
                
                # ============================================================
                # CHECK IF TX EXISTS IN DB
                # ============================================================
                
                existing_tx = db.query(Transaction).filter(
                    Transaction.tx_hash == tx_hash
                ).first()
                
                if existing_tx:
                    # ========================================================
                    # UPDATE EXISTING TRANSACTION
                    # ========================================================
                    
                    should_update = False
                    
                    # Case 1: We have new USD value, existing doesn't
                    if usd_value and usd_value > 0:
                        if not existing_tx.usd_value or existing_tx.usd_value == 0:
                            existing_tx.usd_value = usd_value
                            existing_tx.otc_score = otc_score
                            existing_tx.is_suspected_otc = otc_score > 0.7
                            existing_tx.updated_at = datetime.now()
                            should_update = True
                    
                    if should_update:
                        update_count += 1
                        
                        if update_count <= 5:
                            logger.info(
                                f"      🔄 TX {idx+1} ({tx_hash[:10]}...): "
                                f"Updated with USD value"
                            )
                        
                        # Commit in batches
                        if update_count % 20 == 0:
                            db.commit()
                            logger.info(f"      ✅ Committed {update_count} updates...")
                    else:
                        skip_count += 1
                    
                    continue
                
                # ============================================================
                # INSERT NEW TRANSACTION (✅ FIXED: NO needs_enrichment!)
                # ============================================================
                
                new_tx = Transaction(
                    tx_hash=tx_hash,
                    block_number=int(tx.get('blockNumber', 0) or tx.get('block_number', 0) or 0),
                    timestamp=tx_timestamp,
                    from_address=(tx.get('from_address') or tx.get('from', '')).lower(),
                    to_address=(tx.get('to_address') or tx.get('to', '')).lower(),
                    token_address=tx.get('tokenAddress') or tx.get('token_address'),
                    value=value_decimal,  # ETH-denominated (not raw wei) to fit Numeric(36,18)
                    value_decimal=value_decimal,
                    usd_value=usd_value,
                    gas_used=int(tx.get('gasUsed', 0) or tx.get('gas_used', 0) or 0),
                    gas_price=int(tx.get('gasPrice', 0) or tx.get('gas_price', 0) or 0),
                    is_contract_interaction=tx.get('isContractInteraction', False),
                    method_id=tx.get('methodId') or tx.get('method_id'),
                    otc_score=otc_score,
                    is_suspected_otc=otc_score > 0.7,
                    chain='ethereum',
                    chain_id=1,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                
                # Use savepoint so one failed TX doesn't rollback the whole batch
                try:
                    db.begin_nested()  # SAVEPOINT
                    db.add(new_tx)
                    db.flush()
                except Exception as flush_error:
                    # Savepoint rolled back automatically, session still usable
                    error_count += 1
                    if error_count <= 3:
                        logger.warning(f"      ⚠️ TX {idx+1} ({tx_hash[:10]}...): {str(flush_error)[:80]}")
                    continue

                insert_count += 1

                if insert_count <= 5:
                    usd_display = f"${usd_value:,.2f}" if usd_value else "$0.00"
                    logger.info(
                        f"      ➕ TX {idx+1} ({tx_hash[:10]}...): "
                        f"Inserted ({usd_display})"
                    )

                # Commit in batches
                if insert_count % 50 == 0:
                    db.commit()
                    logger.info(f"      ✅ Committed {insert_count} inserts...")

            except Exception as tx_error:
                error_count += 1
                error_msg = f"TX {idx+1}: {str(tx_error)[:100]}"
                stats["errors"].append(error_msg)

                if error_count <= 3:
                    logger.warning(f"      ⚠️ Error processing {error_msg}")

                db.rollback()

                continue
        
        # ====================================================================
        # FINAL COMMIT & SUMMARY
        # ====================================================================
        
        try:
            db.commit()
        except Exception as commit_error:
            logger.error(f"   ❌ Final commit failed: {commit_error}")
            db.rollback()
            lost_count = insert_count % 50
            stats["errors"].append(f"Final commit failed, ~{lost_count} transactions lost: {str(commit_error)[:200]}")
            logger.warning(f"   ⚠️ ~{lost_count} transactions lost from final batch (previous batches committed OK)")

        stats["saved_count"] = insert_count
        stats["updated_count"] = update_count
        stats["skipped_count"] = skip_count
        stats["source"] = "blockchain"
        
        logger.info(
            f"   ✅ Transaction sync complete: "
            f"Saved {insert_count}, "
            f"Updated {update_count}, "
            f"Skipped {skip_count}"
        )
        
        if error_count > 0:
            logger.warning(f"   ⚠️ {error_count} errors occurred during sync")
        
        # ====================================================================
        # VERIFICATION: Count actual DB entries
        # ====================================================================
        
        if insert_count > 0 or update_count > 0:
            total_in_db = db.query(Transaction).filter(
                or_(
                    Transaction.from_address == wallet_address.lower(),
                    Transaction.to_address == wallet_address.lower()
                )
            ).count()
            
            logger.info(f"   📊 Total transactions in DB for this wallet: {total_in_db}")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error syncing transactions: {e}", exc_info=True)
        db.rollback()
        stats["errors"].append(f"Fatal error: {str(e)}")
        return stats
        
async def enrich_missing_usd_values(
    db: Session,
    batch_size: int = 100,
    delay_between_calls: float = 1.2,
    max_batches: int = 10
) -> Dict[str, Any]:
    """
    Enriched Transaktionen die usd_value = NULL oder 0 haben.
    
    ✨ SMART ENRICHMENT:
    - Priorisiert große Transaktionen (> 0.1 ETH)
    - Rate Limit Protection mit Delays
    - Batch Processing mit Commits
    - Stoppt bei zu vielen Fehlern
    
    Args:
        db: Database session
        batch_size: Anzahl TXs pro Batch (default: 100)
        delay_between_calls: Verzögerung zwischen API-Calls (default: 1.2s)
        max_batches: Max Anzahl Batches (default: 10)
    
    Returns:
        Dict with stats
    """
    from app.core.otc_analysis.models.transaction import Transaction
    from datetime import datetime
    import asyncio
    
    logger.info("="*70)
    logger.info("💰 ENRICHING MISSING USD VALUES")
    logger.info("="*70)
    
    stats = {
        "total_checked": 0,
        "enriched": 0,
        "failed": 0,
        "skipped": 0,
        "batches_processed": 0,
        "rate_limit_hits": 0,
        "start_time": datetime.now()
    }
    
    try:
        for batch_num in range(max_batches):
            logger.info(f"\n📦 Processing batch {batch_num + 1}/{max_batches}...")
            
            # ✨ OPTIMIERTE QUERY: Priorisiere große TXs
            transactions = db.query(Transaction).filter(
                (Transaction.usd_value == None) | (Transaction.usd_value == 0),
                Transaction.value_decimal > 0.01,  # Nur > 0.01 ETH
            ).order_by(
                Transaction.value_decimal.desc()  # Größte zuerst
            ).limit(batch_size).all()
            
            if not transactions:
                logger.info("   ✅ No more transactions to enrich")
                break
            
            logger.info(f"   Found {len(transactions)} transactions needing enrichment")
            stats["total_checked"] += len(transactions)
            
            for tx in transactions:
                try:
                    # Determine token
                    if tx.token_address and tx.token_address.lower() != '0x' * 20:
                        token_symbol = tx.token_address  # Will be resolved by price oracle
                    else:
                        token_symbol = "ETH"
                    
                    # ✨ Get price with retry logic
                    price = None
                    retry_count = 0
                    max_retries = 3
                    
                    while retry_count < max_retries and not price:
                        try:
                            price = await price_oracle.get_token_price_at_time(
                                token_symbol,
                                tx.timestamp
                            )
                            
                            if price and price > 0:
                                break
                            
                            # Delay between retries
                            if retry_count < max_retries - 1:
                                await asyncio.sleep(delay_between_calls * (retry_count + 1))
                            
                        except Exception as price_error:
                            error_msg = str(price_error).lower()
                            
                            if 'rate' in error_msg or 'limit' in error_msg or '429' in error_msg:
                                stats["rate_limit_hits"] += 1
                                logger.warning(f"      ⏱️ Rate limit hit, waiting {delay_between_calls * 2}s...")
                                await asyncio.sleep(delay_between_calls * 2)
                            
                            retry_count += 1
                    
                    if price and price > 0:
                        # Calculate USD value
                        usd_value = tx.value_decimal * price
                        
                        # Update transaction
                        tx.usd_value = usd_value
                        tx.otc_score = min(usd_value / 1000000, 1.0) if usd_value > 100000 else 0.0
                        tx.is_suspected_otc = tx.otc_score > 0.7
                        tx.updated_at = datetime.now()

                        stats["enriched"] += 1
                        
                        # Commit in small batches
                        if stats["enriched"] % 10 == 0:
                            db.commit()
                            logger.info(
                                f"      ✅ Enriched {stats['enriched']} transactions "
                                f"(latest: ${usd_value:,.2f})"
                            )
                        
                        # Delay between successful calls
                        await asyncio.sleep(delay_between_calls)
                        
                    else:
                        tx.updated_at = datetime.now()
                        stats["failed"] += 1
                    
                except Exception as tx_error:
                    logger.debug(f"      ⚠️ Error enriching TX {tx.tx_hash[:10]}: {tx_error}")
                    stats["failed"] += 1
                    
                    # Stop batch if too many consecutive failures
                    if stats["failed"] > 20 and stats["enriched"] == 0:
                        logger.error("   ❌ Too many failures, stopping batch")
                        break
                    
                    continue
            
            # Commit batch
            db.commit()
            stats["batches_processed"] += 1
            
            logger.info(
                f"   ✅ Batch {batch_num + 1} complete: "
                f"{stats['enriched']} enriched, "
                f"{stats['failed']} failed"
            )
            
            # Delay between batches
            if batch_num < max_batches - 1:
                await asyncio.sleep(delay_between_calls * 2)
        
        stats["end_time"] = datetime.now()
        stats["duration_seconds"] = (
            stats["end_time"] - stats["start_time"]
        ).total_seconds()
        
        logger.info("\n" + "="*70)
        logger.info("✅ ENRICHMENT COMPLETE")
        logger.info("="*70)
        logger.info(f"Total checked: {stats['total_checked']}")
        logger.info(f"Successfully enriched: {stats['enriched']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info(f"Rate limit hits: {stats['rate_limit_hits']}")
        logger.info(f"Batches processed: {stats['batches_processed']}")
        logger.info(f"Duration: {stats['duration_seconds']:.1f}s")
        logger.info("="*70)
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Enrichment failed: {e}", exc_info=True)
        db.rollback()
        return stats

async def sync_all_wallets_transactions(
    db: Session,
    max_wallets: int = 10,
    max_transactions_per_wallet: int = 100
) -> Dict[str, Any]:
    """
    Synchronisiert Transaktionen für alle aktiven Wallets.
    
    Holt Transaktionen für alle OTC Wallets in der DB und speichert sie.
    Dadurch wird die Heatmap mit echten Daten gefüllt.
    
    Args:
        db: Database session
        max_wallets: Max number of wallets to sync (default: 10)
        max_transactions_per_wallet: Max TXs per wallet (default: 100)
    
    Returns:
        Dict with overall stats
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from datetime import datetime, timedelta
    
    logger.info("="*70)
    logger.info("🔄 SYNCING TRANSACTIONS FOR ALL WALLETS")
    logger.info("="*70)
    
    overall_stats = {
        "wallets_processed": 0,
        "total_fetched": 0,
        "total_saved": 0,
        "total_skipped": 0,
        "errors": 0,
        "start_time": datetime.now()
    }
    
    try:
        # Get active wallets
        wallets = db.query(OTCWallet).filter(
            OTCWallet.is_active == True,
            OTCWallet.confidence_score >= 50.0
        ).order_by(OTCWallet.total_volume.desc()).limit(max_wallets).all()
        
        logger.info(f"📊 Found {len(wallets)} active wallets to sync")
        
        for wallet in wallets:
            try:
                logger.info(f"\n🔄 Processing wallet {wallet.address[:10]}... ({wallet.label})")
                
                stats = await sync_wallet_transactions_to_db(
                    db=db,
                    wallet_address=wallet.address,
                    max_transactions=max_transactions_per_wallet,
                    force_refresh=False
                )
                
                overall_stats["wallets_processed"] += 1
                overall_stats["total_fetched"] += stats["fetched_count"]
                overall_stats["total_saved"] += stats["saved_count"]
                overall_stats["total_skipped"] += stats["skipped_count"]
                
                # Small delay to avoid rate limits
                import asyncio
                await asyncio.sleep(0.5)
                
            except Exception as wallet_error:
                logger.error(f"❌ Error processing wallet {wallet.address[:10]}: {wallet_error}")
                overall_stats["errors"] += 1
                continue
        
        overall_stats["end_time"] = datetime.now()
        overall_stats["duration_seconds"] = (
            overall_stats["end_time"] - overall_stats["start_time"]
        ).total_seconds()
        
        logger.info("\n" + "="*70)
        logger.info("✅ TRANSACTION SYNC COMPLETE")
        logger.info("="*70)
        logger.info(f"Wallets processed: {overall_stats['wallets_processed']}")
        logger.info(f"Transactions fetched: {overall_stats['total_fetched']}")
        logger.info(f"Transactions saved: {overall_stats['total_saved']}")
        logger.info(f"Transactions skipped: {overall_stats['total_skipped']}")
        logger.info(f"Errors: {overall_stats['errors']}")
        logger.info(f"Duration: {overall_stats['duration_seconds']:.1f}s")
        logger.info("="*70)
        
        return overall_stats
        
    except Exception as e:
        logger.error(f"❌ Overall sync failed: {e}", exc_info=True)
        return overall_stats



# ============================================================================
# EXPORT ALL
# ============================================================================

__all__ = [
    # Dependencies
    "get_db",
    "get_current_user",
    "get_otc_registry",
    "get_cache_manager",
    "get_labeling_service",
    "get_transaction_extractor",
    "get_price_oracle",
    "get_wallet_profiler",
    "get_otc_detector",
    "get_link_builder",
    "get_balance_fetcher",      # ✨ NEW
    "get_activity_analyzer",    # ✨ NEW
    "get_balance_scorer",       # ✨ NEW
    
# Services
    "cache_manager",
    "otc_registry",
    "labeling_service",
    "otc_detector",
    "wallet_profiler",
    "wallet_stats_api",
    "api_error_tracker",
    "api_health_monitor",
    "flow_tracer",
    "node_provider",
    "etherscan",
    "price_oracle",
    "transaction_extractor",
    "block_scanner",
    "statistics_service",
    "graph_builder",
    "link_builder",
    "balance_fetcher",      # ✨ NEW
    "activity_analyzer",    # ✨ NEW
    "balance_scorer",       # ✨ NEW
    "discover_high_volume_from_transactions",
    "save_wallet_links_to_db",

    # ✨ NEW: Transaction Sync
    "sync_wallet_transactions_to_db",
    "sync_all_wallets_transactions",    
    
    # Helpers
    "ensure_registry_wallets_in_db",
    "discover_new_otc_desks",
    "discover_from_last_5_transactions",
]
