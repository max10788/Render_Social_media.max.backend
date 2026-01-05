"""
Shared Dependencies & Services - ALWAYS USE QUICK STATS FIRST
===============================================================

✨ IMPROVED VERSION - Quick Stats API als PRIMARY Strategy:
- IMMER zuerst Quick Stats API versuchen (unabhängig von TX Count)
- Nur bei Fehler/Unavailable → Fallback auf Transaction Processing
- 15x schneller für ALLE Registry Wallets

Version: 6.0 - Always Quick Stats First
Date: 2025-01-04
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
from app.core.otc_analysis.blockchain.etherscan import EtherscanAPI

# Data Sources
from app.core.otc_analysis.data_sources.price_oracle import PriceOracle
from app.core.otc_analysis.data_sources.otc_desks import OTCDeskRegistry
from app.core.otc_analysis.data_sources.wallet_labels import WalletLabelingService
from app.core.otc_analysis.discovery.discovery_scorer import DiscoveryScorer

# Analysis Services
from app.core.otc_analysis.analysis.statistics_service import StatisticsService
from app.core.otc_analysis.analysis.graph_builder import GraphBuilderService
from app.core.otc_analysis.analysis.network_graph import NetworkAnalysisService

# Database Models
from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
from app.core.otc_analysis.models.watchlist import WatchlistItem as OTCWatchlist
from app.core.otc_analysis.models.alert import Alert as OTCAlert

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
price_oracle = PriceOracle(cache_manager, etherscan)

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

# Other services
otc_registry = OTCDeskRegistry(cache_manager)
labeling_service = WalletLabelingService(cache_manager)
otc_detector = OTCDetector(cache_manager, otc_registry, labeling_service)
flow_tracer = FlowTracer()
block_scanner = BlockScanner(node_provider, chain_id=1)

# Analysis services
statistics_service = StatisticsService(cache_manager)
graph_builder = GraphBuilderService(cache_manager)

logger.info("✅ All OTC services initialized successfully")
logger.info(f"   • Strategy: ALWAYS Quick Stats First (15x faster)")
logger.info(f"   • WalletProfiler: with PriceOracle + WalletStatsAPI")
logger.info(f"   • WalletStatsAPI: Multi-tier fallback")
logger.info(f"   • TransactionExtractor: Moralis enabled")


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
    """🔍 Analyze counterparties from last N transactions."""
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
        
        # Initialize analyzer (✨ WITH wallet_stats_api for Quick Stats)
        analyzer = SimpleLastTxAnalyzer(
            db=db,
            transaction_extractor=transaction_extractor,
            wallet_profiler=wallet_profiler,
            price_oracle=price_oracle,
            wallet_stats_api=wallet_stats_api  # ✨ NEW: Enable Quick Stats in Discovery
        )
        
        # Initialize scorer
        discovery_scorer = DiscoveryScorer(known_addresses)
        
        # Get transactions
        transactions = transaction_extractor.extract_wallet_transactions(
            otc_address,
            include_internal=True,
            include_tokens=True
        )
        
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
            
            # Calculate discovery score
            discovery_result = discovery_scorer.score_discovered_wallet(
                address=address,
                transactions=cp_transactions,
                counterparty_data=cp_data,
                profile=analysis.get('profile', {})
            )
            
            # Combine scores
            base_confidence = analysis['confidence']
            discovery_score = discovery_result['score']
            final_confidence = (base_confidence * 0.4) + (discovery_score * 0.6)
            
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
            
            # Save if confidence >= 50%
            if final_confidence >= 50.0:
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
                    f"(threshold: 50%, confidence: {final_confidence:.1f}%)"
                )
            else:
                logger.info(f"      ⚠️ Below threshold (50%)")
        
        logger.info(
            f"✅ Discovery complete: {len(discovered)} new wallets found "
            f"(filtered {filtered_count} known entities)"
        )
        
        return discovered
        
    except Exception as e:
        logger.error(f"❌ Discovery failed: {e}", exc_info=True)
        db.rollback()
        return []


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
    
    # Helpers
    "ensure_registry_wallets_in_db",
    "discover_new_otc_desks",
    "discover_from_last_5_transactions",
]
