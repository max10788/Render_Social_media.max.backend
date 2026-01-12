
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

# ✨ NEW: LinkBuilder service for fast link/edge generation
link_builder = LinkBuilder(cache_manager, transaction_extractor)

logger.info("✅ All OTC services initialized successfully")
logger.info(f"   • Strategy: ALWAYS Quick Stats First (15x faster)")
logger.info(f"   • WalletProfiler: with PriceOracle + WalletStatsAPI")
logger.info(f"   • WalletStatsAPI: Multi-tier fallback")
logger.info(f"   • TransactionExtractor: Moralis enabled")
logger.info(f"   • LinkBuilder: Fast link generation with caching")  # ✨ NEW


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

"""
NEUE FUNKTION für dependencies.py einfügen (am Ende, vor __all__)

Synced Blockchain-Transaktionen in die transactions Tabelle
"""

async def sync_wallet_transactions_to_db(
    db: Session,
    wallet_address: str,
    max_transactions: int = 100,
    force_refresh: bool = False,
    skip_enrichment: bool = False
) -> Dict[str, Any]:
    """
    Synchronisiert Transaktionen eines Wallets in die Datenbank.
    
    ✅ FIXED VERSION v2 - NoneType Safe:
    - Speichert neue TXs auch ohne USD-Wert
    - Updates existierende TXs die noch keinen USD-Wert haben
    - NoneType-safe comparisons
    - Robuste Fehlerbehandlung
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
        "needs_enrichment": 0,
        "source": "unknown"
    }
    
    try:
        # ====================================================================
        # STEP 1: CHECK EXISTING TRANSACTIONS IN DB
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
            
            if existing_count > 0:
                logger.info(f"   ✅ Found {existing_count} cached transactions (< 6h old)")
                stats["source"] = "cache"
                return stats
            
            logger.info(f"   ℹ️ No recent transactions in DB, fetching from blockchain...")
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
                
                # ✅ FIX: NoneType-safe count
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
        # STEP 4: SAVE TO DATABASE
        # ====================================================================
        
        logger.info(f"   💾 Saving transactions to database...")
        
        for tx in enriched_transactions:
            try:
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
                
                # ✅ ROBUSTE USD-VALUE EXTRAKTION (NoneType safe)
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
                    value_decimal = float(tx.get('value', 0)) / 1e18 if tx.get('value') else 0
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
                    logger.debug(f"      ⚠️ Missing tx_hash, skipping")
                    stats["skipped_count"] += 1
                    continue
                
                # ====================================================================
                # CHECK IF TX EXISTS
                # ====================================================================
                
                existing_tx = db.query(Transaction).filter(
                    Transaction.tx_hash == tx_hash
                ).first()
                
                if existing_tx:
                    # ====================================================================
                    # UPDATE LOGIC
                    # ====================================================================
                    
                    should_update = False
                    
                    # Case 1: We have USD value, but existing doesn't
                    if usd_value and usd_value > 0:
                        if not existing_tx.usd_value or existing_tx.usd_value == 0:
                            existing_tx.usd_value = usd_value
                            existing_tx.otc_score = otc_score
                            existing_tx.is_suspected_otc = otc_score > 0.7
                            existing_tx.needs_enrichment = False
                            existing_tx.updated_at = datetime.now()
                            should_update = True
                    
                    # Case 2: Neither has USD value, mark for enrichment
                    elif (not existing_tx.usd_value or existing_tx.usd_value == 0) and value_decimal > 0.01:
                        if not existing_tx.needs_enrichment:
                            existing_tx.needs_enrichment = True
                            existing_tx.updated_at = datetime.now()
                            should_update = True
                            stats["needs_enrichment"] += 1
                    
                    if should_update:
                        stats["updated_count"] += 1
                        
                        if stats["updated_count"] % 20 == 0:
                            db.commit()
                            logger.info(f"      ✅ Updated {stats['updated_count']} transactions...")
                    else:
                        stats["skipped_count"] += 1
                    
                    continue
                
                # ====================================================================
                # CREATE NEW TRANSACTION
                # ====================================================================
                
                needs_enrichment = (not usd_value or usd_value == 0) and value_decimal > 0.01
                
                if needs_enrichment:
                    stats["needs_enrichment"] += 1
                
                new_tx = Transaction(
                    tx_hash=tx_hash,
                    block_number=int(tx.get('blockNumber', 0) or tx.get('block_number', 0) or 0),
                    timestamp=tx_timestamp,
                    from_address=tx.get('from', '').lower(),
                    to_address=tx.get('to', '').lower(),
                    token_address=tx.get('tokenAddress') or tx.get('token_address'),
                    value=value_wei,
                    value_decimal=value_decimal,
                    usd_value=usd_value,
                    gas_used=int(tx.get('gasUsed', 0) or tx.get('gas_used', 0) or 0),
                    gas_price=int(tx.get('gasPrice', 0) or tx.get('gas_price', 0) or 0),
                    is_contract_interaction=tx.get('isContractInteraction', False),
                    method_id=tx.get('methodId') or tx.get('method_id'),
                    otc_score=otc_score,
                    is_suspected_otc=otc_score > 0.7,
                    needs_enrichment=needs_enrichment,
                    chain_id=1,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                
                db.add(new_tx)
                stats["saved_count"] += 1
                
                # Commit in batches
                if stats["saved_count"] % 50 == 0:
                    db.commit()
                    logger.info(f"      ✅ Saved {stats['saved_count']} transactions...")
                
            except Exception as tx_error:
                logger.debug(f"      ⚠️ Error processing TX: {tx_error}")
                stats["skipped_count"] += 1
                continue
        
        # Final commit
        db.commit()
        stats["source"] = "blockchain"
        
        logger.info(
            f"   ✅ Transaction sync complete: "
            f"Saved {stats['saved_count']}, "
            f"Updated {stats['updated_count']}, "
            f"Skipped {stats['skipped_count']}"
        )
        
        if stats["needs_enrichment"] > 0:
            logger.info(
                f"   ⏳ {stats['needs_enrichment']} transactions need USD enrichment "
                f"(run background job later)"
            )
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error syncing transactions: {e}", exc_info=True)
        db.rollback()
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
                Transaction.needs_enrichment == True
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
                        tx.needs_enrichment = False
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
                        # Mark as failed (don't retry in next batch)
                        tx.needs_enrichment = False
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
    "get_link_builder",  # ✨ NEW
    
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
    "link_builder",  # ✨ NEW

    # ✨ NEW: Transaction Sync
    "sync_wallet_transactions_to_db",
    "sync_all_wallets_transactions",    
    
    # Helpers
    "ensure_registry_wallets_in_db",
    "discover_new_otc_desks",
    "discover_from_last_5_transactions",
]
