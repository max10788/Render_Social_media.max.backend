"""
Shared Dependencies & Services
===============================

✨ UPDATED: Proper service initialization with dependency injection

All shared dependencies, service initialization, and helper functions.
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
# SERVICE INITIALIZATION - ✨ WITH PROPER DEPENDENCY INJECTION
# ============================================================================

# Core services
cache_manager = CacheManager()

# Blockchain services (initialize first)
node_provider = NodeProvider(chain_id=1)  # Ethereum mainnet
etherscan = EtherscanAPI(chain_id=1)

# ✅ PriceOracle WITH Etherscan injected
price_oracle = PriceOracle(cache_manager, etherscan)  # Pass etherscan!

# ✅ WalletProfiler WITH PriceOracle injected
wallet_profiler = WalletProfiler(price_oracle)  # Pass price_oracle!

# Transaction extractor
transaction_extractor = TransactionExtractor(node_provider, etherscan)

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
logger.info(f"   • PriceOracle: {type(price_oracle).__name__} (with Etherscan)")
logger.info(f"   • WalletProfiler: {type(wallet_profiler).__name__} (with PriceOracle)")


# ============================================================================
# DEPENDENCY FUNCTIONS
# ============================================================================

def get_current_user():
    """
    Get current authenticated user.
    
    TODO: Implement real JWT authentication in production
    """
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
# HELPER FUNCTIONS
# ============================================================================

async def ensure_registry_wallets_in_db(
    db: Session,
    max_to_fetch: int = 1,  # ✅ CHANGED: 1 statt 3
    skip_if_recent: bool = True
):
    """
    Ensures registry wallets are in database.
    
    ✅ OPTIMIZED: 12h Cache + nur 1 Wallet pro Request
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from datetime import datetime, timedelta
    from sqlalchemy.exc import IntegrityError
    
    # ✅ CHANGED: 12h Cache statt 1h
    if skip_if_recent:
        cache_threshold = datetime.now() - timedelta(hours=12)  # ← HIER!
        
        recent_count = db.query(OTCWallet).filter(
            OTCWallet.updated_at >= cache_threshold
        ).count()
        
        if recent_count >= 3:  # ✅ CHANGED: 3 statt 5
            logger.info(f"⚡ Fast path: {recent_count} wallets cached (12h)")
            return {"cached": True, "count": recent_count}
    
    stats = {"fetched": 0, "kept": 0, "skipped": 0, "updated": 0}
    
    try:
        # Get all OTC desk addresses from registry
        desks = otc_registry.get_desk_list()
        all_addresses = []
        
        # ✅ Build address-to-desk mapping
        address_to_desk = {}
        
        logger.info(f"🔄 Auto-sync: Checking {len(desks)} OTC desks...")
        
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
                else:
                    logger.warning(f"   ⚠️  {desk_name}: No addresses found")
                    
            except Exception as e:
                logger.error(f"❌ Error processing desk {desk_name}: {e}")
                continue
        
        logger.info(f"📊 Found {len(all_addresses)} total addresses across all desks")
        
        if len(all_addresses) == 0:
            logger.warning(f"⚠️  No addresses found in registry!")
            return stats
        
        # Check each address
        addresses_to_fetch = []
        
        # ✅ OPTIMIZED: Längerer Cache per Wallet (12h)
        cache_threshold = datetime.now() - timedelta(hours=12)
        
        for address in all_addresses:
            try:
                address_str = str(address).strip()
                
                if not address_str.startswith('0x') or len(address_str) != 42:
                    logger.warning(f"⚠️  Invalid address format: {address_str}")
                    continue
                
                validated = validate_ethereum_address(address_str)
                
                # Check if exists in DB with recent update
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
        
        # ✅ OPTIMIZED: Fetch nur max_to_fetch Wallets (default: 1)
        for address in addresses_to_fetch[:max_to_fetch]:
            try:
                logger.info(f"🔄 Auto-fetching {address[:10]}...")
                
                # Extract transactions
                transactions = transaction_extractor.extract_wallet_transactions(
                    address,
                    include_internal=True,
                    include_tokens=True
                )
                
                if not transactions:
                    logger.info(f"⚠️  No transactions found - skipping")
                    stats["skipped"] += 1
                    continue
                
                # ✅ OPTIMIZED: Nur 30 statt 50 Transaktionen enrichen
                transactions = transaction_extractor.enrich_with_usd_value(
                    transactions,
                    price_oracle,
                    max_transactions=30  # ← 30 statt 50!
                )
                
                # Get labels from REGISTRY FIRST
                desk_info = address_to_desk.get(address.lower())
                
                if desk_info:
                    labels = {
                        'entity_type': 'otc_desk',
                        'entity_name': desk_info.get('name'),
                        'labels': ['verified_otc_desk', 'registry', desk_info.get('type', 'otc')],
                        'source': 'registry',
                        'confidence': 1.0
                    }
                    logger.info(f"   ✅ Registry labels: {desk_info.get('name')} (otc_desk)")
                else:
                    external_labels = labeling_service.get_wallet_labels(address)
                    
                    if external_labels and external_labels.get('entity_type') != 'unknown':
                        labels = external_labels
                        logger.info(
                            f"   🏷️ External labels: {external_labels.get('entity_name')} "
                            f"({external_labels.get('entity_type')})"
                        )
                    else:
                        labels = {
                            'entity_type': 'unknown',
                            'entity_name': None,
                            'labels': [],
                            'source': 'none'
                        }
                        logger.info(f"   ⚠️  No labels found for {address[:10]}...")
                
                # Create profile
                profile = wallet_profiler.create_profile(address, transactions, labels)
                
                # Calculate confidence
                otc_probability = wallet_profiler.calculate_otc_probability(profile)
                confidence = otc_probability * 100
                
                # Get metrics
                total_volume = profile.get('total_volume_usd', 0)
                data_quality = profile.get('data_quality', 'unknown')
                
                logger.info(f"📊 Profile complete:")
                logger.info(f"   • Entity: {labels.get('entity_type')} / {labels.get('entity_name')}")
                logger.info(f"   • Confidence: {confidence:.1f}%")
                logger.info(f"   • Volume: ${total_volume:,.0f}")
                logger.info(f"   • Data quality: {data_quality}")
                
                # Check confidence threshold
                if labels.get('source') == 'registry':
                    min_confidence = 40.0
                    logger.info(f"   ℹ️  Registry wallet - using 40% threshold")
                else:
                    min_confidence = 60.0
                
                if confidence >= min_confidence:
                    # Determine entity_type
                    if labels.get('source') == 'registry':
                        entity_type = 'otc_desk'
                    else:
                        entity_type = labels.get('entity_type', 'unknown')
                    
                    # ✅ Check if wallet exists - UPDATE instead of INSERT
                    existing_wallet = db.query(OTCWallet).filter(
                        OTCWallet.address == address
                    ).first()
                    
                    if existing_wallet:
                        # ✅ UPDATE existing wallet
                        logger.info(f"📝 Updating existing wallet {address[:10]}...")
                        
                        existing_wallet.entity_type = entity_type
                        existing_wallet.entity_name = labels.get('entity_name')
                        existing_wallet.label = labels.get('entity_name') or f"{address[:8]}..."
                        existing_wallet.confidence_score = confidence
                        existing_wallet.total_volume = total_volume
                        existing_wallet.transaction_count = len(transactions)
                        existing_wallet.last_active = datetime.now()
                        existing_wallet.is_active = True
                        existing_wallet.tags = labels.get('labels', [])
                        existing_wallet.updated_at = datetime.now()
                        
                        try:
                            db.commit()
                            logger.info(
                                f"✅ Updated {address[:10]}... "
                                f"(threshold: {min_confidence}%)"
                            )
                            stats["updated"] += 1
                        except Exception as commit_error:
                            logger.error(f"❌ Commit failed for UPDATE: {commit_error}")
                            db.rollback()
                            stats["skipped"] += 1
                    else:
                        # ✅ INSERT new wallet
                        wallet = OTCWallet(
                            address=address,
                            label=labels.get('entity_name') or f"{address[:8]}...",
                            entity_type=entity_type,
                            entity_name=labels.get('entity_name'),
                            confidence_score=confidence,
                            total_volume=total_volume,
                            transaction_count=len(transactions),
                            first_seen=datetime.now() - timedelta(days=365),
                            last_active=datetime.now(),
                            is_active=True,
                            tags=labels.get('labels', []),
                            created_at=datetime.now(),
                            updated_at=datetime.now()
                        )
                        
                        try:
                            db.add(wallet)
                            db.commit()
                            logger.info(
                                f"✅ Inserted new wallet {address[:10]}... "
                                f"(threshold: {min_confidence}%)"
                            )
                            stats["fetched"] += 1
                        except IntegrityError as ie:
                            # Race condition: Another process inserted this wallet
                            logger.warning(
                                f"⚠️  Wallet {address[:10]}... was inserted "
                                f"by another process"
                            )
                            db.rollback()
                            
                            # Try to update instead
                            existing = db.query(OTCWallet).filter(
                                OTCWallet.address == address
                            ).first()
                            
                            if existing:
                                existing.confidence_score = confidence
                                existing.total_volume = total_volume
                                existing.updated_at = datetime.now()
                                db.commit()
                                logger.info(f"✅ Updated after race condition")
                                stats["updated"] += 1
                            else:
                                stats["skipped"] += 1
                        except Exception as commit_error:
                            logger.error(f"❌ Commit failed for INSERT: {commit_error}")
                            db.rollback()
                            stats["skipped"] += 1
                else:
                    logger.info(
                        f"⚠️  Low confidence ({confidence:.1f}% < {min_confidence}%) - "
                        f"not saving"
                    )
                    stats["skipped"] += 1
                
                # ✅ OPTIMIZED: Kurze Pause zwischen Wallets
                time.sleep(0.3)  # 0.3s statt 0.5s
                
            except Exception as e:
                logger.error(f"❌ Error auto-fetching {address}: {e}", exc_info=True)
                db.rollback()
                stats["skipped"] += 1
                continue
        
        # Final summary
        if stats["fetched"] > 0 or stats["updated"] > 0:
            logger.info(
                f"✅ Auto-sync: "
                f"Fetched {stats['fetched']}, "
                f"Updated {stats['updated']}, "
                f"Kept {stats['kept']}, "
                f"Skipped {stats['skipped']}"
            )
        else:
            logger.info(
                f"ℹ️  Auto-sync: No changes "
                f"(Kept {stats['kept']}, Skipped {stats['skipped']})"
            )
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Auto-sync failed: {e}", exc_info=True)
        db.rollback()
        return stats

async def discover_new_otc_desks(
    db: Session,
    max_discoveries: int = 5
) -> List[Dict]:
    """
    🕵️ Discover new OTC desks through counterparty analysis.
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from app.core.otc_analysis.discovery.counterparty_analyzer import CounterpartyAnalyzer
    
    logger.info("🕵️ Starting OTC discovery...")
    
    try:
        # Get known OTC desks
        known_otc = db.query(OTCWallet).filter(
            OTCWallet.entity_type == 'otc_desk',
            OTCWallet.confidence_score >= 70.0
        ).all()
        
        if len(known_otc) < 2:
            logger.warning("⚠️ Need 2+ known OTC desks")
            return []
        
        known_addresses = [w.address for w in known_otc]
        logger.info(f"📊 Analyzing {len(known_addresses)} OTC desks...")
        
        # Initialize analyzer
        analyzer = CounterpartyAnalyzer(
            db=db,
            transaction_extractor=transaction_extractor,
            wallet_profiler=wallet_profiler
        )
        
        # Discover candidates
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
        
        # Validate and save
        discovered = []
        
        for candidate in candidates:
            address = candidate['address']
            
            # Skip if exists
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
            
            # Full profile
            try:
                transactions = transaction_extractor.extract_wallet_transactions(
                    address,
                    include_internal=True,
                    include_tokens=True
                )
                
                if transactions:
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
                    
                    # Combine scores
                    combined = (confidence + candidate['discovery_score']) / 2
                    
                    logger.info(f"      Profile: {combined:.1f}% confidence")
                    
                    # Save if good enough
                    if combined >= 60.0:
                        wallet = OTCWallet(
                            address=address,
                            label=f"Discovered {address[:8]}",
                            entity_type='otc_desk',
                            entity_name=f"Discovered OTC {address[:8]}",
                            confidence_score=combined,
                            total_volume=profile.get('total_volume_usd', 0),
                            transaction_count=len(transactions),
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

# ============================================================================
# ✅ NEU: Simple Last-5-TX Discovery
# ============================================================================

async def discover_from_last_5_transactions(
    db: Session,
    otc_address: str,
    num_transactions: int = 5
) -> List[Dict]:
    """
    🔍 Analysiere Counterparties der letzten N Transaktionen.
    
    Simplest Discovery:
    1. Hole letzte 5 TXs vom OTC Desk
    2. Extrahiere Counterparty-Adressen
    3. Analysiere jede Counterparty
    4. Speichere wenn OTC-Score gut
    
    Args:
        db: Database session
        otc_address: Bekannter OTC Desk
        num_transactions: Anzahl Transaktionen (default: 5)
    
    Returns:
        Liste von discovered wallets
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from app.core.otc_analysis.discovery.simple_analyzer import SimpleLastTxAnalyzer
    
    logger.info(f"🔍 Simple Discovery: Last {num_transactions} TXs from {otc_address[:10]}...")
    
    try:
        # Initialize analyzer
        analyzer = SimpleLastTxAnalyzer(
            db=db,
            transaction_extractor=transaction_extractor,
            wallet_profiler=wallet_profiler,
            price_oracle=price_oracle
        )
        
        # 1. Get counterparties from last N transactions
        counterparties = analyzer.discover_from_last_transactions(
            otc_address=otc_address,
            num_transactions=num_transactions
        )
        
        if not counterparties:
            logger.info("ℹ️ No counterparties found")
            return []
        
        # 2. Analyze each counterparty
        discovered = []
        
        for cp_data in counterparties:
            address = cp_data['address']
            
            # Skip if already in DB
            existing = db.query(OTCWallet).filter(
                OTCWallet.address == address
            ).first()
            
            if existing:
                logger.info(f"   ⚠️ {address[:10]}... already exists (score: {existing.confidence_score:.1f}%)")
                continue
            
            # Analyze
            analysis = analyzer.analyze_counterparty(address)
            
            if not analysis:
                logger.info(f"   ⚠️ {address[:10]}... analysis failed")
                continue
            
            logger.info(
                f"   🆕 {address[:10]}... - "
                f"Confidence: {analysis['confidence']:.1f}%, "
                f"Volume: ${analysis['total_volume']:,.0f}, "
                f"TXs: {analysis['transaction_count']}"
            )
            
            # Save if confidence is good
            if analysis['confidence'] >= 60.0:
                wallet = OTCWallet(
                    address=address,
                    label=f"Discovered {address[:8]}",
                    entity_type='otc_desk',
                    entity_name=f"Discovered OTC {address[:8]}",
                    confidence_score=analysis['confidence'],
                    total_volume=analysis['total_volume'],
                    transaction_count=analysis['transaction_count'],
                    first_seen=analysis['first_seen'],
                    last_active=analysis['last_seen'],
                    is_active=True,
                    tags=['discovered', 'last_tx_analysis'],
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                
                db.add(wallet)
                db.commit()
                
                discovered.append({
                    'address': address,
                    'confidence': analysis['confidence'],
                    'volume': analysis['total_volume'],
                    'tx_count': analysis['transaction_count'],
                    'counterparty_data': cp_data
                })
                
                logger.info(f"      ✅ Saved to DB")
            else:
                logger.info(f"      ⚠️ Low confidence ({analysis['confidence']:.1f}%) - not saving")
        
        logger.info(f"✅ Discovery complete: {len(discovered)} new wallets found")
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
]
