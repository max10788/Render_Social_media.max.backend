"""
Shared Dependencies & Services
===============================

✨ UPDATED: Proper service initialization with dependency injection

All shared dependencies, service initialization, and helper functions.
"""

import os
import time
import logging
from typing import Optional
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
    max_to_fetch: int = 3,
    skip_if_recent: bool = True
):
    """
    Ensures registry wallets are in database.
    
    ✅ FIXED: Now properly uses registry labels for OTC desk detection
    ✅ FIXED: UPDATE existing wallets instead of INSERT
    ✅ FIXED: Proper rollback on errors
    ✅ OPTIMIZATION: Skip if data is recent
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from datetime import datetime, timedelta
    from sqlalchemy.exc import IntegrityError
    
    # ✅ Check if we have recent data
    if skip_if_recent:
        recent_count = db.query(OTCWallet).filter(
            OTCWallet.updated_at >= datetime.now() - timedelta(hours=1)
        ).count()
        
        if recent_count >= 5:
            logger.info(f"⏭️  Skipping auto-sync: {recent_count} wallets updated in last hour")
            return
    
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
        
        for address in all_addresses:
            try:
                address_str = str(address).strip()
                
                if not address_str.startswith('0x') or len(address_str) != 42:
                    logger.warning(f"⚠️  Invalid address format: {address_str}")
                    continue
                
                validated = validate_ethereum_address(address_str)
                
                # Check if exists in DB with good confidence
                wallet = db.query(OTCWallet).filter(
                    OTCWallet.address == validated
                ).first()
                
                if wallet and wallet.confidence_score >= 50.0:
                    stats["kept"] += 1
                    continue
                else:
                    addresses_to_fetch.append(validated)
                    
            except Exception as e:
                logger.error(f"❌ Error validating address {address}: {e}")
                continue
        
        logger.info(f"🎯 Need to fetch: {len(addresses_to_fetch)} wallets (kept {stats['kept']})")
        
        # Fetch missing wallets (up to max_to_fetch)
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
                
                # Enrich with prices
                transactions = transaction_extractor.enrich_with_usd_value(
                    transactions,
                    price_oracle,
                    max_transactions=50
                )
                
                # ====================================================================
                # Get labels from REGISTRY FIRST
                # ====================================================================
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
                        logger.info(f"   🏷️ External labels: {external_labels.get('entity_name')} ({external_labels.get('entity_type')})")
                    else:
                        labels = {
                            'entity_type': 'unknown',
                            'entity_name': None,
                            'labels': [],
                            'source': 'none'
                        }
                        logger.info(f"   ⚠️  No labels found for {address[:10]}...")
                
                # ====================================================================
                # Create profile
                # ====================================================================
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
                
                # ====================================================================
                # ✅ FIXED: Check confidence threshold
                # ====================================================================
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
                    
                    # ====================================================================
                    # ✅ CRITICAL FIX: Check if wallet exists - UPDATE instead of INSERT
                    # ====================================================================
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
                            logger.info(f"✅ Updated {address[:10]}... (threshold: {min_confidence}%)")
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
                            logger.info(f"✅ Inserted new wallet {address[:10]}... (threshold: {min_confidence}%)")
                            stats["fetched"] += 1
                        except IntegrityError as ie:
                            # ✅ Handle race condition: Another process inserted this wallet
                            logger.warning(f"⚠️  Wallet {address[:10]}... was inserted by another process")
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
                    logger.info(f"⚠️  Low confidence ({confidence:.1f}% < {min_confidence}%) - not saving")
                    stats["skipped"] += 1
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Error auto-fetching {address}: {e}", exc_info=True)
                db.rollback()  # ✅ CRITICAL: Rollback on any error
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
        db.rollback()  # ✅ CRITICAL: Rollback on top-level error
        return stats

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
