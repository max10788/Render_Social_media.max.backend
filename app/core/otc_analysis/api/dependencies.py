"""
Shared Dependencies & Services
===============================

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
# SERVICE INITIALIZATION
# ============================================================================

# Core services
cache_manager = CacheManager()
otc_registry = OTCDeskRegistry(cache_manager)
labeling_service = WalletLabelingService(cache_manager)
otc_detector = OTCDetector(cache_manager, otc_registry, labeling_service)
wallet_profiler = WalletProfiler()
flow_tracer = FlowTracer()

# Blockchain services
node_provider = NodeProvider(chain_id=1)  # Ethereum mainnet
etherscan = EtherscanAPI(chain_id=1)
price_oracle = PriceOracle(cache_manager)
transaction_extractor = TransactionExtractor(node_provider, etherscan)
block_scanner = BlockScanner(node_provider, chain_id=1)

# Analysis services
statistics_service = StatisticsService(cache_manager)
graph_builder = GraphBuilderService(cache_manager)


# ============================================================================
# DEPENDENCY FUNCTIONS
# ============================================================================

def get_current_user():
    """
    Get current authenticated user.
    
    TODO: Implement real JWT authentication in production:
    
    from jose import jwt
    
    def get_current_user(authorization: str = Header(...)):
        token = authorization.replace("Bearer ", "")
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload.get("sub")
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
    max_to_fetch: int = 3,  # ✅ Reduced von 5 auf 3
    skip_if_recent: bool = True  # ✅ NEW Parameter
):
    """
    Ensures registry wallets are in database.
    
    ✅ OPTIMIZATION: Skip if data is recent
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from datetime import datetime, timedelta
    
    # ✅ Check if we have recent data
    if skip_if_recent:
        recent_count = db.query(OTCWallet).filter(
            OTCWallet.updated_at >= datetime.now() - timedelta(hours=1)
        ).count()
        
        if recent_count >= 5:
            logger.info(f"⏭️  Skipping auto-sync: {recent_count} wallets updated in last hour")
            return
    
    stats = {"fetched": 0, "kept": 0, "skipped": 0}
    
    try:
        # Get all OTC desk addresses from registry
        desks = otc_registry.get_desk_list()
        all_addresses = []
        
        logger.info(f"🔄 Auto-sync: Checking {len(desks)} OTC desks...")
        
        for desk in desks:
            try:
                desk_name = desk.get('name', 'unknown')
                desk_info = otc_registry.get_desk_by_name(desk_name)
                
                if desk_info and 'addresses' in desk_info:
                    addresses = desk_info['addresses']
                    
                    if isinstance(addresses, list):
                        all_addresses.extend(addresses)
                        logger.info(f"   ✅ {desk_name}: {len(addresses)} addresses")
                    elif isinstance(addresses, str):
                        all_addresses.append(addresses)
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
                
                if wallet and wallet.confidence_score >= 80.0:
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
                
                enriched = [tx for tx in transactions if tx.get('usd_value')]
                
                if not enriched:
                    logger.info(f"⚠️  No enriched transactions - skipping")
                    stats["skipped"] += 1
                    continue
                
                # Calculate metrics
                total_volume = sum(tx['usd_value'] for tx in enriched)
                
                # Get labels
                labels = labeling_service.get_wallet_labels(address)
                
                # Create profile
                profile = wallet_profiler.create_profile(address, transactions, labels)
                
                # Calculate confidence
                otc_probability = wallet_profiler.calculate_otc_probability(profile)
                confidence = otc_probability * 100
                
                logger.info(f"📊 Confidence: {confidence:.1f}%, Volume: ${total_volume:,.0f}")
                
                # Auto-save if high confidence
                if confidence >= 80.0:
                    wallet = OTCWallet(
                        address=address,
                        label=labels.get('entity_name') if labels else f"{address[:8]}...",
                        entity_type=labels.get('entity_type', 'unknown') if labels else 'unknown',
                        entity_name=labels.get('entity_name') if labels else None,
                        confidence_score=confidence,
                        total_volume=total_volume,
                        transaction_count=len(transactions),
                        first_seen=datetime.now() - timedelta(days=365),
                        last_active=datetime.now(),
                        is_active=True,
                        tags=labels.get('labels', []) if labels else [],
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    db.add(wallet)
                    db.commit()
                    
                    logger.info(f"✅ Auto-saved {address[:10]}... to DB")
                    stats["fetched"] += 1
                else:
                    logger.info(f"⚠️  Low confidence ({confidence:.1f}%) - not saving")
                    stats["skipped"] += 1
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Error auto-fetching {address}: {e}", exc_info=True)
                stats["skipped"] += 1
                continue
        
        if stats["fetched"] > 0:
            logger.info(f"✅ Auto-sync: Fetched {stats['fetched']}, Kept {stats['kept']}, Skipped {stats['skipped']}")
        else:
            logger.info(f"ℹ️  Auto-sync: No new wallets fetched (Kept {stats['kept']}, Skipped {stats['skipped']})")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Auto-sync failed: {e}", exc_info=True)
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
