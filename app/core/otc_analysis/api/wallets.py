"""
Wallet Profile Endpoints - ENHANCED VERSION
============================================

✅ COMPLETE FEATURES:
- Real-time balance data via BalanceFetcher
- Chart data generation (activity, transfer size)
- Period volume calculations (7d, 30d, etc)
- Metadata enrichment (is_verified, data_source, tags)
- Network metrics normalization
- Proper error handling and fallbacks

Version: 2.0 - Complete Enhancement
Date: 2025-01-17
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone

from .dependencies import (
    get_db,
    get_labeling_service,
    get_transaction_extractor,
    get_price_oracle,
    get_wallet_profiler,
    get_cache_manager,
    get_balance_fetcher
)

from app.core.otc_analysis.api.validators import validate_ethereum_address
from app.core.otc_analysis.analysis.network_graph import NetworkAnalysisService
from app.core.otc_analysis.utils.chart_generators import (
    ChartDataGenerator,
    NetworkMetricsNormalizer
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/wallet", tags=["Wallets"])


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def ensure_aware_datetime(dt):
    """
    Stelle sicher dass datetime timezone-aware ist (UTC).
    
    Args:
        dt: datetime object (aware oder naive)
        
    Returns:
        timezone-aware datetime (immer UTC)
    """
    if dt is None:
        return None
    
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    
    return dt


# ============================================================================
# ✨ ENHANCED WALLET PROFILE ENDPOINT
# ============================================================================

@router.get("/{address}/profile")
async def get_wallet_profile(
    address: str,
    include_network_metrics: bool = Query(True),
    include_labels: bool = Query(True),
    include_charts: bool = Query(True),
    include_balance: bool = Query(True),
    labeling = Depends(get_labeling_service),
    tx_extractor = Depends(get_transaction_extractor),
    oracle = Depends(get_price_oracle),
    profiler = Depends(get_wallet_profiler),
    cache = Depends(get_cache_manager),
    balance_fetcher = Depends(get_balance_fetcher)
):
    """
    Get detailed profile for a wallet address.
    
    ✨ ENHANCED VERSION with:
    - Real-time balance data
    - Activity & transfer size charts
    - Period volume calculations
    - Metadata enrichment
    - Network metrics normalization
    
    GET /api/otc/wallet/0x.../profile
    
    Query Parameters:
        include_network_metrics: Include network centrality (default: true)
        include_labels: Include external labels (default: true)
        include_charts: Include chart data (default: true)
        include_balance: Include live balance (default: true)
    """
    logger.info(f"👤 Fetching ENHANCED profile for {address[:10]}...")
    
    try:
        address = validate_ethereum_address(address)
        
        # ====================================================================
        # STEP 1: Check cache (optional)
        # ====================================================================
        
        cached_profile = cache.get_wallet_profile(address)
        if cached_profile and not include_balance:  # Don't use cache if balance requested
            logger.info(f"✅ Profile loaded from cache")
            return {"success": True, "data": cached_profile, "cached": True}
        
        # ====================================================================
        # STEP 2: Fetch transactions
        # ====================================================================
        
        logger.info(f"📡 Fetching transactions from Etherscan...")
        transactions = tx_extractor.extract_wallet_transactions(
            address,
            include_internal=True,
            include_tokens=True
        )
        logger.info(f"✅ Found {len(transactions)} transactions")
        
        # Transaction breakdown
        normal_txs = [tx for tx in transactions if tx.get('tx_type') == 'normal']
        internal_txs = [tx for tx in transactions if tx.get('tx_type') == 'internal']
        token_txs = [tx for tx in transactions if tx.get('tx_type') == 'erc20']
        
        logger.info(f"📊 Transaction Breakdown:")
        logger.info(f"   • Normal: {len(normal_txs)}")
        logger.info(f"   • Internal: {len(internal_txs)}")
        logger.info(f"   • ERC20 Tokens: {len(token_txs)}")
        
        # ====================================================================
        # STEP 3: Enrich with prices
        # ====================================================================
        
        logger.info(f"💰 Enriching with prices...")
        transactions = tx_extractor.enrich_with_usd_value(
            transactions,
            oracle
        )
        
        enriched_txs = [tx for tx in transactions if tx.get('usd_value') is not None]
        logger.info(f"💵 Enriched {len(enriched_txs)}/{len(transactions)} transactions")
        
        # ====================================================================
        # STEP 4: Get labels
        # ====================================================================
        
        labels = None
        if include_labels:
            logger.info(f"🏷️  Fetching wallet labels...")
            labels = labeling.get_wallet_labels(address)
            
            if labels and labels.get('entity_type') != 'unknown':
                logger.info(f"🏢 Entity Identified:")
                logger.info(f"   • Type: {labels.get('entity_type')}")
                logger.info(f"   • Name: {labels.get('entity_name', 'N/A')}")
        
        # ====================================================================
        # STEP 5: Build base profile
        # ====================================================================
        
        logger.info(f"📊 Building wallet profile...")
        profile = profiler.create_profile(address, transactions, labels)
        
        # ====================================================================
        # ✨ STEP 6: CALCULATE PERIOD VOLUMES
        # ====================================================================
        
        logger.info(f"📊 Calculating period volumes...")
        period_volumes = ChartDataGenerator.calculate_period_volumes(enriched_txs)
        
        # Add to profile
        profile['volume_24h'] = period_volumes.get('volume_24h', 0)
        profile['volume_7d'] = period_volumes.get('volume_7d', 0)
        profile['volume_30d'] = period_volumes.get('volume_30d', 0)
        profile['volume_90d'] = period_volumes.get('volume_90d', 0)
        profile['volume_1y'] = period_volumes.get('volume_1y', 0)
        
        # Keep legacy field for backwards compatibility
        if 'total_volume_usd' not in profile or profile['total_volume_usd'] == 0:
            profile['total_volume_usd'] = period_volumes.get('volume_1y', 0)
        
        profile['lifetime_volume'] = profile.get('total_volume_usd', 0)
        
        # ====================================================================
        # ✨ STEP 7: ADD LIVE BALANCE DATA
        # ====================================================================
        
        if include_balance:
            logger.info(f"💰 Fetching live balance data...")
            try:
                balance_data = balance_fetcher.get_wallet_balance(address)
                
                profile['balance_eth'] = balance_data.get('balance_eth', 0)
                profile['balance_usd'] = balance_data.get('balance_usd', 0)
                profile['balance_wei'] = balance_data.get('balance_wei', 0)
                profile['balance_last_updated'] = balance_data.get('last_updated')
                profile['balance_source'] = balance_data.get('source', 'unknown')
                
                logger.info(
                    f"   ✅ Balance: {balance_data.get('balance_eth', 0):.4f} ETH "
                    f"(${balance_data.get('balance_usd', 0):,.2f})"
                )
                
            except Exception as balance_error:
                logger.warning(f"   ⚠️ Balance fetch failed: {balance_error}")
                profile['balance_eth'] = None
                profile['balance_usd'] = None
                profile['balance_error'] = str(balance_error)
        else:
            logger.info(f"   ⏭️ Skipping balance fetch (include_balance=False)")
        
        # ====================================================================
        # ✨ STEP 8: GENERATE CHART DATA
        # ====================================================================
        
        if include_charts:
            logger.info(f"📈 Generating chart data...")
            
            try:
                # Activity chart (7 days)
                activity_data = ChartDataGenerator.generate_activity_chart(
                    enriched_txs,
                    days=7
                )
                profile['activity_data'] = activity_data
                
                # Transfer size chart (7 days)
                transfer_size_data = ChartDataGenerator.generate_transfer_size_chart(
                    enriched_txs,
                    days=7
                )
                profile['transfer_size_data'] = transfer_size_data
                
                logger.info(f"   ✅ Charts generated (7-day window)")
                
            except Exception as chart_error:
                logger.warning(f"   ⚠️ Chart generation failed: {chart_error}")
                profile['activity_data'] = []
                profile['transfer_size_data'] = []
        else:
            logger.info(f"   ⏭️ Skipping chart generation (include_charts=False)")
            profile['activity_data'] = []
            profile['transfer_size_data'] = []
        
        # ====================================================================
        # ✨ STEP 9: ADD METADATA ENRICHMENT
        # ====================================================================
        
        logger.info(f"🏷️  Adding metadata enrichment...")
        
        # is_verified: Based on confidence + profile method
        profile['is_verified'] = (
            profile.get('confidence_score', 0) >= 0.8 and
            profile.get('profile_method') in ['quick_stats', 'transaction_processing']
        )
        
        # data_source
        if profile.get('profile_method') == 'quick_stats':
            profile['data_source'] = 'etherscan_live'
        elif profile.get('profile_method') == 'transaction_processing':
            profile['data_source'] = 'etherscan_live'
        else:
            profile['data_source'] = 'database'
        
        # tags (convert from labels + entity_type)
        tags = []
        if labels:
            tags.extend(labels.get('labels', []))
        if profile.get('entity_type') and profile['entity_type'] != 'unknown':
            tags.append(profile['entity_type'])
        if profile.get('is_verified'):
            tags.append('verified')
        
        # Remove duplicates while preserving order
        seen = set()
        unique_tags = []
        for tag in tags:
            if tag not in seen:
                seen.add(tag)
                unique_tags.append(tag)
        
        profile['tags'] = unique_tags
        
        # last_activity (formatted for UI)
        if profile.get('last_seen'):
            profile['last_activity'] = ChartDataGenerator.format_last_activity(
                profile['last_seen']
            )
        else:
            profile['last_activity'] = 'Unknown'
        
        # last_updated timestamp
        profile['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        # ====================================================================
        # STEP 10: Calculate OTC probability
        # ====================================================================
        
        otc_probability = profiler.calculate_otc_probability(profile)
        profile['otc_probability'] = otc_probability
        
        logger.info(f"🎯 OTC Probability: {otc_probability:.2%}")
        
        # ====================================================================
        # STEP 11: Network metrics (optional)
        # ====================================================================
        
        if include_network_metrics and len(transactions) > 0:
            logger.info(f"🕸️  Calculating network metrics...")
            try:
                network_analyzer = NetworkAnalysisService()
                network_analyzer.build_graph(transactions)
                network_metrics = network_analyzer.analyze_wallet_centrality(address)
                
                # Normalize to 0-100 scale
                normalized_metrics = NetworkMetricsNormalizer.normalize(network_metrics)
                
                if normalized_metrics:
                    profile['network_metrics'] = normalized_metrics
                    logger.info(f"   ✅ Network metrics calculated and normalized")
                else:
                    profile['network_metrics'] = None
                    logger.info(f"   ℹ️ Network metrics not available")
                    
            except Exception as network_error:
                logger.warning(f"   ⚠️ Network analysis failed: {network_error}")
                profile['network_metrics'] = None
        else:
            profile['network_metrics'] = None
        
        # ====================================================================
        # STEP 12: Add transaction stats
        # ====================================================================
        
        profile['enriched_transaction_count'] = len(enriched_txs)
        profile['total_transaction_count'] = len(transactions)
        profile['enrichment_rate'] = (
            len(enriched_txs) / len(transactions) if transactions else 0
        )
        
        # ====================================================================
        # STEP 13: Cache profile (optional)
        # ====================================================================
        
        # Only cache if not including live balance
        if not include_balance:
            cache.cache_wallet_profile(address, profile)
            logger.info(f"💾 Profile cached")
        
        # ====================================================================
        # FINAL SUMMARY
        # ====================================================================
        
        logger.info(f"✅ Profile complete:")
        logger.info(f"   • Entity: {profile.get('entity_type')} / {profile.get('entity_name')}")
        logger.info(f"   • Lifetime Volume: ${profile.get('lifetime_volume', 0):,.2f}")
        logger.info(f"   • 30-Day Volume: ${profile.get('volume_30d', 0):,.2f}")
        logger.info(f"   • 7-Day Volume: ${profile.get('volume_7d', 0):,.2f}")
        logger.info(f"   • Balance: {profile.get('balance_eth', 0):.4f} ETH")
        logger.info(f"   • OTC Probability: {otc_probability:.2%}")
        logger.info(f"   • Verified: {profile.get('is_verified')}")
        logger.info(f"   • Data Source: {profile.get('data_source')}")
        logger.info(f"   • Tags: {len(profile.get('tags', []))}")
        logger.info(f"   • Charts: {len(profile.get('activity_data', []))} days")
        
        return {
            "success": True,
            "data": profile,
            "cached": False
        }
        
    except Exception as e:
        logger.error(f"❌ Profile fetch failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ✨ NEW: WALLET DETAILS ENDPOINT (SIMPLIFIED)
# ============================================================================

@router.get("/{address}/details")
async def get_wallet_details(
    address: str,
    db: Session = Depends(get_db),
    balance_fetcher = Depends(get_balance_fetcher),
    labeling = Depends(get_labeling_service)
):
    """
    Get quick wallet details with live balance.
    
    ✨ ROBUST VERSION:
    - Tries DB first
    - Falls back to live data if not in DB
    - Always includes live balance
    
    GET /api/otc/wallet/0x.../details
    """
    try:
        from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
        
        logger.info(f"👤 GET /wallet/{address[:10]}.../details")
        
        address = validate_ethereum_address(address)
        
        # ====================================================================
        # Try to get wallet from DB
        # ====================================================================
        
        wallet = db.query(OTCWallet).filter(
            OTCWallet.address == address
        ).first()
        
        # ====================================================================
        # CASE 1: Wallet found in DB
        # ====================================================================
        
        if wallet:
            logger.info(f"   ✅ Found wallet in DB")
            
            details = {
                'address': wallet.address,
                'label': wallet.label,
                'entity_type': wallet.entity_type,
                'entity_name': wallet.entity_name,
                'confidence_score': wallet.confidence_score,
                'total_volume': wallet.total_volume,
                'transaction_count': wallet.transaction_count,
                'first_seen': wallet.first_seen,
                'last_active': wallet.last_active,
                'is_active': wallet.is_active,
                'tags': wallet.tags or [],
                
                # Metadata
                'is_verified': wallet.confidence_score >= 80.0,
                'data_source': 'database',
                'last_updated': wallet.updated_at
            }
            
            # Format last activity
            if wallet.last_active:
                details['last_activity'] = ChartDataGenerator.format_last_activity(
                    wallet.last_active
                )
            else:
                details['last_activity'] = 'Unknown'
        
        # ====================================================================
        # CASE 2: Wallet NOT in DB - Use live data
        # ====================================================================
        
        else:
            logger.info(f"   ⚠️ Wallet not in DB, using live data...")
            
            # Get labels
            labels = labeling.get_wallet_labels(address)
            
            details = {
                'address': address,
                'label': labels.get('entity_name') if labels else f"{address[:8]}...",
                'entity_type': labels.get('entity_type', 'unknown') if labels else 'unknown',
                'entity_name': labels.get('entity_name') if labels else None,
                'confidence_score': 50.0 if labels and labels.get('entity_type') != 'unknown' else 0.0,
                'total_volume': 0,
                'transaction_count': 0,
                'first_seen': None,
                'last_active': None,
                'is_active': False,
                'tags': labels.get('labels', []) if labels else [],
                
                # Metadata
                'is_verified': False,
                'data_source': 'live',
                'last_updated': datetime.now(timezone.utc),
                'last_activity': 'Unknown'
            }
            
            logger.info(f"   ℹ️ Using live data (wallet not tracked)")
        
        # ====================================================================
        # Add live balance (for both cases)
        # ====================================================================
        
        try:
            logger.info(f"💰 Fetching live balance...")
            balance_data = balance_fetcher.get_wallet_balance(address)
            
            details['balance_eth'] = balance_data.get('balance_eth', 0)
            details['balance_usd'] = balance_data.get('balance_usd', 0)
            details['balance_source'] = balance_data.get('source', 'unknown')
            
            logger.info(
                f"   ✅ Balance: {balance_data.get('balance_eth', 0):.4f} ETH "
                f"(${balance_data.get('balance_usd', 0):,.2f})"
            )
            
        except Exception as balance_error:
            logger.warning(f"   ⚠️ Balance fetch failed: {balance_error}")
            details['balance_eth'] = None
            details['balance_usd'] = None
            details['balance_error'] = str(balance_error)
        
        logger.info(f"✅ Details fetched successfully")
        
        return {
            "success": True,
            "data": details,
            "from_database": wallet is not None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in /wallet/details: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# EXPORT
# ============================================================================

__all__ = ['router']
