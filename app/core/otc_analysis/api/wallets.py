"""
Wallet Profile Endpoints
=========================

Endpoints for wallet profiling and analysis:
- Wallet profile
- Wallet details
- Wallet scanning
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

from .dependencies import (
    get_db,
    get_labeling_service,
    get_transaction_extractor,
    get_price_oracle,
    get_wallet_profiler,
    get_cache_manager
)

from app.core.otc_analysis.api.validators import validate_ethereum_address
from app.core.otc_analysis.analysis.network_graph import NetworkAnalysisService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/wallet", tags=["Wallets"])


# ============================================================================
# WALLET ENDPOINTS
# ============================================================================

@router.get("/{address}/profile")
async def get_wallet_profile(
    address: str,
    include_network_metrics: bool = Query(True),
    include_labels: bool = Query(True),
    labeling = Depends(get_labeling_service),
    tx_extractor = Depends(get_transaction_extractor),
    oracle = Depends(get_price_oracle),
    profiler = Depends(get_wallet_profiler),
    cache = Depends(get_cache_manager)
):
    """
    Get detailed profile for a wallet address.
    
    GET /api/otc/wallet/0x.../profile?include_network_metrics=true
    """
    logger.info(f"👤 Fetching profile for {address[:10]}...")
    
    try:
        address = validate_ethereum_address(address)
        
        cached_profile = cache.get_wallet_profile(address)
        if cached_profile:
            logger.info(f"✅ Profile loaded from cache")
            return {"success": True, "data": cached_profile, "cached": True}
        
        logger.info(f"📡 Fetching transactions from Etherscan...")
        transactions = tx_extractor.extract_wallet_transactions(
            address,
            include_internal=True,
            include_tokens=True
        )
        logger.info(f"✅ Found {len(transactions)} transactions")
        
        normal_txs = [tx for tx in transactions if tx.get('tx_type') == 'normal']
        internal_txs = [tx for tx in transactions if tx.get('tx_type') == 'internal']
        token_txs = [tx for tx in transactions if tx.get('tx_type') == 'erc20']
        
        logger.info(f"📊 Transaction Breakdown:")
        logger.info(f"   • Normal: {len(normal_txs)}")
        logger.info(f"   • Internal: {len(internal_txs)}")
        logger.info(f"   • ERC20 Tokens: {len(token_txs)}")
        
        logger.info(f"💰 Enriching with prices...")
        transactions = tx_extractor.enrich_with_usd_value(
            transactions,
            oracle
        )
        
        enriched_txs = [tx for tx in transactions if tx.get('usd_value') is not None]
        
        # Calculate metrics
        total_value = 0
        avg_value = 0
        max_tx_value = 0
        volume_30d = 0
        volume_7d = 0
        
        if enriched_txs:
            total_value = sum(tx['usd_value'] for tx in enriched_txs)
            avg_value = total_value / len(enriched_txs)
            max_tx = max(enriched_txs, key=lambda x: x['usd_value'])
            max_tx_value = max_tx['usd_value']
            
            # Calculate 30-day and 7-day volumes
            now = datetime.now()
            thirty_days_ago = now - timedelta(days=30)
            seven_days_ago = now - timedelta(days=7)
            
            recent_30d_txs = [
                tx for tx in enriched_txs 
                if tx.get('timestamp') and tx['timestamp'] >= thirty_days_ago
            ]
            recent_7d_txs = [
                tx for tx in enriched_txs 
                if tx.get('timestamp') and tx['timestamp'] >= seven_days_ago
            ]
            
            volume_30d = sum(tx['usd_value'] for tx in recent_30d_txs)
            volume_7d = sum(tx['usd_value'] for tx in recent_7d_txs)
            
            logger.info(f"💵 Transaction Values:")
            logger.info(f"   • Total Volume: ${total_value:,.2f}")
            logger.info(f"   • Average Value: ${avg_value:,.2f}")
            logger.info(f"   • Largest Tx: ${max_tx_value:,.2f}")
            logger.info(f"   • 30-Day Volume: ${volume_30d:,.2f}")
            logger.info(f"   • 7-Day Volume: ${volume_7d:,.2f}")
        
        labels = None
        if include_labels:
            logger.info(f"🏷️  Fetching wallet labels...")
            labels = labeling.get_wallet_labels(address)
            
            if labels and labels.get('entity_type') != 'unknown':
                logger.info(f"🏢 Entity Identified:")
                logger.info(f"   • Type: {labels.get('entity_type')}")
                logger.info(f"   • Name: {labels.get('entity_name', 'N/A')}")
        
        logger.info(f"📊 Building wallet profile...")
        profile = profiler.create_profile(address, transactions, labels)
        
        # Add calculated USD values
        profile['lifetime_volume'] = total_value
        profile['volume_30d'] = volume_30d
        profile['volume_7d'] = volume_7d
        profile['avg_transfer'] = avg_value
        profile['max_transfer'] = max_tx_value
        profile['enriched_transaction_count'] = len(enriched_txs)
        profile['total_transaction_count'] = len(transactions)
        
        otc_probability = profiler.calculate_otc_probability(profile)
        profile['otc_probability'] = otc_probability
        
        logger.info(f"🎯 OTC Probability: {otc_probability:.2%}")
        
        if include_network_metrics and len(transactions) > 0:
            logger.info(f"🕸️  Calculating network metrics...")
            network_analyzer = NetworkAnalysisService()
            network_analyzer.build_graph(transactions)
            network_metrics = network_analyzer.analyze_wallet_centrality(address)
            
            profile['network_metrics'] = network_metrics
        
        cache.cache_wallet_profile(address, profile)
        
        logger.info(f"✅ Profile complete - OTC probability: {otc_probability:.2%}")
        
        return {
            "success": True,
            "data": profile,
            "cached": False
        }
        
    except Exception as e:
        logger.error(f"❌ Profile fetch failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{address}/details")
async def get_wallet_details(
    address: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed wallet information with LIVE Etherscan data.
    
    GET /api/otc/wallet/0x.../details
    
    ✅ IF confidence_score >= 80%: Update DB with real data
    ⚠️ IF confidence_score < 80%: Only show, don't update DB
    """
    try:
        from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
        from app.core.otc_analysis.detection.wallet_profiler import WalletDetailsService
        
        logger.info(f"👤 GET /wallet/{address[:10]}.../details")
        
        # Get wallet from DB
        wallet = db.query(OTCWallet).filter(
            OTCWallet.address == address
        ).first()
        
        if not wallet:
            raise HTTPException(status_code=404, detail="Wallet not found")
        
        # Delegate to service
        details = await WalletDetailsService.get_wallet_details(
            address=address,
            wallet=wallet,
            db=db
        )
        
        return details
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in /wallet/details: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
