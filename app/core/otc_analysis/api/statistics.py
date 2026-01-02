"""
Statistics Endpoints - Simplified URLs
=======================================

Endpoints for OTC statistics and analytics.
Frontend expects: /api/otc/distributions
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime, timedelta

from .dependencies import (
    get_db,
    get_otc_detector,
    get_cache_manager,
    ensure_registry_wallets_in_db
)

from app.core.otc_analysis.models.wallet import Wallet as OTCWallet

logger = logging.getLogger(__name__)

# ✅ Simplified router - no prefix
router = APIRouter(prefix="", tags=["Statistics"])


# ============================================================================
# STATISTICS ENDPOINTS - SIMPLIFIED URLS
# ============================================================================

@router.get("/statistics")
async def get_statistics(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get OTC statistics with 24h change calculations.
    
    GET /api/otc/statistics?start_date=2024-11-21&end_date=2024-12-21
    """
    try:
        # AUTO-SYNC
        await ensure_registry_wallets_in_db(db, max_to_fetch=3)
        
        # Parse dates
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"📊 GET /statistics: {start.date()} to {end.date()}")
        
        # Current period wallets
        current_wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).all()
        
        # 24h ago period (for comparison)
        start_24h = start - timedelta(days=1)
        end_24h = end - timedelta(days=1)
        
        previous_wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start_24h,
            OTCWallet.last_active <= end_24h
        ).all()
        
        # Calculate current statistics
        current_volume = sum(w.total_volume or 0 for w in current_wallets)
        current_count = sum(w.transaction_count or 0 for w in current_wallets)
        current_avg_size = current_volume / current_count if current_count > 0 else 0
        current_avg_confidence = (
            sum(w.confidence_score or 0 for w in current_wallets) / len(current_wallets)
            if current_wallets else 0
        )
        
        # Calculate previous statistics
        previous_volume = sum(w.total_volume or 0 for w in previous_wallets)
        previous_count = sum(w.transaction_count or 0 for w in previous_wallets)
        previous_avg_size = previous_volume / previous_count if previous_count > 0 else 0
        previous_avg_confidence = (
            sum(w.confidence_score or 0 for w in previous_wallets) / len(previous_wallets)
            if previous_wallets else 0
        )
        
        # Calculate percentage changes
        def calculate_change(current, previous):
            if previous == 0:
                return 0 if current == 0 else 100
            return ((current - previous) / previous) * 100
        
        volume_change = calculate_change(current_volume, previous_volume)
        wallets_change = calculate_change(len(current_wallets), len(previous_wallets))
        avg_size_change = calculate_change(current_avg_size, previous_avg_size)
        confidence_change = calculate_change(current_avg_confidence, previous_avg_confidence)
        
        logger.info(f"✅ Statistics: {len(current_wallets)} wallets, ${current_volume:,.0f}")
        
        return {
            "total_volume_usd": current_volume,
            "active_wallets": len(current_wallets),
            "total_transactions": current_count,
            "avg_transfer_size": current_avg_size,
            "volume_change_24h": round(volume_change, 2),
            "wallets_change_24h": round(wallets_change, 2),
            "avg_size_change_24h": round(avg_size_change, 2),
            "avg_confidence_score": round(current_avg_confidence, 1),
            "confidence_change_24h": round(confidence_change, 2),
            "last_updated": datetime.now().isoformat(),
            "period": {
                "start": start.isoformat(),
                "end": end.isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /statistics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/distributions")
async def get_distributions(
    startDate: Optional[str] = Query(None),
    endDate: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get distribution statistics.
    
    GET /api/otc/distributions?startDate=2025-12-03&endDate=2026-01-02
    """
    try:
        # Parse dates
        if startDate:
            start = datetime.fromisoformat(startDate.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if endDate:
            end = datetime.fromisoformat(endDate.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"📊 GET /distributions: {start.date()} to {end.date()}")
        
        # Get wallets
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).all()
        
        # Calculate entity type distribution
        entity_distribution = {}
        for w in wallets:
            entity_type = w.entity_type or "unknown"
            if entity_type not in entity_distribution:
                entity_distribution[entity_type] = {"count": 0, "volume": 0}
            entity_distribution[entity_type]["count"] += 1
            entity_distribution[entity_type]["volume"] += (w.total_volume or 0)
        
        # Calculate volume distribution (buckets)
        volume_buckets = {
            "0-1M": 0,
            "1M-10M": 0,
            "10M-100M": 0,
            "100M-1B": 0,
            "1B+": 0
        }
        
        for w in wallets:
            vol = w.total_volume or 0
            if vol < 1_000_000:
                volume_buckets["0-1M"] += 1
            elif vol < 10_000_000:
                volume_buckets["1M-10M"] += 1
            elif vol < 100_000_000:
                volume_buckets["10M-100M"] += 1
            elif vol < 1_000_000_000:
                volume_buckets["100M-1B"] += 1
            else:
                volume_buckets["1B+"] += 1
        
        logger.info(f"✅ Distributions: {len(entity_distribution)} entity types, {len(wallets)} wallets")
        
        return {
            "entity_distribution": entity_distribution,
            "volume_distribution": volume_buckets,
            "metadata": {
                "total_wallets": len(wallets),
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                }
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /distributions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_stats_old(
    detector = Depends(get_otc_detector),
    cache = Depends(get_cache_manager)
):
    """
    Get overall OTC detection statistics (legacy endpoint).
    
    GET /api/otc/stats
    """
    logger.info(f"📊 Fetching OTC stats (legacy)...")
    
    try:
        stats = detector.get_detection_stats()
        cache_stats = cache.get_stats()
        
        return {
            "success": True,
            "data": {
                "detection_stats": stats,
                "cache_stats": cache_stats,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch stats: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Export
__all__ = ["router"]
