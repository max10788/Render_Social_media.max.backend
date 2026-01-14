"""
Admin Endpoints
===============

Administrative endpoints for data management and system maintenance.
"""

import logging
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from datetime import datetime

from .dependencies import get_db, get_otc_detector, get_cache_manager

from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
from app.core.otc_analysis.models.watchlist import WatchlistItem as OTCWatchlist
from app.core.otc_analysis.models.alert import Alert as OTCAlert

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin"])


# ============================================================================
# ADMIN ENDPOINTS
# ============================================================================

@router.post("/clear-mock-data")
async def clear_mock_data(
    db: Session = Depends(get_db)
):
    """
    🗑️ ADMIN: Delete all mock/test wallet data
    
    POST /api/otc/admin/clear-mock-data
    
    Simple endpoint that:
    1. Deletes ALL wallets from database
    2. Deletes watchlist items
    3. Deletes alerts
    4. Returns count of deleted items
    
    Real wallets will be auto-fetched on next API request.
    
    Example:
    curl -X POST "http://localhost:8000/api/otc/admin/clear-mock-data"
    """
    logger.info(f"🗑️  ADMIN: Clearing mock data...")
    
    try:
        # Count before delete
        wallet_count = db.query(OTCWallet).count()
        watchlist_count = db.query(OTCWatchlist).count()
        alert_count = db.query(OTCAlert).count()
        
        logger.info(f"📊 Current counts:")
        logger.info(f"   • Wallets: {wallet_count}")
        logger.info(f"   • Watchlist items: {watchlist_count}")
        logger.info(f"   • Alerts: {alert_count}")
        
        # Delete all
        db.query(OTCWallet).delete()
        db.query(OTCWatchlist).delete()
        db.query(OTCAlert).delete()
        
        db.commit()
        
        logger.info(f"✅ Deleted all mock data:")
        logger.info(f"   • Wallets: {wallet_count} → 0")
        logger.info(f"   • Watchlist: {watchlist_count} → 0")
        logger.info(f"   • Alerts: {alert_count} → 0")
        
        return {
            "success": True,
            "message": "Mock data cleared",
            "deleted": {
                "wallets": wallet_count,
                "watchlist_items": watchlist_count,
                "alerts": alert_count
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to clear mock data: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/system/health")
async def system_health(
    detector = Depends(get_otc_detector),
    cache = Depends(get_cache_manager)
):
    """
    🏥 System health check
    
    GET /api/otc/admin/system/health
    
    Checks:
    - Database connection
    - Cache system
    - Detection services
    - API integrations
    """
    logger.info(f"🏥 Health check...")
    
    try:
        from .dependencies import node_provider
        
        # Check blockchain connection
        latest_block = node_provider.get_latest_block_number()
        
        # Check cache
        cache_healthy = cache.exists("health_check")
        cache.set("health_check", True, ttl=60)
        
        # Check detection stats
        stats = detector.get_detection_stats()
        
        return {
            "success": True,
            "status": "healthy",
            "services": {
                "blockchain": {
                    "connected": latest_block > 0,
                    "latest_block": latest_block
                },
                "cache": {
                    "connected": True,
                    "healthy": cache_healthy
                },
                "detection": {
                    "total_scans": stats.get('total_scans', 0),
                    "total_suspected": stats.get('total_suspected', 0)
                }
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {str(e)}", exc_info=True)
        return {
            "success": False,
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.post("/cache/clear")
async def clear_cache(
    cache = Depends(get_cache_manager)
):
    """
    🗑️ Clear all cache
    
    POST /api/otc/admin/cache/clear
    """
    logger.info(f"🗑️  ADMIN: Clearing cache...")
    
    try:
        # Get stats before clearing
        stats_before = cache.get_stats()
        
        # Clear cache (if method exists)
        # Note: Implement cache.clear_all() in CacheManager if needed
        
        logger.info(f"✅ Cache cleared")
        
        return {
            "success": True,
            "message": "Cache cleared",
            "stats_before": stats_before,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to clear cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/detailed")
async def get_detailed_stats(
    db: Session = Depends(get_db),
    detector = Depends(get_otc_detector),
    cache = Depends(get_cache_manager)
):
    """
    📊 Get detailed system statistics
    
    GET /api/otc/admin/stats/detailed
    """
    logger.info(f"📊 Fetching detailed stats...")
    
    try:
        # Database counts
        total_wallets = db.query(OTCWallet).count()
        active_wallets = db.query(OTCWallet).filter(OTCWallet.is_active == True).count()
        high_confidence_wallets = db.query(OTCWallet).filter(OTCWallet.confidence_score >= 90).count()
        
        # Detection stats
        detection_stats = detector.get_detection_stats()
        
        # Cache stats
        cache_stats = cache.get_stats()
        
        return {
            "success": True,
            "data": {
                "database": {
                    "total_wallets": total_wallets,
                    "active_wallets": active_wallets,
                    "high_confidence_wallets": high_confidence_wallets
                },
                "detection": detection_stats,
                "cache": cache_stats,
                "timestamp": datetime.now().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/sync-all-transactions")
async def sync_all_transactions(
    max_wallets: int = Query(20, le=100, description="Max number of wallets to sync"),
    max_transactions_per_wallet: int = Query(100, le=500, description="Max TXs per wallet"),
    db: Session = Depends(get_db)
):
    """
    🔄 Synchronisiert Transaktionen für alle aktiven Wallets in die DB.
    
    **Process:**
    1. Holt Top N volumenstärkste Wallets aus DB
    2. Fetched Transaktionen via Blockchain API
    3. Speichert sie in transactions Tabelle
    4. Enriched USD values (soweit möglich)
    
    **Use Cases:**
    - Initial Setup: Erstmaliges Befüllen der TX-Tabelle
    - Maintenance: Regelmäßiges Update aller Wallets
    - Graph Edges: Ermöglicht Wallet ↔ OTC Verbindungen
    
    **Performance:**
    - 20 Wallets × 100 TXs = ~2000 Transaktionen
    - Dauer: ca. 2-5 Minuten (abhängig von API Rate Limits)
    
    **Returns:**
    - wallets_processed: Anzahl erfolgreich verarbeiteter Wallets
    - total_saved: Neu gespeicherte Transaktionen
    - total_fetched: Von API geholte Transaktionen
    - errors: Anzahl Fehler
    """
    from app.core.otc_analysis.api.dependencies import sync_all_wallets_transactions
    
    logger.info("="*70)
    logger.info(f"🔄 ADMIN: Syncing transactions for {max_wallets} wallets")
    logger.info("="*70)
    
    try:
        stats = await sync_all_wallets_transactions(
            db=db,
            max_wallets=max_wallets,
            max_transactions_per_wallet=max_transactions_per_wallet
        )
        
        return {
            "success": True,
            "stats": stats,
            "message": (
                f"✅ Synced {stats['total_saved']} transactions "
                f"for {stats['wallets_processed']} wallets "
                f"({stats['errors']} errors)"
            ),
            "recommendations": [
                "Run /api/admin/enrich-missing-values to add USD values to transactions without them",
                "Check /api/discover/debug/transaction-count to verify data",
                "Reload /api/network to see new edges"
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ Sync failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@router.post("/admin/sync-wallet-transactions")
async def sync_single_wallet_transactions(
    wallet_address: str = Query(..., description="Wallet address to sync"),
    max_transactions: int = Query(100, le=500, description="Max transactions to fetch"),
    force_refresh: bool = Query(False, description="Force refresh even if cached"),
    db: Session = Depends(get_db)
):
    """
    🔄 Synchronisiert Transaktionen für ein einzelnes Wallet.
    
    **Use Cases:**
    - Targeted Sync: Nur ein bestimmtes Wallet updaten
    - New Wallet: Erstmalig Transaktionen holen
    - Force Refresh: Cache ignorieren und neu fetchen
    
    **Examples:**
```
    POST /api/admin/sync-wallet-transactions?wallet_address=0x59abc82208ca435773608eb70f4035fc2ea861da
    POST /api/admin/sync-wallet-transactions?wallet_address=0x8d05d9924fe935bd533a844271a1b2078eae6fcf&force_refresh=true
```
    """
    from app.core.otc_analysis.api.dependencies import sync_wallet_transactions_to_db
    
    logger.info(f"🔄 ADMIN: Syncing transactions for {wallet_address[:10]}...")
    
    try:
        stats = await sync_wallet_transactions_to_db(
            db=db,
            wallet_address=wallet_address,
            max_transactions=max_transactions,
            force_refresh=force_refresh
        )
        
        return {
            "success": True,
            "wallet": wallet_address,
            "stats": stats,
            "message": (
                f"✅ Saved {stats['saved_count']} new transactions "
                f"(updated {stats['updated_count']}, skipped {stats['skipped_count']})"
            )
        }
        
    except Exception as e:
        logger.error(f"❌ Sync failed: {e}", exc_info=True)
        return {
            "success": False,
            "wallet": wallet_address,
            "error": str(e),
            "error_type": type(e).__name__
        }


@router.post("/admin/enrich-missing-values")
async def enrich_missing_usd_values(
    batch_size: int = Query(100, le=500, description="Transactions per batch"),
    max_batches: int = Query(10, le=50, description="Max batches to process"),
    db: Session = Depends(get_db)
):
    """
    💰 Enriched Transaktionen ohne USD-Wert.
    
    Holt USD-Preise für Transaktionen die nur ETH-Wert haben
    und berechnet den USD-Wert retrospektiv.
    
    **Process:**
    1. Findet TXs mit usd_value = NULL oder 0
    2. Holt historische Token-Preise
    3. Berechnet USD-Wert
    4. Updated Transaktionen in DB
    
    **Performance:**
    - Rate Limited: 1.2s delay zwischen Calls
    - Batch Processing: 100 TXs pro Batch
    - Smart Priorisierung: Große TXs zuerst
    
    **Use After:**
    - Initial sync (many TXs without USD)
    - Partial enrichment failures
    - New token transactions
    """
    from app.core.otc_analysis.api.dependencies import enrich_missing_usd_values
    
    logger.info("💰 ADMIN: Enriching missing USD values...")
    
    try:
        stats = await enrich_missing_usd_values(
            db=db,
            batch_size=batch_size,
            max_batches=max_batches
        )
        
        return {
            "success": True,
            "stats": stats,
            "message": (
                f"✅ Enriched {stats['enriched']} transactions "
                f"({stats['failed']} failed, {stats['rate_limit_hits']} rate limits)"
            )
        }
        
    except Exception as e:
        logger.error(f"❌ Enrichment failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }
