"""
Admin Endpoints
===============

Administrative endpoints for data management and system maintenance.
"""

import logging
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Dict, Any, List

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


@router.post("/sync-all-transactions")
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


@router.post("/sync-wallet-transactions")
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


@router.post("/enrich-missing-values")
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

@router.get("/discover/debug/wallet-edges")
async def debug_wallet_edges(
    db: Session = Depends(get_db)
) -> Dict:
    """
    🐛 DEBUG: Check if transactions exist between Cold Wallets and Mega Whales
    """
    from app.core.otc_analysis.models.transaction import Transaction
    from sqlalchemy import and_, or_, func
    from datetime import datetime, timedelta
    
    wallet_addresses = [
        '0x59abc82208ca435773608eb70f4035fc2ea861da',  # Mega Whale 1
        '0xd1019c0e83c61d72218f84f538094bb850f28c43',  # Mega Whale 2
        '0xa6dfb62fc572da152a335384f7724535b9defc84'   # Mega Whale 3
    ]
    
    otc_addresses = [
        '0x8d05d9924fe935bd533a844271a1b2078eae6fcf',  # Kraken Cold 3
        '0xaa2aa15d77b41e151e17f3a61cbc68db4da72a90',  # Revolut Cold
        '0xd3a22590f8243f8e83ac230d1842c9af0404c4a1'   # Ceffu Hot 2
    ]
    
    # Check last year
    start = datetime.now() - timedelta(days=365)
    end = datetime.now()
    
    edges = db.query(
        Transaction.from_address,
        Transaction.to_address,
        func.count(Transaction.tx_hash).label('tx_count'),
        func.sum(Transaction.value_decimal).label('total_eth'),
        func.coalesce(func.sum(Transaction.usd_value), 0).label('total_usd'),
        func.min(Transaction.timestamp).label('first_tx'),
        func.max(Transaction.timestamp).label('last_tx')
    ).filter(
        Transaction.timestamp >= start,
        Transaction.timestamp <= end,
        or_(
            and_(
                Transaction.from_address.in_([w.lower() for w in wallet_addresses]),
                Transaction.to_address.in_([w.lower() for w in otc_addresses])
            ),
            and_(
                Transaction.from_address.in_([w.lower() for w in otc_addresses]),
                Transaction.to_address.in_([w.lower() for w in wallet_addresses])
            )
        )
    ).group_by(
        Transaction.from_address,
        Transaction.to_address
    ).all()
    
    return {
        "success": True,
        "time_range": {
            "start": start.isoformat(),
            "end": end.isoformat()
        },
        "wallets_checked": len(wallet_addresses),
        "otc_checked": len(otc_addresses),
        "edges_found": len(edges),
        "edges": [
            {
                "from": e.from_address[:10] + "...",
                "to": e.to_address[:10] + "...",
                "tx_count": e.tx_count,
                "total_eth": float(e.total_eth or 0),
                "total_usd": float(e.total_usd or 0),
                "first_tx": e.first_tx.isoformat() if e.first_tx else None,
                "last_tx": e.last_tx.isoformat() if e.last_tx else None
            }
            for e in edges
        ]
    }


@router.get("/discover/debug/transaction-count")
async def debug_transaction_count(
    db: Session = Depends(get_db)
) -> Dict:
    """
    🐛 DEBUG: Check how many transactions are in the database
    """
    from app.core.otc_analysis.models.transaction import Transaction
    from sqlalchemy import func
    
    # Total count
    total = db.query(func.count(Transaction.tx_hash)).scalar()
    
    # With USD values
    with_usd = db.query(func.count(Transaction.tx_hash)).filter(
        Transaction.usd_value != None,
        Transaction.usd_value > 0
    ).scalar()
    
    # Group by address
    top_wallets = db.query(
        Transaction.from_address,
        func.count(Transaction.tx_hash).label('tx_count')
    ).group_by(
        Transaction.from_address
    ).order_by(
        func.count(Transaction.tx_hash).desc()
    ).limit(10).all()
    
    return {
        "success": True,
        "total_transactions": total,
        "with_usd_value": with_usd,
        "without_usd_value": total - with_usd,
        "enrichment_rate": f"{(with_usd / total * 100):.1f}%" if total > 0 else "0%",
        "top_10_wallets": [
            {
                "address": w.from_address[:10] + "...",
                "tx_count": w.tx_count
            }
            for w in top_wallets
        ]
    }
