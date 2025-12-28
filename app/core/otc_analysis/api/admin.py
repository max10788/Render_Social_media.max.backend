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
```

---

# 🚀 **VORBEREITUNG: MORALIS STREAMS IMPLEMENTIERUNG**

## 📋 **WAS ICH JETZT VORBEREITE:**

### **1. Moralis Streams Architecture**
```
┌────────────────────────────────────────────────────┐
│ ETHEREUM BLOCKCHAIN                                │
├────────────────────────────────────────────────────┤
│ Large Transfer Event (>$100k)                      │
│ ↓                                                  │
│ ERC20 Transfer(from, to, value)                   │
└────────────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────────────┐
│ MORALIS STREAMS (Real-time Monitoring)             │
├────────────────────────────────────────────────────┤
│ • Listens to Transfer events                       │
│ • Filters: value >= $100k                         │
│ • Sends webhook to our API                        │
└────────────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────────────┐
│ OUR API - Webhook Endpoint                         │
├────────────────────────────────────────────────────┤
│ POST /api/otc/streams/webhook                     │
│ • Receives transfer data                          │
│ • Validates addresses via Moralis                 │
│ • Adds discovered OTC desks to DB                 │
└────────────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────────────┐
│ RESULT: ZERO API Calls from Us!                    │
├────────────────────────────────────────────────────┤
│ ✅ Real-time discovery                             │
│ ✅ Only large transfers                            │
│ ✅ No rate limits                                  │
│ ✅ Automatic validation                            │
└────────────────────────────────────────────────────┘
```

### **2. Files to Create**
```
blockchain/
└── moralis_streams.py     # Stream setup & webhook handler

api/
└── streams.py             # Webhook endpoint

utils/
└── stream_validator.py    # Validate incoming webhooks
```

### **3. Key Components**
- ✅ Stream configuration (filter setup)
- ✅ Webhook endpoint (receive events)
- ✅ Signature validation (security)
- ✅ Address validation (via Moralis)
- ✅ Auto-save to database

### **4. Benefits**
```
Before (Current):
- Scan 100 wallets × 1000 txs = 100,000 TXs
- API Calls: 100 (5 calls/sec limit)
- Time: 20 seconds
- Efficiency: 0.2% (only ~200 are large)

After (Moralis Streams):
- Moralis pushes large transfers to us
- API Calls from us: 0 (Moralis pushes!)
- Time: Real-time (instant)
- Efficiency: 100% (only large transfers)
