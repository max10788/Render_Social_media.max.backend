"""
Monitoring Endpoints
====================

Watchlist and alert management endpoints.
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime

from .dependencies import get_db

from app.core.otc_analysis.models.watchlist import WatchlistItem as OTCWatchlist
from app.core.otc_analysis.models.alert import Alert as OTCAlert

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["Monitoring"])


# ============================================================================
# WATCHLIST ENDPOINTS
# ============================================================================

@router.get("/watchlist")
async def get_watchlist(
    user_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get user's watchlist.
    
    GET /api/otc/watchlist?user_id=dev_user_123
    """
    try:
        if not user_id:
            logger.info(f"📋 GET /watchlist: No user_id, returning empty")
            return {
                "items": [],
                "message": "No user authenticated"
            }
        
        logger.info(f"📋 GET /watchlist for user {user_id[:20]}...")
        
        items = db.query(OTCWatchlist).filter(
            OTCWatchlist.user_id == user_id
        ).all()
        
        logger.info(f"✅ Found {len(items)} watchlist items")
        
        return {
            "items": [
                {
                    "id": str(item.id),
                    "wallet_address": item.wallet_address,
                    "notes": item.notes,
                    "alert_enabled": item.alert_enabled,
                    "alert_threshold": item.alert_threshold,
                    "created_at": item.created_at.isoformat() if item.created_at else None
                }
                for item in items
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /watchlist: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/watchlist")
async def add_to_watchlist(
    user_id: str = Query(...),
    wallet_address: str = Query(...),
    notes: Optional[str] = Query(None),
    alert_threshold: Optional[float] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Add wallet to watchlist.
    
    POST /api/otc/watchlist?user_id=dev_user_123&wallet_address=0x...&notes=Test
    """
    try:
        logger.info(f"➕ Adding to watchlist: {wallet_address[:10]}...")
        
        # Check if already exists
        existing = db.query(OTCWatchlist).filter(
            OTCWatchlist.user_id == user_id,
            OTCWatchlist.wallet_address == wallet_address
        ).first()
        
        if existing:
            logger.warning(f"⚠️  Wallet already in watchlist")
            raise HTTPException(status_code=400, detail="Wallet already in watchlist")
        
        # Create new watchlist item
        item = OTCWatchlist(
            user_id=user_id,
            wallet_address=wallet_address,
            notes=notes,
            alert_enabled=alert_threshold is not None,
            alert_threshold=alert_threshold
        )
        
        db.add(item)
        db.commit()
        db.refresh(item)
        
        logger.info(f"✅ Added to watchlist: ID {item.id}")
        
        return {
            "id": str(item.id),
            "wallet_address": item.wallet_address,
            "created_at": item.created_at.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error adding to watchlist: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/watchlist/{item_id}")
async def remove_from_watchlist(
    item_id: str,
    db: Session = Depends(get_db)
):
    """
    Remove wallet from watchlist.
    
    DELETE /api/otc/watchlist/123
    """
    try:
        item = db.query(OTCWatchlist).filter(OTCWatchlist.id == item_id).first()
        
        if not item:
            raise HTTPException(status_code=404, detail="Watchlist item not found")
        
        db.delete(item)
        db.commit()
        
        return {"message": "Removed from watchlist"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error removing from watchlist: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ALERT ENDPOINTS
# ============================================================================

@router.get("/alerts")
async def get_alerts(
    user_id: str = Query(...),
    unread_only: bool = Query(False),
    db: Session = Depends(get_db)
):
    """
    Get user's alerts.
    
    GET /api/otc/alerts?user_id=dev_user_123&unread_only=false
    """
    try:
        query = db.query(OTCAlert).filter(OTCAlert.user_id == user_id)
        
        if unread_only:
            query = query.filter(OTCAlert.is_read == False)
        
        alerts = query.order_by(OTCAlert.created_at.desc()).limit(100).all()
        
        return {
            "alerts": [
                {
                    "id": str(a.id),
                    "alert_type": a.alert_type,
                    "wallet_address": a.wallet_address,
                    "message": a.message,
                    "severity": a.severity,
                    "is_read": a.is_read,
                    "created_at": a.created_at.isoformat() if a.created_at else None
                }
                for a in alerts
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))
