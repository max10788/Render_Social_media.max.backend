"""
OTC Discovery API Endpoints
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Dict
import logging

from app.core.backend_crypto_tracker.config.database import get_db
from app.core.otc_analysis.api.dependencies import discover_new_otc_desks

router = APIRouter(tags=["Discovery"])
logger = logging.getLogger(__name__)


@router.post("/discover")  # ✅ /discover statt /discovery/discover
async def trigger_discovery(
    max_discoveries: int = Query(5, ge=1, le=20),
    db: Session = Depends(get_db)
) -> Dict:
    """
    🕵️ Discover new OTC desks via counterparty analysis.
    """
    logger.info(f"🔍 Discovery triggered (max: {max_discoveries})")
    
    try:
        discovered = await discover_new_otc_desks(db, max_discoveries)
        
        return {
            "success": True,
            "discovered_count": len(discovered),
            "candidates": discovered,
            "message": f"Found {len(discovered)} new OTC desks"
        }
        
    except Exception as e:
        logger.error(f"❌ Discovery error: {e}", exc_info=True)
        return {
            "success": False,
            "discovered_count": 0,
            "candidates": [],
            "error": str(e)
        }


@router.get("/candidates")  # ✅ /candidates statt /discovery/candidates
async def get_candidates(
    min_confidence: float = Query(60.0, ge=0, le=100),
    db: Session = Depends(get_db)
) -> Dict:
    """Get all discovered OTC desk candidates."""
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    
    try:
        candidates = db.query(OTCWallet).filter(
            OTCWallet.tags.contains(['discovered']),
            OTCWallet.confidence_score >= min_confidence
        ).all()
        
        return {
            "success": True,
            "count": len(candidates),
            "candidates": [
                {
                    "address": w.address,
                    "entity_name": w.entity_name,
                    "confidence": w.confidence_score,
                    "volume": w.total_volume,
                    "tags": w.tags,
                    "discovered_at": w.created_at
                }
                for w in candidates
            ]
        }
    except Exception as e:
        logger.error(f"❌ Error getting candidates: {e}", exc_info=True)
        return {
            "success": False,
            "count": 0,
            "candidates": [],
            "error": str(e)
        }
