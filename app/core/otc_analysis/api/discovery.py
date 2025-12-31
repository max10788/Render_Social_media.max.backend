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

@router.post("/discover/simple")
async def simple_discovery(
    otc_address: str = Query(..., description="OTC Desk Adresse"),
    num_transactions: int = Query(5, ge=1, le=20, description="Anzahl Transaktionen"),
    db: Session = Depends(get_db)
) -> Dict:
    """
    🔍 Simple Discovery: Analysiere letzte N Transaktionen.
    
    Schritte:
    1. Hole letzte 5 Transaktionen vom OTC Desk
    2. Extrahiere Counterparty-Adressen (from/to)
    3. Analysiere jede Counterparty
    4. Speichere wenn OTC-Score >= 60%
    """
    from app.core.otc_analysis.api.dependencies import discover_from_last_5_transactions
    
    logger.info(f"🔍 Simple Discovery: {otc_address[:10]}... last {num_transactions} TXs")
    
    try:
        discovered = await discover_from_last_5_transactions(
            db=db,
            otc_address=otc_address,
            num_transactions=num_transactions
        )
        
        return {
            "success": True,
            "otc_address": otc_address,
            "transactions_analyzed": num_transactions,
            "discovered_count": len(discovered),
            "wallets": discovered,
            "message": f"Analyzed last {num_transactions} transactions, found {len(discovered)} new OTC desks"
        }
        
    except Exception as e:
        logger.error(f"❌ Simple discovery error: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

@router.get("/debug/transactions")
async def debug_transactions(
    otc_address: str = Query(...),
    limit: int = Query(5, ge=1, le=20)
) -> Dict:
    """🐛 DEBUG: Show raw transaction data"""
    from datetime import datetime
    
    # ✅ Import die GLOBALEN Singletons direkt
    import app.core.otc_analysis.api.dependencies as deps
    
    try:
        # ✅ Nutze das bereits initialisierte transaction_extractor Objekt
        transactions = deps.transaction_extractor.extract_wallet_transactions(
            otc_address,
            include_internal=True,
            include_tokens=True
        )
        
        if not transactions:
            return {
                "success": False,
                "error": "No transactions found",
                "otc_address": otc_address
            }
        
        # Sortiere und nimm letzte N
        recent = sorted(
            transactions, 
            key=lambda x: x.get('timestamp', datetime.min), 
            reverse=True
        )[:limit]
        
        # Normalisiere OTC Adresse
        otc_lower = otc_address.lower().strip()
        
        return {
            "success": True,
            "otc_address": otc_address,
            "otc_lower": otc_lower,
            "total_transactions": len(transactions),
            "transactions": [
                {
                    "num": i,
                    "hash": tx.get('hash', 'N/A')[:20] + "...",
                    "from": tx.get('from', 'N/A'),
                    "from_lower": str(tx.get('from', '')).lower(),
                    "to": tx.get('to', 'N/A'),
                    "to_lower": str(tx.get('to', '')).lower(),
                    "contract": tx.get('contractAddress', 'N/A'),
                    "symbol": tx.get('tokenSymbol', 'ETH'),
                    "value": tx.get('value', 0),
                    "usd_value": tx.get('usd_value', 0),
                    "timestamp": str(tx.get('timestamp', 'N/A')),
                    "matches_from": str(tx.get('from', '')).lower() == otc_lower,
                    "matches_to": str(tx.get('to', '')).lower() == otc_lower
                }
                for i, tx in enumerate(recent, 1)
            ]
        }
        
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }
