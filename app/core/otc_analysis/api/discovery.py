"""
OTC Discovery API Endpoints
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Dict
import logging

from app.core.backend_crypto_tracker.config.database import get_db
from app.core.otc_analysis.api.dependencies import discover_new_otc_desks, discover_from_last_5_transactions

router = APIRouter(tags=["Discovery"])
logger = logging.getLogger(__name__)


@router.post("/discover")
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


@router.get("/candidates")
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
    filter_known_entities: bool = Query(
        True, 
        description="Filtere bekannte Exchanges/Protocols (Binance, Uniswap, etc.) via Moralis Labels"
    ),
    db: Session = Depends(get_db)
) -> Dict:
    """
    🔍 Simple Discovery: Analysiere letzte N Transaktionen.
    
    ✅ NEW: Moralis Label-basierte Filterung
    
    Schritte:
    1. Hole letzte N Transaktionen vom OTC Desk (mit Moralis Labels)
    2. Extrahiere Counterparty-Adressen (from/to)
    3. **Filtere bekannte Exchanges/Protocols** (wenn filter_known_entities=True)
    4. Analysiere verbleibende Counterparties
    5. Speichere wenn OTC-Score >= 50%
    
    **Moralis Filter entfernt:**
    - Binance, Coinbase, Kraken, Gemini, etc.
    - Uniswap, 1inch, SushiSwap, Curve, etc.
    - MEV Bots, Flashbots
    - Bridge Protocols
    - Lending Protocols (Aave, Compound, etc.)
    """
    logger.info(
        f"🔍 Simple Discovery: {otc_address[:10]}... "
        f"last {num_transactions} TXs (filter={filter_known_entities})"
    )
    
    try:
        discovered = await discover_from_last_5_transactions(
            db=db,
            otc_address=otc_address,
            num_transactions=num_transactions,
            filter_known_entities=filter_known_entities
        )
        
        # Build detailed response
        wallets_response = []
        
        for wallet in discovered:
            wallet_data = {
                "address": wallet["address"],
                "confidence": wallet["confidence"],
                "volume": wallet["volume"],
                "tx_count": wallet["tx_count"],
                "discovery_breakdown": wallet["discovery_breakdown"]
            }
            
            # ✅ Include Moralis labels if available
            if wallet.get("moralis_label"):
                wallet_data["moralis_label"] = wallet["moralis_label"]
            if wallet.get("moralis_entity"):
                wallet_data["moralis_entity"] = wallet["moralis_entity"]
            
            wallets_response.append(wallet_data)
        
        return {
            "success": True,
            "otc_address": otc_address,
            "transactions_analyzed": num_transactions,
            "filter_enabled": filter_known_entities,
            "discovered_count": len(discovered),
            "wallets": wallets_response,
            "message": (
                f"Analyzed last {num_transactions} transactions, "
                f"found {len(discovered)} new OTC desks "
                f"({'with' if filter_known_entities else 'without'} known entity filtering)"
            )
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
    """
    🐛 DEBUG: Show ALL transaction fields including Moralis labels
    """
    from datetime import datetime
    import app.core.otc_analysis.api.dependencies as deps
    
    try:
        transactions = deps.transaction_extractor.extract_wallet_transactions(
            otc_address,
            include_internal=True,
            include_tokens=True
        )
        
        if not transactions:
            return {
                "success": False,
                "error": "No transactions found"
            }
        
        recent = sorted(
            transactions, 
            key=lambda x: x.get('timestamp', datetime.min), 
            reverse=True
        )[:limit]
        
        first_tx = recent[0] if recent else {}
        
        return {
            "success": True,
            "otc_address": otc_address,
            "total_transactions": len(transactions),
            "source": first_tx.get('source', 'unknown'),  # 'moralis' or 'etherscan'
            "first_tx_keys": list(first_tx.keys()),
            "moralis_enabled": deps.transaction_extractor.use_moralis,
            "transactions": [
                {
                    "num": i,
                    "hash": tx.get('tx_hash', 'N/A')[:16],
                    "from": tx.get('from_address', 'N/A')[:10],
                    "to": tx.get('to_address', 'N/A')[:10],
                    "value_eth": tx.get('value_decimal', 0),
                    "token": tx.get('token_symbol', 'ETH'),
                    "source": tx.get('source', 'unknown'),
                    # ✅ Moralis Labels
                    "to_label": tx.get('to_address_label'),
                    "from_label": tx.get('from_address_label'),
                    "to_entity": tx.get('to_address_entity'),
                    "from_entity": tx.get('from_address_entity'),
                    "to_is_known": tx.get('to_is_known_entity', False),
                    "from_is_known": tx.get('from_is_known_entity', False)
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
