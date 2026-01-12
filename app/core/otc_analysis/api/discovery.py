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


@router.post("/discover/high-volume")
async def discover_high_volume_wallets(
    source_address: str = Query(..., description="Source wallet address to analyze"),
    num_transactions: int = Query(5, ge=1, le=20, description="Number of recent transactions to analyze"),
    min_volume_threshold: float = Query(
        1_000_000, 
        ge=100_000, 
        le=1_000_000_000,
        description="Minimum USD volume threshold (default: $1M)"
    ),
    filter_known_entities: bool = Query(
        True, 
        description="Filter out known exchanges/protocols via Moralis labels"
    ),
    db: Session = Depends(get_db)
) -> Dict:
    """
    🔍 Discover high-volume wallets from recent transactions.
    
    ✅ VOLUME-FOCUSED DISCOVERY (not OTC-specific):
    
    **Process:**
    1. Extract counterparties from last N transactions (with Moralis labels)
    2. Filter known exchanges/protocols (optional)
    3. Analyze volume patterns using Moralis ERC20 transfers
    4. Score based on:
       - Total USD volume
       - Average transaction size
       - Transaction frequency
       - Token diversity
       - Large transfer count
    5. Save wallets meeting threshold with 'high_volume_wallet' entity type
    
    **Classifications:**
    - `mega_whale`: $100M+ volume, $1M+ avg transaction
    - `whale`: $10M+ volume, $500K+ avg transaction
    - `high_volume_trader`: $5M+ volume, 100+ transactions
    - `institutional`: $1M+ avg transaction, selective trading
    - `active_trader`: 200+ transactions, $2M+ volume
    - `moderate_volume`: Meets threshold ($1M+)
    
    **Scoring Breakdown (0-100):**
    - Total Volume: 0-30 points
    - Avg Transaction Size: 0-25 points
    - Transaction Frequency: 0-20 points
    - Token Diversity: 0-15 points
    - Large Transfers ($100K+): 0-10 points
    
    **Minimum Requirements:**
    - Volume Score: ≥40/100
    - Total Volume: ≥ min_volume_threshold
    
    **Example Response:**
    ```json
    {
      "success": true,
      "source_address": "0x...",
      "transactions_analyzed": 5,
      "min_volume_threshold": 1000000,
      "discovered_count": 2,
      "wallets": [
        {
          "address": "0x...",
          "volume_score": 75,
          "total_volume": 25000000,
          "classification": "whale",
          "tags": ["whale", "very_high_volume", "large_transactions"],
          "moralis_label": "Wallet Name",
          "volume_breakdown": {...}
        }
      ]
    }
    ```
    """
    logger.info(
        f"🔍 High Volume Discovery: {source_address[:10]}... "
        f"last {num_transactions} TXs (threshold: ${min_volume_threshold:,.0f})"
    )
    
    try:
        # Import here to avoid circular imports
        from app.core.otc_analysis.api.dependencies import discover_high_volume_from_transactions
        
        # Discover high-volume wallets
        discovered = await discover_high_volume_from_transactions(
            db=db,
            source_address=source_address,
            num_transactions=num_transactions,
            min_volume_threshold=min_volume_threshold,
            filter_known_entities=filter_known_entities
        )
        
        # Build detailed response
        wallets_response = []
        
        for wallet in discovered:
            wallet_data = {
                "address": wallet["address"],
                "volume_score": wallet["volume_score"],
                "total_volume": wallet["total_volume"],
                "tx_count": wallet["tx_count"],
                "avg_transaction": wallet["avg_transaction"],
                "classification": wallet["classification"],
                "tags": wallet["tags"],
                "volume_breakdown": wallet["volume_breakdown"]
            }
            
            # Include Moralis labels if available
            if wallet.get("moralis_label"):
                wallet_data["moralis_label"] = wallet["moralis_label"]
            if wallet.get("moralis_entity"):
                wallet_data["moralis_entity"] = wallet["moralis_entity"]
            
            # Include counterparty data
            if wallet.get("counterparty_data"):
                cp_data = wallet["counterparty_data"]
                wallet_data["counterparty_info"] = {
                    "interactions_with_source": cp_data.get("tx_count", 0),
                    "volume_with_source": cp_data.get("total_volume", 0),
                    "first_interaction": cp_data.get("first_seen"),
                    "last_interaction": cp_data.get("last_seen")
                }
            
            wallets_response.append(wallet_data)
        
        # Calculate summary stats
        if wallets_response:
            total_volume = sum(w["total_volume"] for w in wallets_response)
            avg_score = sum(w["volume_score"] for w in wallets_response) / len(wallets_response)
            
            # Count classifications
            classifications = {}
            for w in wallets_response:
                classification = w["classification"]
                classifications[classification] = classifications.get(classification, 0) + 1
            
            summary = {
                "total_volume_discovered": total_volume,
                "average_score": round(avg_score, 1),
                "classifications": classifications
            }
        else:
            summary = None
        
        return {
            "success": True,
            "source_address": source_address,
            "transactions_analyzed": num_transactions,
            "min_volume_threshold": min_volume_threshold,
            "filter_enabled": filter_known_entities,
            "discovered_count": len(discovered),
            "wallets": wallets_response,
            "summary": summary,
            "message": (
                f"Analyzed last {num_transactions} transactions, "
                f"found {len(discovered)} high-volume wallets "
                f"(threshold: ${min_volume_threshold:,.0f}, "
                f"filter: {'enabled' if filter_known_entities else 'disabled'})"
            )
        }
        
    except Exception as e:
        logger.error(f"❌ High volume discovery error: {e}", exc_info=True)
        return {
            "success": False,
            "source_address": source_address,
            "error": str(e),
            "error_type": type(e).__name__
        }


@router.get("/discover/high-volume/stats")
async def get_high_volume_stats(
    min_volume: float = Query(1_000_000, ge=0),
    db: Session = Depends(get_db)
) -> Dict:
    """
    📊 Get statistics about discovered high-volume wallets.
    
    Returns counts and metrics for wallets in the database with
    entity_type='high_volume_wallet'.
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from sqlalchemy import func
    
    try:
        # Query high-volume wallets
        query = db.query(OTCWallet).filter(
            OTCWallet.entity_type == 'high_volume_wallet',
            OTCWallet.total_volume >= min_volume
        )
        
        wallets = query.all()
        
        if not wallets:
            return {
                "success": True,
                "count": 0,
                "message": "No high-volume wallets found"
            }
        
        # Calculate stats
        total_volume = sum(w.total_volume or 0 for w in wallets)
        avg_volume = total_volume / len(wallets)
        avg_score = sum(w.confidence_score or 0 for w in wallets) / len(wallets)
        
        # Count by classification (from tags)
        classifications = {}
        for wallet in wallets:
            tags = wallet.tags or []
            # Find classification tag
            for tag in tags:
                if tag in ['mega_whale', 'whale', 'high_volume_trader', 
                          'institutional', 'active_trader', 'moderate_volume']:
                    classifications[tag] = classifications.get(tag, 0) + 1
                    break
        
        # Top wallets by volume
        top_wallets = sorted(wallets, key=lambda w: w.total_volume or 0, reverse=True)[:10]
        
        return {
            "success": True,
            "count": len(wallets),
            "min_volume_filter": min_volume,
            "statistics": {
                "total_volume": total_volume,
                "average_volume": avg_volume,
                "average_score": round(avg_score, 1),
                "classifications": classifications
            },
            "top_10_by_volume": [
                {
                    "address": w.address,
                    "label": w.label,
                    "volume": w.total_volume,
                    "score": w.confidence_score,
                    "tags": w.tags[:5] if w.tags else []
                }
                for w in top_wallets
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting high-volume stats: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

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
