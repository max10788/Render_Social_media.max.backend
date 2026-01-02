"""
Flow & Timeline Endpoints
==========================

Money flow tracing, Sankey diagrams, and transfer timelines.

This module handles all flow-related endpoints:
- Sankey flow visualization
- Transfer timeline events
- Money flow path tracing
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime, timedelta

from .dependencies import (
    get_db,
    get_transaction_extractor,
    get_price_oracle,
    flow_tracer,
    ensure_registry_wallets_in_db
)

from app.core.otc_analysis.api.validators import validate_ethereum_address, FlowTraceRequest
from app.core.otc_analysis.models.wallet import Wallet as OTCWallet

logger = logging.getLogger(__name__)

# Flow router - no prefix, URLs added via /api/otc in main.py
flow_router = APIRouter(prefix="", tags=["Flow"])


# ============================================================================
# FLOW ENDPOINTS
# ============================================================================

@flow_router.get("/sankey")
async def get_sankey_flow(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    min_flow_size: float = Query(100000),
    db: Session = Depends(get_db)
):
    """
    Get Sankey flow diagram data.
    
    GET /api/otc/sankey?start_date=2024-12-01&end_date=2024-12-28&min_flow_size=100000
    
    Returns:
        nodes: List of wallet nodes with volume data
        links: List of flow connections between wallets
        metadata: Period info and statistics
    
    The Sankey diagram visualizes money flows between wallets,
    with node size representing total volume and links showing
    transaction relationships.
    """
    try:
        # AUTO-SYNC: Ensure registry wallets are in database
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
        
        logger.info(f"💱 GET /sankey: {start.date()} to {end.date()}, min_flow=${min_flow_size:,.0f}")
        
        # Query wallets with significant volume in the time range
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end,
            OTCWallet.total_volume >= min_flow_size
        ).order_by(OTCWallet.total_volume.desc()).limit(20).all()
        
        # Format nodes for Sankey diagram
        nodes = [
            {
                "name": w.label or f"{w.address[:8]}...",
                "category": (w.entity_type or "Unknown").replace('_', ' ').title(),
                "value": float(w.total_volume or 0),
                "address": w.address
            }
            for w in wallets
        ]
        
        # Links will be populated when we implement transaction relationship tracking
        # For now, return empty array
        links = []
        
        logger.info(f"✅ Sankey: {len(nodes)} nodes, {len(links)} links")
        
        return {
            "nodes": nodes,
            "links": links,
            "metadata": {
                "node_count": len(nodes),
                "link_count": len(links),
                "min_flow_size": min_flow_size,
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                }
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /sankey: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@flow_router.get("/timeline")
async def get_transfer_timeline(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    min_confidence: float = Query(0),
    db: Session = Depends(get_db)
):
    """
    Get transfer timeline data.
    
    GET /api/otc/timeline?start_date=2024-12-17&end_date=2024-12-24&min_confidence=0.5
    
    Returns:
        events: Chronological list of transfer events
        metadata: Period info and event count
    
    Timeline shows wallet activity over time, useful for identifying
    patterns and anomalies in OTC transfer behavior.
    """
    try:
        # Parse dates
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=7)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"📅 GET /timeline: {start.date()} to {end.date()}, min_confidence={min_confidence}")
        
        # Query wallets in time range with minimum confidence score
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end,
            OTCWallet.confidence_score >= min_confidence
        ).order_by(OTCWallet.last_active.desc()).limit(50).all()
        
        # Format as timeline events
        events = [
            {
                "timestamp": w.last_active.isoformat() if w.last_active else datetime.now().isoformat(),
                "from_address": w.address,
                "to_address": None,  # Will be populated when we have transaction relationship data
                "amount_usd": float(w.total_volume or 0) / (w.transaction_count or 1),
                "token": "USDT",  # Default token, will be dynamic when we have token data
                "confidence_score": float(w.confidence_score or 0),
                "entity_type": w.entity_type,
                "label": w.label
            }
            for w in wallets
        ]
        
        logger.info(f"✅ Timeline: {len(events)} events")
        
        return {
            "events": events,
            "metadata": {
                "event_count": len(events),
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                }
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /timeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@flow_router.post("/flow/trace")
async def trace_flow(
    request: FlowTraceRequest,
    tx_extractor = Depends(get_transaction_extractor),
    oracle = Depends(get_price_oracle)
):
    """
    Trace money flow from source to target address.
    
    POST /api/otc/flow/trace
    
    Request Body:
        {
            "source_address": "0x...",
            "target_address": "0x...",
            "max_hops": 5,
            "min_confidence": 0.5
        }
    
    Returns:
        success: Boolean indicating if trace completed
        data: {
            path_exists: Boolean
            path_count: Number of paths found
            paths: List of transaction paths
            total_volume: Combined volume across paths
        }
    
    This endpoint analyzes blockchain transactions to find
    money flow paths between two addresses, useful for:
    - AML/KYC compliance
    - Transaction investigation
    - Wallet relationship mapping
    - OTC desk activity tracking
    """
    logger.info(f"🔄 Tracing flow: {request.source_address[:10]}... → {request.target_address[:10]}...")
    
    try:
        # Validate Ethereum addresses
        source = validate_ethereum_address(request.source_address)
        target = validate_ethereum_address(request.target_address)
        
        logger.info(f"📡 Fetching transaction data for both addresses...")
        
        # Extract transactions for source address
        source_txs = tx_extractor.extract_wallet_transactions(source)
        logger.info(f"   Source: {len(source_txs)} transactions")
        
        # Extract transactions for target address
        target_txs = tx_extractor.extract_wallet_transactions(target)
        logger.info(f"   Target: {len(target_txs)} transactions")
        
        # Combine and deduplicate transactions by hash
        all_transactions = {tx['tx_hash']: tx for tx in source_txs + target_txs}
        transactions = list(all_transactions.values())
        
        logger.info(f"✅ Loaded {len(transactions)} unique transactions")
        
        # Enrich transactions with USD values using price oracle
        logger.info(f"💰 Enriching transactions with USD values...")
        transactions = tx_extractor.enrich_with_usd_value(
            transactions,
            oracle
        )
        
        # Trace the flow path
        logger.info(f"🎯 Tracing flow path (max_hops={request.max_hops}, min_confidence={request.min_confidence})...")
        result = flow_tracer.trace_flow(
            source,
            target,
            transactions,
            max_hops=request.max_hops,
            min_confidence=request.min_confidence
        )
        
        # Log results
        if result['path_exists']:
            logger.info(f"✅ Found {result['path_count']} path(s) from source to target")
            logger.info(f"   Total volume: ${result.get('total_volume', 0):,.2f}")
        else:
            logger.info(f"❌ No path found between addresses within {request.max_hops} hops")
        
        return {
            "success": True,
            "data": result
        }
        
    except ValueError as e:
        # Handle validation errors
        logger.error(f"❌ Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"❌ Flow trace failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# EXPORTS
# ============================================================================

# Export router for main.py
router = flow_router
__all__ = ["flow_router", "router"]
