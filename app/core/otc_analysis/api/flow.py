"""
Network & Flow Endpoints
=========================

Network graph, flow tracing, and activity monitoring.
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

# Network router
network_router = APIRouter(prefix="", tags=["Network"])

# Flow router
flow_router = APIRouter(prefix="", tags=["Flow"])


# ============================================================================
# NETWORK ENDPOINTS
# ============================================================================

@network_router.get("/network/graph")
async def get_network_graph(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    max_nodes: int = Query(500, le=1000),
    db: Session = Depends(get_db)
):
    """Get network graph data for NetworkGraph and SankeyFlow components"""
    try:
        await ensure_registry_wallets_in_db(db, max_to_fetch=5)
        
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"🌐 GET /network/graph: {start.date()} to {end.date()}")
        
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).order_by(OTCWallet.total_volume.desc()).limit(max_nodes).all()
        
        # Format for NetworkGraph (Cytoscape)
        cytoscape_nodes = [
            {
                "address": w.address,
                "label": w.label or f"{w.address[:6]}...{w.address[-4:]}",
                "entity_type": w.entity_type or "unknown",
                "entity_name": w.entity_name or "",
                "total_volume_usd": float(w.total_volume or 0),
                "transaction_count": w.transaction_count or 0,
                "confidence_score": float(w.confidence_score or 0),
                "is_active": w.is_active if w.is_active is not None else False,
                "tags": w.tags or []
            }
            for w in wallets
        ]
        
        # Format for SankeyFlow (D3-Sankey)
        sankey_nodes = [
            {
                "name": w.label or f"{w.address[:8]}...",
                "category": (w.entity_type or "unknown").replace('_', ' ').title(),
                "value": float(w.total_volume or 0),
                "address": w.address
            }
            for w in wallets
        ]
        
        cytoscape_edges = []
        sankey_links = []
        
        logger.info(f"✅ Graph: {len(cytoscape_nodes)} nodes")
        
        return {
            "nodes": cytoscape_nodes,
            "edges": cytoscape_edges,
            "sankeyNodes": sankey_nodes,
            "sankeyLinks": sankey_links,
            "metadata": {
                "node_count": len(cytoscape_nodes),
                "edge_count": 0,
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                }
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /network/graph: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@network_router.get("/heatmap")
async def get_activity_heatmap(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """Get 24x7 activity heatmap"""
    try:
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=7)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"🔥 GET /heatmap: {start.date()} to {end.date()}")
        
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).all()
        
        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        heatmap = []
        
        for day_idx, day in enumerate(days):
            for hour in range(24):
                volume = sum(
                    (w.total_volume or 0) / (7 * 24)
                    for w in wallets
                    if w.last_active and w.last_active.weekday() == day_idx
                )
                
                heatmap.append({
                    "day": day,
                    "hour": hour,
                    "volume": volume,
                    "count": len([w for w in wallets if w.last_active and w.last_active.weekday() == day_idx]) // 24
                })
        
        logger.info(f"✅ Heatmap: {len(heatmap)} cells")
        
        return {
            "heatmap": heatmap,
            "period": {
                "start": start.isoformat(),
                "end": end.isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /heatmap: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# FLOW ENDPOINTS
# ============================================================================

@flow_router.post("/trace")
async def trace_flow(
    request: FlowTraceRequest,
    tx_extractor = Depends(get_transaction_extractor),
    oracle = Depends(get_price_oracle)
):
    """
    Trace money flow from source to target address.
    
    POST /api/otc/flow/trace
    """
    logger.info(f"🔄 Tracing flow: {request.source_address[:10]}... → {request.target_address[:10]}...")
    
    try:
        source = validate_ethereum_address(request.source_address)
        target = validate_ethereum_address(request.target_address)
        
        logger.info(f"📡 Fetching transaction data...")
        
        source_txs = tx_extractor.extract_wallet_transactions(source)
        target_txs = tx_extractor.extract_wallet_transactions(target)
        
        all_transactions = {tx['tx_hash']: tx for tx in source_txs + target_txs}
        transactions = list(all_transactions.values())
        
        logger.info(f"✅ Loaded {len(transactions)} transactions")
        
        transactions = tx_extractor.enrich_with_usd_value(
            transactions,
            oracle
        )
        
        logger.info(f"🎯 Tracing flow path...")
        result = flow_tracer.trace_flow(
            source,
            target,
            transactions,
            max_hops=request.max_hops,
            min_confidence=request.min_confidence
        )
        
        if result['path_exists']:
            logger.info(f"✅ Found {result['path_count']} path(s)")
        else:
            logger.info(f"❌ No path found")
        
        return {
            "success": True,
            "data": result
        }
        
    except Exception as e:
        logger.error(f"❌ Flow trace failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@flow_router.get("/sankey")
async def get_sankey_flow(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    min_flow_size: float = Query(100000),
    db: Session = Depends(get_db)
):
    """Get Sankey flow diagram data"""
    try:
        await ensure_registry_wallets_in_db(db, max_to_fetch=3)
        
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"💱 GET /flow/sankey: {start.date()} to {end.date()}")
        
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end,
            OTCWallet.total_volume >= min_flow_size
        ).order_by(OTCWallet.total_volume.desc()).limit(20).all()
        
        nodes = [
            {
                "name": w.label or f"{w.address[:8]}...",
                "category": (w.entity_type or "Unknown").replace('_', ' ').title(),
                "value": float(w.total_volume or 0),
                "address": w.address
            }
            for w in wallets
        ]
        
        links = []
        
        logger.info(f"✅ Sankey: {len(nodes)} nodes")
        
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
        logger.error(f"❌ Error in /flow/sankey: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@flow_router.get("/transfers/timeline")
async def get_transfer_timeline(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    min_confidence: float = Query(0),
    db: Session = Depends(get_db)
):
    """Get transfer timeline data"""
    try:
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=7)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"📅 GET /transfers/timeline: {start.date()} to {end.date()}")
        
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end,
            OTCWallet.confidence_score >= min_confidence
        ).order_by(OTCWallet.last_active.desc()).limit(50).all()
        
        events = [
            {
                "timestamp": w.last_active.isoformat() if w.last_active else datetime.now().isoformat(),
                "from_address": w.address,
                "to_address": None,
                "amount_usd": float(w.total_volume or 0) / (w.transaction_count or 1),
                "token": "USDT",
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
        logger.error(f"❌ Error in /transfers/timeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Export routers
router = flow_router  # ✅ Default export
__all__ = ["network_router", "flow_router", "router"]
