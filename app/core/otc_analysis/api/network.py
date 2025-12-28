"""
Network Graph Endpoints
========================

Network visualization and analysis endpoints.
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime, timedelta

from .dependencies import get_db, ensure_registry_wallets_in_db

from app.core.otc_analysis.models.wallet import Wallet as OTCWallet

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/network", tags=["Network"])


# ============================================================================
# NETWORK ENDPOINTS
# ============================================================================

@router.get("/graph")
async def get_network_graph(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    max_nodes: int = Query(500, le=1000),
    db: Session = Depends(get_db)
):
    """
    Get network graph data for NetworkGraph and SankeyFlow components.
    
    GET /api/otc/network/graph?start_date=2024-12-01&end_date=2024-12-28&max_nodes=500
    
    Returns data in two formats:
    - Cytoscape format (for NetworkGraph component)
    - D3-Sankey format (for SankeyFlow component)
    """
    try:
        # AUTO-SYNC
        await ensure_registry_wallets_in_db(db, max_to_fetch=5)
        
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"🌐 GET /network/graph: {start.date()} to {end.date()}, max_nodes={max_nodes}")
        
        # Get top wallets by volume
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
        
        # Empty edges/links (no transaction relationships yet)
        cytoscape_edges = []
        sankey_links = []
        
        logger.info(f"✅ Graph: {len(cytoscape_nodes)} nodes, 0 edges")
        
        return {
            # For NetworkGraph component (Cytoscape)
            "nodes": cytoscape_nodes,
            "edges": cytoscape_edges,
            
            # For SankeyFlow component (D3-Sankey)
            "sankeyNodes": sankey_nodes,
            "sankeyLinks": sankey_links,
            
            "metadata": {
                "node_count": len(cytoscape_nodes),
                "edge_count": len(cytoscape_edges),
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                }
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /network/graph: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/heatmap")
async def get_activity_heatmap(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get 24x7 activity heatmap.
    
    GET /api/otc/network/heatmap?start_date=2024-12-17&end_date=2024-12-24
    
    Returns heatmap data showing activity by day of week and hour of day.
    """
    try:
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=7)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"🔥 GET /network/heatmap: {start.date()} to {end.date()}")
        
        # Get wallets in time range
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).all()
        
        # Create 7x24 heatmap
        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        heatmap = []
        
        for day_idx, day in enumerate(days):
            for hour in range(24):
                # Distribute volume across hours
                volume = sum(
                    (w.total_volume or 0) / (7 * 24)
                    for w in wallets
                    if w.last_active and w.last_active.weekday() == day_idx
                )
                
                count = len([
                    w for w in wallets 
                    if w.last_active and w.last_active.weekday() == day_idx
                ]) // 24
                
                heatmap.append({
                    "day": day,
                    "hour": hour,
                    "volume": volume,
                    "count": count
                })
        
        logger.info(f"✅ Heatmap: {len(heatmap)} cells generated")
        
        return {
            "heatmap": heatmap,
            "period": {
                "start": start.isoformat(),
                "end": end.isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /network/heatmap: {e}")
        raise HTTPException(status_code=500, detail=str(e))
