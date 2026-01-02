"""
Network Graph Endpoints
========================

Network visualization and activity heatmap endpoints.

This module handles all network-related endpoints:
- Network graph visualization (Cytoscape + Sankey formats)
- Activity heatmap (24x7 time-based activity)
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime, timedelta

from .dependencies import get_db, ensure_registry_wallets_in_db

from app.core.otc_analysis.models.wallet import Wallet as OTCWallet

logger = logging.getLogger(__name__)

# Network router - no prefix, URLs added via /api/otc in main.py
network_router = APIRouter(prefix="", tags=["Network"])


# ============================================================================
# NETWORK ENDPOINTS
# ============================================================================

@network_router.get("/network")
async def get_network_graph(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    max_nodes: int = Query(500, le=1000),
    db: Session = Depends(get_db)
):
    """
    Get network graph data for NetworkGraph and SankeyFlow components.
    
    GET /api/otc/network?start_date=2024-12-01&end_date=2024-12-28&max_nodes=500
    
    Returns:
        nodes: Cytoscape-formatted nodes for network visualization
        edges: Cytoscape-formatted edges (currently empty)
        sankeyNodes: D3-Sankey formatted nodes
        sankeyLinks: D3-Sankey formatted links (currently empty)
        metadata: Period info and node/edge counts
    
    This endpoint provides data in two formats:
    1. Cytoscape.js format - for interactive network graphs
    2. D3-Sankey format - for flow diagrams
    
    Both formats represent the same wallet data but are structured
    differently for their respective visualization libraries.
    """
    try:
        # AUTO-SYNC: Ensure registry wallets are in database
        await ensure_registry_wallets_in_db(db, max_to_fetch=5)
        
        # Parse dates with timezone handling
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"🌐 GET /network: {start.date()} to {end.date()}, max_nodes={max_nodes}")
        
        # Query top wallets by volume in the specified time range
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).order_by(OTCWallet.total_volume.desc()).limit(max_nodes).all()
        
        # Format for NetworkGraph component (Cytoscape.js)
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
        
        # Format for SankeyFlow component (D3-Sankey)
        sankey_nodes = [
            {
                "name": w.label or f"{w.address[:8]}...",
                "category": (w.entity_type or "unknown").replace('_', ' ').title(),
                "value": float(w.total_volume or 0),
                "address": w.address
            }
            for w in wallets
        ]
        
        # Edges/links will be empty until we implement transaction relationships
        # These would connect wallets that have transacted with each other
        cytoscape_edges = []
        sankey_links = []
        
        logger.info(f"✅ Network: {len(cytoscape_nodes)} nodes, {len(cytoscape_edges)} edges")
        
        return {
            # For NetworkGraph component (Cytoscape.js)
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
        logger.error(f"❌ Error in /network: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@network_router.get("/heatmap")
async def get_activity_heatmap(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get 24x7 activity heatmap.
    
    GET /api/otc/heatmap?start_date=2024-12-17&end_date=2024-12-24
    
    Returns:
        heatmap: List of cells with day/hour/volume/count data
        period: Time range for the heatmap
    
    The heatmap shows wallet activity patterns across:
    - 7 days (Monday to Sunday)
    - 24 hours (0-23)
    
    Each cell contains:
    - day: Day of week (Mon, Tue, etc.)
    - hour: Hour of day (0-23)
    - volume: Total transaction volume in USD
    - count: Number of active wallets
    
    This helps identify:
    - Peak trading hours
    - Weekly patterns
    - Timezone-based activity
    - Anomalous activity periods
    """
    try:
        # Parse dates with timezone handling
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=7)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"🔥 GET /heatmap: {start.date()} to {end.date()}")
        
        # Get all wallets active in the time range
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).all()
        
        # Create 7x24 heatmap grid (7 days × 24 hours = 168 cells)
        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        heatmap = []
        
        for day_idx, day in enumerate(days):
            for hour in range(24):
                # Calculate volume for this day/hour combination
                # Note: This is a simplified distribution model
                # In production, you would:
                # 1. Group transactions by actual timestamp
                # 2. Extract day and hour from each transaction
                # 3. Sum volumes for matching day/hour pairs
                volume = sum(
                    (w.total_volume or 0) / (7 * 24)  # Distribute evenly for now
                    for w in wallets
                    if w.last_active and w.last_active.weekday() == day_idx
                )
                
                # Calculate wallet count for this day/hour
                # Simplified: divide wallets evenly across hours
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
        
        logger.info(f"✅ Heatmap: {len(heatmap)} cells generated (7 days × 24 hours)")
        
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
# EXPORTS
# ============================================================================

# Export router for main.py
router = network_router
__all__ = ["network_router", "router"]
