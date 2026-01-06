"""
Network Graph Endpoints
========================

Network visualization and activity heatmap endpoints.

This module handles all network-related endpoints:
- Network graph visualization (✨ WITH ACTUAL EDGES via LinkBuilder)
- Activity heatmap (24x7 time-based activity)

✨ NEW IN v2.0:
- Generates real edges using LinkBuilder
- Returns both Cytoscape and Sankey formats
- 5-minute caching
- Discovery + transaction data sources
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime, timedelta

from .dependencies import (
    get_db, 
    get_link_builder,  # ✨ NEW
    ensure_registry_wallets_in_db
)

from app.core.otc_analysis.models.wallet import Wallet as OTCWallet

logger = logging.getLogger(__name__)

# Network router - no prefix, URLs added via /api/otc in main.py
network_router = APIRouter(prefix="", tags=["Network"])


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _get_category_from_wallet(wallet: OTCWallet) -> str:
    """
    Determine display category from wallet entity type.
    
    Returns: 'Exchange', 'Otc Desk', 'Institutional', or 'Unknown'
    """
    entity_type = (wallet.entity_type or "").lower()
    
    if 'exchange' in entity_type:
        return 'Exchange'
    elif 'otc' in entity_type or 'desk' in entity_type:
        return 'Otc Desk'
    elif 'institution' in entity_type or 'fund' in entity_type:
        return 'Institutional'
    else:
        # Check tags as fallback
        tags = wallet.tags or []
        if 'exchange' in tags:
            return 'Exchange'
        elif 'otc_desk' in tags or 'verified' in tags or 'discovered' in tags:
            return 'Otc Desk'
        elif 'institutional' in tags:
            return 'Institutional'
    
    return 'Unknown'


# ============================================================================
# NETWORK ENDPOINTS
# ============================================================================

@network_router.get("/network")
async def get_network_graph(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    max_nodes: int = Query(500, le=1000),
    generate_edges: bool = Query(True, description="Generate edges between wallets"),
    use_discovery: bool = Query(True, description="Use discovery data for edge generation"),
    use_transactions: bool = Query(False, description="Use blockchain transactions (slower)"),
    min_flow_size: float = Query(100000, description="Minimum flow value for edges (USD)"),
    db: Session = Depends(get_db),
    link_builder = Depends(get_link_builder)  # ✨ NEW
):
    """
    Get network graph data for NetworkGraph and SankeyFlow components.
    
    GET /api/otc/network?start_date=2024-12-01&end_date=2024-12-28&max_nodes=500&generate_edges=true
    
    ✨ NEW IN v2.0:
    - Generates actual edges using LinkBuilder
    - Multiple data sources: discovery (fast) + blockchain (accurate)
    - 5-minute caching for performance
    - Returns both Cytoscape and Sankey formats
    
    Parameters:
        start_date: ISO format date (e.g., '2024-12-01')
        end_date: ISO format date (e.g., '2024-12-28')
        max_nodes: Maximum number of nodes to return (default: 500, max: 1000)
        generate_edges: If true, generates edges between wallets (default: true)
        use_discovery: Use discovery data for fast edge generation (default: true)
        use_transactions: Use blockchain transactions (slower but complete, default: false)
        min_flow_size: Minimum flow value for edges in USD (default: 100000)
    
    Returns:
        nodes: Cytoscape-formatted nodes for network visualization
        edges: Cytoscape-formatted edges (✨ NOW GENERATED!)
        sankeyNodes: D3-Sankey formatted nodes
        sankeyLinks: D3-Sankey formatted links (✨ NOW GENERATED!)
        metadata: Period info, counts, and data sources
    
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
        logger.info(f"   • generate_edges={generate_edges}, use_discovery={use_discovery}, use_transactions={use_transactions}")
        
        # Query top wallets by volume in the specified time range
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).order_by(OTCWallet.total_volume.desc()).limit(max_nodes).all()
        
        logger.info(f"   Found {len(wallets)} wallets for network")
        
        # ====================================================================
        # FORMAT NODES (Both Cytoscape and Sankey)
        # ====================================================================
        
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
                "category": _get_category_from_wallet(w),
                "value": float(w.total_volume or 0),
                "address": w.address,
                "confidence": float(w.confidence_score or 0)
            }
            for w in wallets
        ]
        
        # ====================================================================
        # ✨ NEW: GENERATE EDGES/LINKS USING LINKBUILDER
        # ====================================================================
        
        cytoscape_edges = []
        sankey_links = []
        edge_metadata = {}
        
        if generate_edges and len(wallets) > 1:
            try:
                logger.info(f"   🔗 Generating edges using LinkBuilder...")
                
                # Use LinkBuilder service
                result = link_builder.build_links(
                    db=db,
                    wallets=wallets,
                    start_date=start,
                    end_date=end,
                    min_flow_size=min_flow_size,
                    use_discovery=use_discovery,
                    use_transactions=use_transactions
                )
                
                # Extract both formats
                cytoscape_edges = result.get("cytoscape_edges", [])
                sankey_links = result.get("sankey_links", [])
                edge_metadata = result.get("metadata", {})
                
                logger.info(
                    f"   ✅ LinkBuilder: {len(cytoscape_edges)} edges, {len(sankey_links)} links "
                    f"(source: {edge_metadata.get('source', 'unknown')}, "
                    f"cached: {edge_metadata.get('cached', False)})"
                )
                
            except Exception as edge_error:
                logger.error(f"   ⚠️ Error generating edges: {edge_error}", exc_info=True)
                logger.info(f"   Continuing with nodes only...")
        
        # ====================================================================
        # RETURN RESULT
        # ====================================================================
        
        logger.info(f"✅ Network: {len(cytoscape_nodes)} nodes, {len(cytoscape_edges)} edges")
        
        return {
            # For NetworkGraph component (Cytoscape.js)
            "nodes": cytoscape_nodes,
            "edges": cytoscape_edges,  # ✨ NOW POPULATED!
            
            # For SankeyFlow component (D3-Sankey)
            "sankeyNodes": sankey_nodes,
            "sankeyLinks": sankey_links,  # ✨ NOW POPULATED!
            
            "metadata": {
                "node_count": len(cytoscape_nodes),
                "edge_count": len(cytoscape_edges),
                "link_count": len(sankey_links),
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                },
                # ✨ NEW: Edge generation metadata
                "edges_generated": generate_edges,
                "edge_source": edge_metadata.get("source", "none"),
                "edge_cached": edge_metadata.get("cached", False),
                "discovery_used": edge_metadata.get("discovery_used", False),
                "transactions_used": edge_metadata.get("transactions_used", False),
                "min_flow_size": min_flow_size
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
