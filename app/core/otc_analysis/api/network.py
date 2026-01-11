"""
Network Graph Endpoints
========================

Network visualization and activity heatmap endpoints.

✅ FIXED IN v2.2 - TRANSACTION-BASED HEATMAP:
- Uses actual Transaction table for volume aggregation
- Real hourly variation based on transaction timestamps
- Accurate peak hour detection
- SQL GROUP BY for performance

Previous versions used Wallet.total_volume (lifetime) / 168 which was incorrect.
Now queries transactions grouped by EXTRACT(DOW) and EXTRACT(HOUR).
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from typing import Optional
from datetime import datetime, timedelta

from .dependencies import (
    get_db, 
    get_link_builder,
    ensure_registry_wallets_in_db
)

from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
from app.core.otc_analysis.models.transaction import Transaction  # ✅ NEW

logger = logging.getLogger(__name__)

# Network router
network_router = APIRouter(prefix="", tags=["Network"])


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _get_category_from_wallet(wallet: OTCWallet) -> str:
    """Determine display category from wallet entity type."""
    entity_type = (wallet.entity_type or "").lower()
    
    if 'exchange' in entity_type:
        return 'Exchange'
    elif 'otc' in entity_type or 'desk' in entity_type:
        return 'Otc Desk'
    elif 'institution' in entity_type or 'fund' in entity_type:
        return 'Institutional'
    else:
        tags = wallet.tags or []
        if 'exchange' in tags:
            return 'Exchange'
        elif 'otc_desk' in tags or 'verified' in tags or 'discovered' in tags:
            return 'Otc Desk'
        elif 'institutional' in tags:
            return 'Institutional'
    
    return 'Unknown'


def _empty_heatmap_response(start: datetime, end: datetime) -> dict:
    """Return empty heatmap structure when no data available."""
    return {
        "heatmap": [[0] * 24 for _ in range(7)],
        "peak_hours": [],
        "patterns": [],
        "period": {
            "start": start.isoformat(),
            "end": end.isoformat()
        },
        "metadata": {
            "total_wallets": 0,
            "total_volume": 0,
            "total_transactions": 0,
            "days": 7,
            "hours_per_day": 24,
            "total_cells": 168,
            "non_zero_cells": 0,
            "data_source": "transactions_table",
            "aggregation_method": "postgres_extract"
        }
    }


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
    link_builder = Depends(get_link_builder)
):
    """Get network graph data for NetworkGraph and SankeyFlow components."""
    try:
        await ensure_registry_wallets_in_db(db, max_to_fetch=5)
        
        # Parse dates
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"🌐 GET /network: {start.date()} to {end.date()}, max_nodes={max_nodes}")
        logger.info(f"   • generate_edges={generate_edges}, use_discovery={use_discovery}")
        
        # Query wallets
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).order_by(OTCWallet.total_volume.desc()).limit(max_nodes).all()
        
        logger.info(f"   Found {len(wallets)} wallets for network")
        
        # Format nodes
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
        
        # Generate edges
        cytoscape_edges = []
        sankey_links = []
        edge_metadata = {}
        
        if generate_edges and len(wallets) > 1:
            try:
                logger.info(f"   🔗 Generating edges using LinkBuilder...")
                
                result = link_builder.build_links(
                    db=db,
                    wallets=wallets,
                    start_date=start,
                    end_date=end,
                    min_flow_size=min_flow_size,
                    use_discovery=use_discovery,
                    use_transactions=use_transactions
                )
                
                cytoscape_edges = result.get("cytoscape_edges", [])
                sankey_links = result.get("sankey_links", [])
                edge_metadata = result.get("metadata", {})
                
                logger.info(
                    f"   ✅ LinkBuilder: {len(cytoscape_edges)} edges, {len(sankey_links)} links "
                    f"(source: {edge_metadata.get('source', 'unknown')})"
                )
                
            except Exception as edge_error:
                logger.error(f"   ⚠️ Error generating edges: {edge_error}", exc_info=True)
        
        logger.info(f"✅ Network: {len(cytoscape_nodes)} nodes, {len(cytoscape_edges)} edges")
        
        return {
            "nodes": cytoscape_nodes,
            "edges": cytoscape_edges,
            "sankeyNodes": sankey_nodes,
            "sankeyLinks": sankey_links,
            "metadata": {
                "node_count": len(cytoscape_nodes),
                "edge_count": len(cytoscape_edges),
                "link_count": len(sankey_links),
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                },
                "edges_generated": generate_edges,
                "edge_source": edge_metadata.get("source", "none"),
                "edge_cached": edge_metadata.get("cached", False),
                "min_flow_size": min_flow_size
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /network: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


"""
ERSETZE den @network_router.get("/heatmap") Endpoint in network.py

✨ NEU: Auto-Sync wenn keine Transaktionen gefunden werden
"""

@network_router.get("/heatmap")
async def get_activity_heatmap(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    auto_sync: bool = Query(True, description="Auto-sync transactions if none found"),
    db: Session = Depends(get_db)
):
    """
    Get 24x7 activity heatmap with REAL transaction aggregation.
    
    ✅ ENHANCED IN v2.3:
    - Auto-syncs transactions if DB is empty
    - Uses Transaction table for accurate volumes
    - SQL GROUP BY with EXTRACT(DOW/HOUR)
    - Real hourly variation (not fake distribution)
    
    Parameters:
        start_date: Start date (ISO format)
        end_date: End date (ISO format)
        auto_sync: Auto-fetch transactions if none found (default: true)
    
    Returns:
        heatmap: 2D array [7 days][24 hours] with actual transaction volumes
        peak_hours: Top 5 busiest day/hour combinations
        patterns: Detected activity patterns
        period: Time range metadata
    """
    try:
        logger.info("=" * 80)
        logger.info("🔥 HEATMAP REQUEST (TRANSACTION-BASED v2.3 + AUTO-SYNC)")
        logger.info("=" * 80)
        
        # Parse dates
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            logger.info(f"📅 Start: {start.isoformat()}")
        else:
            start = datetime.now() - timedelta(days=7)
            logger.info(f"📅 Start (default): {start.isoformat()}")
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            logger.info(f"📅 End: {end.isoformat()}")
        else:
            end = datetime.now()
            logger.info(f"📅 End (default): {end.isoformat()}")
        
        # ================================================================
        # STEP 1: SELECT WALLETS
        # ================================================================
        
        logger.info(f"🔍 Step 1: Selecting active wallets...")
        
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).all()
        
        logger.info(f"✅ Found {len(wallets)} active wallets")
        
        if len(wallets) == 0:
            logger.warning("⚠️ No wallets found - returning empty heatmap")
            return _empty_heatmap_response(start, end)
        
        # Extract addresses (lowercase)
        wallet_addresses = [w.address.lower() for w in wallets]
        logger.info(f"📋 Prepared {len(wallet_addresses)} addresses for query")
        
        # Sample addresses for debugging
        if len(wallet_addresses) > 0:
            logger.info(f"   Sample addresses: {wallet_addresses[:3]}")
        
        # ================================================================
        # STEP 2: CHECK IF TRANSACTIONS EXIST IN DB
        # ================================================================
        
        logger.info(f"🔍 Step 2: Checking for existing transactions...")
        
        # Quick check: Do we have ANY transactions for these wallets?
        tx_check = db.query(Transaction).filter(
            Transaction.timestamp >= start,
            Transaction.timestamp <= end,
            or_(
                Transaction.from_address.in_(wallet_addresses),
                Transaction.to_address.in_(wallet_addresses)
            )
        ).limit(1).first()
        
        # ================================================================
        # ✨ AUTO-SYNC: If no transactions found, sync them!
        # ================================================================
        
        if not tx_check and auto_sync:
            logger.warning("⚠️ No transactions found in DB - triggering AUTO-SYNC")
            logger.info("🔄 Auto-syncing transactions for all wallets...")
            
            try:
                from app.core.otc_analysis.api.dependencies import sync_wallet_transactions_to_db
                
                # Sync top 10 wallets (most active)
                top_wallets = sorted(
                    wallets,
                    key=lambda w: w.total_volume or 0,
                    reverse=True
                )[:10]
                
                sync_stats = {
                    "wallets_synced": 0,
                    "transactions_saved": 0,
                    "total_fetched": 0
                }
                
                for wallet in top_wallets:
                    try:
                        logger.info(f"   📡 Syncing {wallet.label or wallet.address[:10]}...")
                        
                        stats = await sync_wallet_transactions_to_db(
                            db=db,
                            wallet_address=wallet.address,
                            max_transactions=100,
                            force_refresh=False
                        )
                        
                        sync_stats["wallets_synced"] += 1
                        sync_stats["transactions_saved"] += stats["saved_count"]
                        sync_stats["total_fetched"] += stats["fetched_count"]
                        
                        # Small delay to avoid rate limits
                        import asyncio
                        await asyncio.sleep(0.3)
                        
                    except Exception as sync_error:
                        logger.error(f"   ⚠️ Error syncing {wallet.address[:10]}: {sync_error}")
                        continue
                
                logger.info(
                    f"✅ Auto-sync complete: "
                    f"{sync_stats['wallets_synced']} wallets, "
                    f"{sync_stats['transactions_saved']} transactions saved"
                )
                
                # Refresh DB session to see new transactions
                db.expire_all()
                
            except Exception as auto_sync_error:
                logger.error(f"❌ Auto-sync failed: {auto_sync_error}", exc_info=True)
                logger.warning("⚠️ Continuing with empty heatmap...")
        
        elif not tx_check and not auto_sync:
            logger.warning("⚠️ No transactions found - auto_sync=false, returning empty heatmap")
            logger.info("💡 Tip: Enable auto_sync=true or run: POST /admin/sync-all-transactions")
            return _empty_heatmap_response(start, end)
        
        # ================================================================
        # STEP 3: AGGREGATE TRANSACTIONS BY DAY/HOUR
        # ================================================================
        
        logger.info(f"🔍 Step 3: Aggregating transactions...")
        
        # Query transactions grouped by day of week and hour
        results = db.query(
            func.extract('dow', Transaction.timestamp).label('day'),      # 0=Sun, 1=Mon, ..., 6=Sat
            func.extract('hour', Transaction.timestamp).label('hour'),    # 0-23
            func.sum(Transaction.usd_value).label('volume'),
            func.count(Transaction.tx_hash).label('tx_count')
        ).filter(
            Transaction.timestamp >= start,
            Transaction.timestamp <= end,
            or_(
                Transaction.from_address.in_(wallet_addresses),
                Transaction.to_address.in_(wallet_addresses)
            )
        ).group_by(
            func.extract('dow', Transaction.timestamp),
            func.extract('hour', Transaction.timestamp)
        ).all()
        
        logger.info(f"✅ Query returned {len(results)} aggregated cells")
        
        if len(results) > 0:
            total_volume = sum(r.volume or 0 for r in results)
            total_txs = sum(r.tx_count or 0 for r in results)
            logger.info(f"💰 Total volume: ${total_volume:,.2f}")
            logger.info(f"📊 Total transactions: {total_txs:,}")
            
            # Show sample results
            logger.info(f"📋 Sample results:")
            for r in results[:5]:
                logger.info(f"   DOW={int(r.day)} Hour={int(r.hour)} Vol=${r.volume:,.0f} TXs={r.tx_count}")
        else:
            logger.warning("⚠️ Still no transactions after sync - returning empty heatmap")
            return _empty_heatmap_response(start, end)
        
        # ================================================================
        # STEP 4: BUILD 2D HEATMAP ARRAY
        # ================================================================
        
        logger.info(f"🏗️ Step 4: Building 2D heatmap array...")
        
        # Create lookup: (postgres_dow, hour) -> volume
        volume_lookup = {}
        for r in results:
            day_idx = int(r.day)  # PostgreSQL DOW: 0=Sun, 1=Mon, ..., 6=Sat
            hour_idx = int(r.hour)  # 0-23
            volume = float(r.volume or 0)
            volume_lookup[(day_idx, hour_idx)] = volume
        
        logger.info(f"📋 Created lookup with {len(volume_lookup)} entries")
        
        # Map PostgreSQL DOW to our Monday-first index
        # PostgreSQL: 0=Sun, 1=Mon, 2=Tue, ..., 6=Sat
        # We want:    0=Mon, 1=Tue, 2=Wed, ..., 6=Sun
        dow_to_idx = {
            1: 0,  # Mon
            2: 1,  # Tue
            3: 2,  # Wed
            4: 3,  # Thu
            5: 4,  # Fri
            6: 5,  # Sat
            0: 6   # Sun
        }
        
        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        heatmap_2d = []
        peak_data = []
        
        for our_day_idx, day_name in enumerate(days):
            day_row = []
            
            # Find PostgreSQL DOW
            pg_dow = [k for k, v in dow_to_idx.items() if v == our_day_idx][0]
            
            for hour in range(24):
                volume = volume_lookup.get((pg_dow, hour), 0.0)
                day_row.append(volume)
                
                if volume > 0:
                    peak_data.append((our_day_idx, hour, volume))
            
            heatmap_2d.append(day_row)
            
            day_total = sum(day_row)
            non_zero = sum(1 for v in day_row if v > 0)
            logger.info(f"   ✓ {day_name}: {non_zero}/24 active, total=${day_total:,.2f}")
        
        logger.info(f"✅ Heatmap built: 7×24")
        
        # ================================================================
        # STEP 5: DETECT PEAK HOURS
        # ================================================================
        
        logger.info(f"🔍 Step 5: Detecting peaks...")
        
        peak_data.sort(key=lambda x: x[2], reverse=True)
        peak_hours = [
            {
                "day": day_idx,
                "day_name": days[day_idx],
                "hour": hour,
                "value": volume
            }
            for day_idx, hour, volume in peak_data[:5]
            if volume > 0
        ]
        
        logger.info(f"✅ Found {len(peak_hours)} peaks:")
        for i, peak in enumerate(peak_hours[:3], 1):
            logger.info(f"   {i}. {peak['day_name']} {peak['hour']:02d}:00 - ${peak['value']:,.2f}")
        
        # ================================================================
        # STEP 6: DETECT PATTERNS
        # ================================================================
        
        logger.info(f"🔍 Step 6: Detecting patterns...")
        
        patterns = []
        
        # Weekend vs Weekday
        weekday_vol = sum(sum(heatmap_2d[i]) for i in range(5))
        weekend_vol = sum(sum(heatmap_2d[i]) for i in range(5, 7))
        
        if weekday_vol > 0 or weekend_vol > 0:
            if weekend_vol > weekday_vol * 1.2:
                patterns.append({"icon": "📅", "description": "Higher activity on weekends"})
            elif weekday_vol > weekend_vol * 1.2:
                patterns.append({"icon": "💼", "description": "Higher activity on weekdays"})
        
        # Day vs Night
        day_hours = sum(sum(heatmap_2d[d][h] for h in range(6, 18)) for d in range(7))
        night_hours = sum(sum(heatmap_2d[d][h] for h in list(range(0, 6)) + list(range(18, 24))) for d in range(7))
        
        if day_hours > 0 or night_hours > 0:
            if night_hours > day_hours * 1.2:
                patterns.append({"icon": "🌙", "description": "High activity during night hours (18:00-06:00)"})
            elif day_hours > night_hours * 1.2:
                patterns.append({"icon": "☀️", "description": "High activity during day hours (06:00-18:00)"})
        
        # 24/7 Consistent
        non_zero_cells = sum(1 for day in heatmap_2d for hour in day if hour > 0)
        if non_zero_cells > 120:
            patterns.append({"icon": "🔄", "description": "Consistent 24/7 activity across all days"})
        
        logger.info(f"✅ Detected {len(patterns)} patterns:")
        for p in patterns:
            logger.info(f"   {p['icon']} {p['description']}")
        
        # ================================================================
        # STEP 7: BUILD RESPONSE
        # ================================================================
        
        total_volume = sum(sum(day) for day in heatmap_2d)
        total_txs = sum(r.tx_count for r in results)
        
        response = {
            "heatmap": heatmap_2d,
            "peak_hours": peak_hours,
            "patterns": patterns,
            "period": {
                "start": start.isoformat(),
                "end": end.isoformat()
            },
            "metadata": {
                "total_wallets": len(wallets),
                "total_volume": total_volume,
                "total_transactions": total_txs,
                "days": 7,
                "hours_per_day": 24,
                "total_cells": 168,
                "non_zero_cells": non_zero_cells,
                "data_source": "transactions_table",
                "aggregation_method": "postgres_extract",
                "auto_sync_enabled": auto_sync  # ✨ NEW
            }
        }
        
        logger.info("=" * 80)
        logger.info("✅ HEATMAP READY (TRANSACTION-BASED + AUTO-SYNC)")
        logger.info(f"   Total volume: ${total_volume:,.2f}")
        logger.info(f"   Total TXs: {total_txs:,}")
        logger.info(f"   Active cells: {non_zero_cells}/168")
        logger.info("=" * 80)
        
        return response
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ ERROR: {e}")
        logger.error("=" * 80)
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# EXPORTS
# ============================================================================

router = network_router
__all__ = ["network_router", "router"]
