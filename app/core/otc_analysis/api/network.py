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
from typing import Dict, Optional, Any, List, Set
from datetime import datetime, timedelta

from .dependencies import (
    get_db, 
    get_link_builder,
    ensure_registry_wallets_in_db
)

from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
from app.core.otc_analysis.models.transaction import Transaction  # ✅ NEW
from sqlalchemy import func, or_, case  # ✅ case hinzugefügt!

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
        use_transactions: bool = Query(True, description="Use blockchain transactions"),  # ✅ Changed to True
        min_flow_size: float = Query(100000, description="Minimum flow value for edges (USD)"),
        include_high_volume_wallets: bool = Query(True, description="Include high-volume wallets"),
        db: Session = Depends(get_db),
        link_builder = Depends(get_link_builder)
    ):
        """
        ✅ ENHANCED: Network graph with high-volume wallets AND transaction-based edges
        """
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
            
            # ====================================================================
            # STEP 1: Query OTC Desks
            # ====================================================================
            
            otc_query = db.query(OTCWallet).filter(
                OTCWallet.last_active >= start,
                OTCWallet.last_active <= end,
                OTCWallet.entity_type != 'high_volume_wallet'
            )
            
            otc_wallets = otc_query.order_by(
                OTCWallet.total_volume.desc()
            ).limit(max_nodes).all()
            
            logger.info(f"   Found {len(otc_wallets)} OTC desks")
            
            # ====================================================================
            # ✅ STEP 2: Query High-Volume Wallets (IGNORE TIME FILTER!)
            # ====================================================================
            
            wallet_nodes = []
            
            if include_high_volume_wallets:
                # ✅ FIX: Load ALL wallets, regardless of last_active
                wallet_query = db.query(OTCWallet).filter(
                    OTCWallet.entity_type == 'high_volume_wallet'
                )
                
                high_volume_wallets = wallet_query.order_by(
                    OTCWallet.total_volume.desc()
                ).limit(100).all()
                
                logger.info(f"   Found {len(high_volume_wallets)} high-volume wallets (ignoring time filter)")
                
                for w in high_volume_wallets:
                    # Extract classification from tags
                    classification = None
                    for tag in (w.tags or []):
                        if tag in ['mega_whale', 'whale', 'institutional', 'large_wallet', 'medium_wallet']:
                            classification = tag
                            break
                    
                    # Categorize tags
                    categorized_tags = _categorize_wallet_tags(w.tags or [])
                    
                    wallet_nodes.append({
                        "address": w.address,
                        "label": w.label or f"{w.address[:6]}...{w.address[-4:]}",
                        "entity_type": w.entity_type,
                        "node_type": "high_volume_wallet",
                        "classification": classification,
                        "entity_name": w.entity_name or "",
                        "total_volume_usd": float(w.total_volume or 0),
                        "transaction_count": w.transaction_count or 0,
                        "confidence_score": float(w.confidence_score or 0),
                        "volume_score": float(w.confidence_score or 0),
                        "is_active": w.is_active if w.is_active is not None else False,
                        "tags": w.tags or [],
                        "categorized_tags": categorized_tags,
                        "first_seen": w.first_seen.isoformat() if w.first_seen else None,
                        "last_active": w.last_active.isoformat() if w.last_active else None
                    })
            
            # Format OTC nodes
            cytoscape_nodes = [
                {
                    "address": w.address,
                    "label": w.label or f"{w.address[:6]}...{w.address[-4:]}",
                    "entity_type": w.entity_type or "unknown",
                    "node_type": "otc_desk",
                    "entity_name": w.entity_name or "",
                    "total_volume_usd": float(w.total_volume or 0),
                    "transaction_count": w.transaction_count or 0,
                    "confidence_score": float(w.confidence_score or 0),
                    "is_active": w.is_active if w.is_active is not None else False,
                    "tags": w.tags or []
                }
                for w in otc_wallets
            ]
            
            # ✅ Combine both types
            cytoscape_nodes.extend(wallet_nodes)
            
            # Sankey nodes (only OTC desks)
            sankey_nodes = [
                {
                    "name": w.label or f"{w.address[:8]}...",
                    "category": _get_category_from_wallet(w),
                    "value": float(w.total_volume or 0),
                    "address": w.address,
                    "confidence": float(w.confidence_score or 0)
                }
                for w in otc_wallets
            ]
            
            # ====================================================================
            # ✅ STEP 3: Generate Edges (OTC ↔ OTC + Wallet ↔ OTC)
            # ====================================================================
            
            cytoscape_edges = []
            sankey_links = []
            edge_metadata = {}
            
            if generate_edges and (len(otc_wallets) > 1 or len(wallet_nodes) > 0):
                try:
                    logger.info(f"   🔗 Generating edges...")
                    
                    # ✅ Generate OTC ↔ OTC edges
                    if len(otc_wallets) > 1:
                        result = link_builder.build_links(
                            db=db,
                            wallets=otc_wallets,
                            start_date=start,
                            end_date=end,
                            min_flow_size=min_flow_size,
                            use_discovery=use_discovery,
                            use_transactions=use_transactions
                        )
                        
                        cytoscape_edges = result.get("cytoscape_edges", [])
                        sankey_links = result.get("sankey_links", [])
                        edge_metadata = result.get("metadata", {})
                    
                    # ====================================================================
                    # ✅ NEW: Generate Wallet ↔ OTC Edges from Transactions
                    # ====================================================================
                    
                    if len(wallet_nodes) > 0 and use_transactions:
                        wallet_addresses = [w['address'].lower() for w in wallet_nodes]
                        otc_addresses = [w.address.lower() for w in otc_wallets]
                        
                        logger.info(f"   🔗 Generating Wallet ↔ OTC edges from transactions...")
                        
                        # Query transactions between wallets and OTC desks
                        wallet_edges_query = db.query(
                            Transaction.from_address,
                            Transaction.to_address,
                            func.count(Transaction.tx_hash).label('tx_count'),
                            func.coalesce(func.sum(Transaction.usd_value), 0).label('total_volume')
                        ).filter(
                            Transaction.timestamp >= start,
                            Transaction.timestamp <= end,
                            or_(
                                # Wallet → OTC
                                and_(
                                    Transaction.from_address.in_(wallet_addresses),
                                    Transaction.to_address.in_(otc_addresses)
                                ),
                                # OTC → Wallet
                                and_(
                                    Transaction.from_address.in_(otc_addresses),
                                    Transaction.to_address.in_(wallet_addresses)
                                )
                            )
                        ).group_by(
                            Transaction.from_address,
                            Transaction.to_address
                        ).having(
                            func.coalesce(func.sum(Transaction.usd_value), 0) >= min_flow_size
                        ).all()
                        
                        # Add wallet edges
                        for edge in wallet_edges_query:
                            cytoscape_edges.append({
                                "data": {
                                    "source": edge.from_address,
                                    "target": edge.to_address,
                                    "transfer_amount_usd": float(edge.total_volume or 0),
                                    "transaction_count": edge.tx_count,
                                    "is_suspected_otc": False,
                                    "edge_source": "transactions"  # ✅ Mark as transaction-based
                                }
                            })
                        
                        logger.info(f"   ✅ Added {len(wallet_edges_query)} Wallet ↔ OTC edges")
                    
                except Exception as edge_error:
                    logger.error(f"   ⚠️ Error generating edges: {edge_error}", exc_info=True)
            
            logger.info(
                f"✅ Network: {len(cytoscape_nodes)} nodes "
                f"({len(otc_wallets)} OTC, {len(wallet_nodes)} wallets), "
                f"{len(cytoscape_edges)} edges"
            )
            
            return {
                "nodes": cytoscape_nodes,
                "edges": cytoscape_edges,
                "sankeyNodes": sankey_nodes,
                "sankeyLinks": sankey_links,
                "metadata": {
                    "node_count": len(cytoscape_nodes),
                    "otc_desk_count": len(otc_wallets),
                    "wallet_count": len(wallet_nodes),
                    "edge_count": len(cytoscape_edges),
                    "link_count": len(sankey_links),
                    "period": {
                        "start": start.isoformat(),
                        "end": end.isoformat()
                    },
                    "edges_generated": generate_edges,
                    "edge_source": edge_metadata.get("source", "mixed"),
                    "edge_cached": edge_metadata.get("cached", False),
                    "min_flow_size": min_flow_size
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error in /network: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


# ✅ NEW: Helper function to categorize wallet tags
def _categorize_wallet_tags(tags: List[str]) -> Dict[str, List[str]]:
    """
    Categorize wallet tags by type for better UI display.
    
    Categories: volume, activity, tokens, behavior, network, risk, temporal
    """
    categories = {
        "volume": [],
        "activity": [],
        "tokens": [],
        "behavior": [],
        "network": [],
        "risk": [],
        "temporal": []
    }
    
    # Define tag categories
    volume_tags = ['very_high_volume', 'high_volume', 'large_transactions', 'mega_whale', 'whale']
    activity_tags = ['hyperactive', 'active_trader', 'consistent', 'sporadic']
    token_tags = ['multi_token', 'token_diversity', 'defi_native', 'stablecoin_dominant']
    behavior_tags = ['institutional', 'systematic', 'concentrated_bets', 'diversified']
    network_tags = ['well_connected', 'hub_wallet', 'isolated']
    risk_tags = ['systemic_risk_potential', 'material_market_impact', 'high_risk_sizing']
    temporal_tags = ['24/7_active', 'business_hours', 'after_hours']
    
    for tag in tags:
        tag_lower = tag.lower()
        
        if any(t in tag_lower for t in volume_tags):
            categories["volume"].append(tag)
        elif any(t in tag_lower for t in activity_tags):
            categories["activity"].append(tag)
        elif any(t in tag_lower for t in token_tags):
            categories["tokens"].append(tag)
        elif any(t in tag_lower for t in behavior_tags):
            categories["behavior"].append(tag)
        elif any(t in tag_lower for t in network_tags):
            categories["network"].append(tag)
        elif any(t in tag_lower for t in risk_tags):
            categories["risk"].append(tag)
        elif any(t in tag_lower for t in temporal_tags):
            categories["temporal"].append(tag)
    
    # Add "all" category with all tags
    categories["all"] = tags
    
    return categories


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
    
    ✅ ENHANCED IN v2.4:
    - Shows ALL transactions (even without USD values)
    - Tracks enrichment rate
    - Auto-syncs if DB is empty
    - Uses Transaction table for accurate volumes
    
    Parameters:
        start_date: Start date (ISO format)
        end_date: End date (ISO format)
        auto_sync: Auto-fetch transactions if none found (default: true)
    
    Returns:
        heatmap: 2D array [7 days][24 hours] with transaction volumes
        peak_hours: Top 5 busiest day/hour combinations
        patterns: Detected activity patterns
        period: Time range metadata
    """
    try:
        logger.info("=" * 80)
        logger.info("🔥 HEATMAP REQUEST (TRANSACTION-BASED v2.4 + AUTO-SYNC)")
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
        
        # ✅ KORRIGIERTE QUERY mit case()
        results = db.query(
            func.extract('dow', Transaction.timestamp).label('day'),
            func.extract('hour', Transaction.timestamp).label('hour'),
            func.count(Transaction.tx_hash).label('tx_count'),
            func.coalesce(func.sum(Transaction.usd_value), 0).label('volume'),
            func.sum(
                case(
                    (Transaction.usd_value.isnot(None), 1),
                    else_=0
                )
            ).label('enriched_count')
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
            total_enriched = sum(r.enriched_count or 0 for r in results)
            enrichment_rate = (total_enriched / total_txs * 100) if total_txs > 0 else 0
            
            logger.info(f"💰 Total volume: ${total_volume:,.2f}")
            logger.info(f"📊 Total transactions: {total_txs:,}")
            logger.info(f"💵 Enriched: {total_enriched:,} ({enrichment_rate:.1f}%)")
            
            # Show sample results
            logger.info(f"📋 Sample results:")
            for r in results[:5]:
                logger.info(
                    f"   DOW={int(r.day)} Hour={int(r.hour)} "
                    f"Vol=${r.volume:,.0f} TXs={r.tx_count} "
                    f"Enriched={r.enriched_count}"
                )
            
            # ✅ Warning wenn viele TXs nicht enriched sind
            if enrichment_rate < 50 and total_txs > 10:
                logger.warning(
                    f"⚠️ Low enrichment rate ({enrichment_rate:.1f}%) - "
                    f"{total_txs - total_enriched} transactions need USD values"
                )
                logger.info(f"💡 Run: POST /admin/enrich-missing-values")
        else:
            logger.warning("⚠️ Still no transactions after sync - returning empty heatmap")
            return _empty_heatmap_response(start, end)
        
        # ================================================================
        # STEP 4: BUILD 2D HEATMAP ARRAY
        # ================================================================
        
        logger.info(f"🏗️ Step 4: Building 2D heatmap array...")
        
        # Create lookup: (postgres_dow, hour) -> (volume, tx_count)
        cell_lookup = {}
        for r in results:
            day_idx = int(r.day)
            hour_idx = int(r.hour)
            volume = float(r.volume or 0)
            tx_count = int(r.tx_count or 0)
            enriched = int(r.enriched_count or 0)
            
            cell_lookup[(day_idx, hour_idx)] = {
                'volume': volume,
                'tx_count': tx_count,
                'enriched_count': enriched
            }
        
        logger.info(f"📋 Created lookup with {len(cell_lookup)} entries")
        
        # Map PostgreSQL DOW to our Monday-first index
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
                cell_data = cell_lookup.get((pg_dow, hour), {'volume': 0, 'tx_count': 0, 'enriched_count': 0})
                volume = cell_data['volume']
                
                day_row.append(volume)
                
                if volume > 0 or cell_data['tx_count'] > 0:
                    peak_data.append((our_day_idx, hour, volume, cell_data['tx_count']))
            
            heatmap_2d.append(day_row)
            
            day_total = sum(day_row)
            non_zero = sum(1 for v in day_row if v > 0)
            logger.info(f"   ✓ {day_name}: {non_zero}/24 active, total=${day_total:,.2f}")
        
        logger.info(f"✅ Heatmap built: 7×24")
        
        # ================================================================
        # STEP 5: DETECT PEAK HOURS
        # ================================================================
        
        logger.info(f"🔍 Step 5: Detecting peaks...")
        
        # Sort by volume (or tx_count if volume is 0)
        peak_data.sort(key=lambda x: (x[2], x[3]), reverse=True)
        
        peak_hours = [
            {
                "day": day_idx,
                "day_name": days[day_idx],
                "hour": hour,
                "value": volume,
                "tx_count": tx_count
            }
            for day_idx, hour, volume, tx_count in peak_data[:5]
            if volume > 0 or tx_count > 0
        ]
        
        logger.info(f"✅ Found {len(peak_hours)} peaks:")
        for i, peak in enumerate(peak_hours[:3], 1):
            logger.info(
                f"   {i}. {peak['day_name']} {peak['hour']:02d}:00 - "
                f"${peak['value']:,.2f} ({peak['tx_count']} TXs)"
            )
        
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
                "enriched_transactions": total_enriched,
                "enrichment_rate": enrichment_rate,
                "days": 7,
                "hours_per_day": 24,
                "total_cells": 168,
                "non_zero_cells": non_zero_cells,
                "data_source": "transactions_table",
                "aggregation_method": "postgres_extract",
                "auto_sync_enabled": auto_sync
            }
        }
        
        logger.info("=" * 80)
        logger.info("✅ HEATMAP READY (TRANSACTION-BASED + AUTO-SYNC)")
        logger.info(f"   Total volume: ${total_volume:,.2f}")
        logger.info(f"   Total TXs: {total_txs:,}")
        logger.info(f"   Enriched: {total_enriched:,} ({enrichment_rate:.1f}%)")
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
