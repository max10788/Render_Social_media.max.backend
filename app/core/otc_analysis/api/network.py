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
from sqlalchemy import func, or_, case, and_  # ✅ and_ hinzugefügt!
from typing import Dict, Optional, Any, List, Set
from datetime import datetime, timedelta

from .dependencies import (
    get_db, 
    get_link_builder,
    ensure_registry_wallets_in_db
)

from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
from app.core.otc_analysis.models.transaction import Transaction  # ✅ NEW
from app.core.otc_analysis.models.wallet_link import WalletLink
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
    min_flow_size: float = Query(10000),  # ✅ Lowered from $100k to $10k
    min_confidence: float = Query(0),
    include_otc: bool = Query(True, description="Include OTC desks"),
    include_wallets: bool = Query(True, description="Include high volume wallets"),
    # ✨ Link generation parameters
    generate_links: bool = Query(True, description="Generate links between wallets"),
    use_saved_links: bool = Query(True, description="Include saved links from database"),
    use_discovery: bool = Query(True, description="Use discovery data for link generation"),
    use_transactions: bool = Query(True, description="Use blockchain transactions for links"),  # ✅ ENABLED real transaction analysis
    min_link_strength: float = Query(0, description="Minimum link strength for saved links"),
    db: Session = Depends(get_db),
    link_builder = Depends(get_link_builder)
):
    """
    ✅ ENHANCED: Complete network graph with nodes AND links in one endpoint.
    
    GET /api/otc/network?start_date=2024-12-01&end_date=2024-12-28&generate_links=true
    
    ✨ ALL-IN-ONE ENDPOINT:
    - Returns both nodes (OTC desks + high volume wallets) AND links
    - Multiple link sources: saved DB links + generated links + transaction links
    - Fast link generation via LinkBuilder with caching
    - Sankey-ready format for visualization
    
    Parameters:
        start_date: ISO format date
        end_date: ISO format date
        max_nodes: Maximum nodes to return
        min_flow_size: Minimum wallet volume / link volume in USD
        min_confidence: Minimum confidence score for nodes
        include_otc: Include OTC desk nodes
        include_wallets: Include high volume wallet nodes
        generate_links: Generate links using LinkBuilder (default: true)
        use_saved_links: Include saved links from database (default: true)
        use_discovery: Use discovery data for fast links (default: true)
        use_transactions: Use blockchain for accurate links (default: false)
        min_link_strength: Minimum strength for saved links (default: 0)
    
    Returns:
        nodes: All wallet nodes with metadata
        edges: Cytoscape-formatted edges
        sankeyNodes: Sankey diagram nodes
        sankeyLinks: Sankey diagram links (✨ NOW FULLY POPULATED!)
        metadata: Complete statistics and data sources
    """
    try:
        # AUTO-SYNC: Ensure registry wallets are in database
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
        logger.info(f"   • min_flow=${min_flow_size:,.0f}, min_confidence={min_confidence}")
        logger.info(f"   • generate_links={generate_links}, use_saved_links={use_saved_links}")
        logger.info(f"   • use_discovery={use_discovery}, use_transactions={use_transactions}")
        
        # ====================================================================
        # STEP 1: QUERY NODES
        # ====================================================================
        
        base_query = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end,
            OTCWallet.confidence_score >= min_confidence
        )
        
        # Filter by entity type
        entity_filters = []
        if include_otc:
            entity_filters.append(
                OTCWallet.entity_type.in_([
                    'otc_desk', 'exchange', 'institutional', 
                    'cold_wallet', 'hot_wallet'
                ])
            )
        if include_wallets:
            entity_filters.append(OTCWallet.entity_type == 'high_volume_wallet')
        
        if entity_filters:
            base_query = base_query.filter(or_(*entity_filters))
        
        all_wallets = base_query.order_by(
            OTCWallet.total_volume.desc()
        ).limit(max_nodes).all()
        
        logger.info(f"   Found {len(all_wallets)} total wallets")
        
        # Separate by type
        otc_wallets = [
            w for w in all_wallets 
            if w.entity_type in ['otc_desk', 'exchange', 'institutional', 'cold_wallet', 'hot_wallet']
        ]
        high_volume_wallets = [
            w for w in all_wallets 
            if w.entity_type == 'high_volume_wallet'
        ]
        
        logger.info(f"   • {len(otc_wallets)} OTC desks")
        logger.info(f"   • {len(high_volume_wallets)} high volume wallets")
        
        # ====================================================================
        # STEP 2: FORMAT NODES
        # ====================================================================
        
        def wallet_to_node(w):
            """Format wallet for nodes array"""
            base = {
                "address": w.address,
                "label": w.label or f"Discovered {w.address[:8]}",
                "entity_type": w.entity_type,
                "node_type": w.entity_type,
                "entity_name": w.entity_name or w.label or f"Discovered {w.address[:8]}",
                "total_volume_usd": float(w.total_volume or 0),
                "transaction_count": w.transaction_count or 0,
                "confidence_score": float(w.confidence_score or 0),
                "is_active": w.is_active,
                "tags": w.tags or []
            }
            
            # Add high_volume_wallet specific fields
            if w.entity_type == 'high_volume_wallet':
                # Extract classification from tags
                classification = None
                for tag in (w.tags or []):
                    if tag in ['mega_whale', 'whale', 'institutional', 'large_wallet', 'medium_wallet']:
                        classification = tag
                        break
                
                base.update({
                    "classification": classification,
                    "volume_score": float(w.confidence_score or 0),
                    "categorized_tags": _categorize_wallet_tags(w.tags or []),
                    "first_seen": w.first_seen.isoformat() if hasattr(w, 'first_seen') and w.first_seen else None,
                    "last_active": w.last_active.isoformat() if w.last_active else None
                })
            
            return base
        
        def wallet_to_sankey_node(w):
            """Format wallet for Sankey diagram"""
            return {
                "name": w.label or f"{w.address[:8]}...",
                "category": _get_category_from_wallet(w),
                "value": float(w.total_volume or 0),
                "address": w.address,
                "confidence": float(w.confidence_score or 0)
            }
        
        nodes = [wallet_to_node(w) for w in all_wallets]
        sankey_nodes = [wallet_to_sankey_node(w) for w in all_wallets]
        
        # ====================================================================
        # ✨ STEP 3: GENERATE AND COLLECT ALL LINKS
        # ====================================================================
        
        cytoscape_edges = []
        sankey_links = []
        link_metadata = {
            "saved_links_count": 0,
            "dynamic_links_count": 0,
            "transaction_links_count": 0,
            "total_sources": [],
            "source": "none",
            "cached": False,
            "discovery_used": False,
            "transactions_used": False
        }

        # ====================================================================
        # AUTO-SYNC: Fetch transactions if using transaction-based links
        # ====================================================================

        if generate_links and use_transactions and len(all_wallets) > 0:
            logger.info("   🔄 Checking if transaction sync is needed...")

            # Check if we have any transactions for these wallets
            wallet_addresses_check = [w.address.lower() for w in all_wallets[:10]]  # Check first 10
            tx_check = db.query(Transaction).filter(
                or_(
                    Transaction.from_address.in_(wallet_addresses_check),
                    Transaction.to_address.in_(wallet_addresses_check)
                )
            ).first()

            if not tx_check:
                logger.warning("   ⚠️ No transactions found - triggering AUTO-SYNC")
                logger.info("   📡 Syncing transactions for wallets...")

                try:
                    from app.core.otc_analysis.api.dependencies import sync_wallet_transactions_to_db

                    sync_count = 0
                    for wallet in all_wallets[:10]:  # Sync first 10 wallets
                        logger.info(f"   📡 Syncing {wallet.entity_name or wallet.address[:10]}...")
                        try:
                            stats = await sync_wallet_transactions_to_db(
                                wallet_address=wallet.address,
                                start_date=start,
                                end_date=end,
                                db=db,
                                limit=100
                            )
                            if stats.get("saved", 0) > 0:
                                sync_count += stats["saved"]
                        except Exception as wallet_error:
                            logger.error(f"      ❌ Sync failed for {wallet.address[:10]}: {wallet_error}")
                            continue

                    logger.info(f"   ✅ Auto-sync complete: {sync_count} transactions saved")

                except Exception as sync_error:
                    logger.error(f"   ❌ Auto-sync failed: {sync_error}", exc_info=True)
            else:
                logger.info("   ✅ Transactions already exist, skipping auto-sync")

        if generate_links and len(all_wallets) > 1:
            wallet_addresses = [w.address.lower() for w in all_wallets]
            
            # Create address to label mapping for Sankey
            addr_to_label = {
                w.address.lower(): w.label or f"{w.address[:8]}..."
                for w in all_wallets
            }
            
            # Track unique links to avoid duplicates
            unique_links: Set[tuple] = set()
            
            # ================================================================
            # 3A: SAVED LINKS FROM DATABASE
            # ================================================================
            
            if use_saved_links:
                try:
                    from app.core.otc_analysis.models.wallet_link import WalletLink
                    
                    logger.info(f"   💾 Loading saved links from database...")
                    
                    saved_links = db.query(WalletLink).filter(
                        WalletLink.is_active == True,
                        WalletLink.link_strength >= min_link_strength,
                        WalletLink.total_volume_usd >= min_flow_size,
                        WalletLink.from_address.in_(wallet_addresses),
                        WalletLink.to_address.in_(wallet_addresses)
                    ).order_by(WalletLink.total_volume_usd.desc()).limit(200).all()
                    
                    logger.info(f"   ✅ Found {len(saved_links)} saved links")
                    
                    for link in saved_links:
                        from_addr = link.from_address.lower()
                        to_addr = link.to_address.lower()
                        link_pair = (from_addr, to_addr)
                        
                        if link_pair not in unique_links:
                            unique_links.add(link_pair)
                            
                            # Cytoscape edge
                            cytoscape_edges.append({
                                "data": {
                                    "source": from_addr,
                                    "target": to_addr,
                                    "transfer_amount_usd": float(link.total_volume_usd or 0),
                                    "transaction_count": link.transaction_count or 0,
                                    "link_strength": float(link.link_strength or 0),
                                    "is_suspected_otc": link.is_suspected_otc or False,
                                    "edge_source": "saved_link",
                                    "data_quality": link.data_quality,
                                    "detected_patterns": link.detected_patterns or []
                                }
                            })
                            
                            # Sankey link
                            sankey_links.append({
                                "source": addr_to_label.get(from_addr, from_addr[:8]),
                                "target": addr_to_label.get(to_addr, to_addr[:8]),
                                "value": float(link.total_volume_usd or 0),
                                "from_address": from_addr,
                                "to_address": to_addr,
                                "transaction_count": link.transaction_count or 0,
                                "link_strength": float(link.link_strength or 0),
                                "is_suspected_otc": link.is_suspected_otc or False,
                                "source_type": "database"
                            })
                    
                    link_metadata["saved_links_count"] = len(saved_links)
                    link_metadata["total_sources"].append("saved_links")
                    
                except ImportError as e:
                    logger.warning(f"   ⚠️ WalletLink model not found - skipping saved links")
                    db.rollback()  # ✨ WICHTIG: Rollback der Transaction
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # Check if it's a "table does not exist" error
                    if 'does not exist' in error_msg or 'UndefinedTable' in error_msg:
                        logger.warning(f"   ⚠️ wallet_links table not found - run migration first!")
                        logger.info(f"   💡 Tip: Execute scripts/migrations/create_wallet_links_table.sql")
                    else:
                        logger.warning(f"   ⚠️ Error loading saved links: {e}")
                    
                    # ✨ CRITICAL: Rollback transaction to continue with other queries
                    db.rollback()
            # ================================================================
            # 3B: GENERATED LINKS VIA LINKBUILDER
            # ================================================================
            
            if len(all_wallets) > 1:
                try:
                    logger.info(f"   🔗 Generating links via LinkBuilder...")
                    
                    result = link_builder.build_links(
                        db=db,
                        wallets=all_wallets,
                        start_date=start,
                        end_date=end,
                        min_flow_size=min_flow_size,
                        use_discovery=use_discovery,
                        use_transactions=use_transactions
                    )
                    
                    # Extract generated links
                    generated_edges = result.get("cytoscape_edges", [])
                    generated_sankey = result.get("sankey_links", [])
                    lb_metadata = result.get("metadata", {})
                    
                    # Update metadata
                    link_metadata["source"] = lb_metadata.get("source", "unknown")
                    link_metadata["cached"] = lb_metadata.get("cached", False)
                    link_metadata["discovery_used"] = lb_metadata.get("discovery_used", False)
                    link_metadata["transactions_used"] = lb_metadata.get("transactions_used", False)
                    
                    # Add generated edges (avoid duplicates)
                    new_count = 0
                    for edge in generated_edges:
                        from_addr = edge["data"]["source"].lower()
                        to_addr = edge["data"]["target"].lower()
                        link_pair = (from_addr, to_addr)
                        
                        if link_pair not in unique_links:
                            unique_links.add(link_pair)
                            edge["data"]["edge_source"] = "generated"
                            cytoscape_edges.append(edge)
                            new_count += 1
                    
                    # Add generated sankey links (avoid duplicates)
                    for link in generated_sankey:
                        from_addr = link.get("from_address", "").lower()
                        to_addr = link.get("to_address", "").lower()
                        link_pair = (from_addr, to_addr)
                        
                        if link_pair not in unique_links:
                            unique_links.add(link_pair)
                            link["source_type"] = "generated"
                            sankey_links.append(link)
                    
                    link_metadata["dynamic_links_count"] = new_count
                    if new_count > 0:
                        link_metadata["total_sources"].append("generated")
                    
                    logger.info(
                        f"   ✅ LinkBuilder: {len(generated_edges)} total, {new_count} new "
                        f"(cached: {lb_metadata.get('cached', False)})"
                    )
                    
                except Exception as e:
                    logger.error(f"   ⚠️ Error generating links: {e}", exc_info=True)
            
            # ================================================================
            # 3C: TRANSACTION-BASED LINKS (Wallet ↔ OTC)
            # ================================================================
            
            if use_transactions and len(high_volume_wallets) > 0 and len(otc_wallets) > 0:
                try:
                    logger.info(f"   📊 Generating Wallet ↔ OTC links from transactions...")
                    
                    wallet_addrs = [w.address.lower() for w in high_volume_wallets]
                    otc_addrs = [w.address.lower() for w in otc_wallets]
                    
                    from app.core.otc_analysis.models.transaction import Transaction
                    
                    tx_links = db.query(
                        Transaction.from_address,
                        Transaction.to_address,
                        func.count(Transaction.tx_hash).label('tx_count'),
                        func.coalesce(func.sum(Transaction.usd_value), 0).label('total_volume')
                    ).filter(
                        Transaction.timestamp >= start,
                        Transaction.timestamp <= end,
                        or_(
                            and_(
                                Transaction.from_address.in_(wallet_addrs),
                                Transaction.to_address.in_(otc_addrs)
                            ),
                            and_(
                                Transaction.from_address.in_(otc_addrs),
                                Transaction.to_address.in_(wallet_addrs)
                            )
                        )
                    ).group_by(
                        Transaction.from_address,
                        Transaction.to_address
                    ).having(
                        func.coalesce(func.sum(Transaction.usd_value), 0) >= min_flow_size
                    ).all()
                    
                    tx_count = 0
                    for tx_link in tx_links:
                        from_addr = tx_link.from_address.lower()
                        to_addr = tx_link.to_address.lower()
                        link_pair = (from_addr, to_addr)
                        
                        if link_pair not in unique_links:
                            unique_links.add(link_pair)
                            
                            # Cytoscape edge
                            cytoscape_edges.append({
                                "data": {
                                    "source": from_addr,
                                    "target": to_addr,
                                    "transfer_amount_usd": float(tx_link.total_volume or 0),
                                    "transaction_count": tx_link.tx_count,
                                    "is_suspected_otc": False,
                                    "edge_source": "transactions"
                                }
                            })
                            
                            # Sankey link
                            sankey_links.append({
                                "source": addr_to_label.get(from_addr, from_addr[:8]),
                                "target": addr_to_label.get(to_addr, to_addr[:8]),
                                "value": float(tx_link.total_volume or 0),
                                "from_address": from_addr,
                                "to_address": to_addr,
                                "transaction_count": tx_link.tx_count,
                                "source_type": "transactions"
                            })
                            
                            tx_count += 1
                    
                    link_metadata["transaction_links_count"] = tx_count
                    if tx_count > 0:
                        link_metadata["total_sources"].append("transactions")
                    
                    logger.info(f"   ✅ Added {tx_count} transaction-based links")
                    
                except Exception as e:
                    logger.error(f"   ⚠️ Error generating transaction links: {e}", exc_info=True)
        
        # ====================================================================
        # STEP 4: RETURN COMPLETE RESPONSE
        # ====================================================================
        
        logger.info(
            f"✅ Network: {len(nodes)} nodes, {len(cytoscape_edges)} edges, "
            f"{len(sankey_links)} sankey links"
        )
        logger.info(
            f"   Links breakdown: "
            f"saved={link_metadata['saved_links_count']}, "
            f"dynamic={link_metadata['dynamic_links_count']}, "
            f"transactions={link_metadata['transaction_links_count']}"
        )
        
        return {
            "nodes": nodes,
            "edges": cytoscape_edges,
            "sankeyNodes": sankey_nodes,
            "sankeyLinks": sankey_links,  # ✨ NOW FULLY POPULATED!
            "metadata": {
                "node_count": len(nodes),
                "otc_desk_count": len(otc_wallets),
                "wallet_count": len(high_volume_wallets),
                "edge_count": len(cytoscape_edges),
                "link_count": len(sankey_links),
                # ✨ Link source breakdown
                "saved_links_count": link_metadata["saved_links_count"],
                "dynamic_links_count": link_metadata["dynamic_links_count"],
                "transaction_links_count": link_metadata["transaction_links_count"],
                "edge_sources": link_metadata["total_sources"],
                # ✨ Link generation metadata (from LinkBuilder)
                "link_source": link_metadata["source"],
                "link_cached": link_metadata["cached"],
                "discovery_used": link_metadata["discovery_used"],
                "transactions_used": link_metadata["transactions_used"],
                # Period and filters
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                },
                "use_saved_links": use_saved_links,
                "min_flow_size": min_flow_size,
                "min_link_strength": min_link_strength
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
