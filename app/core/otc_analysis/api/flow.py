"""
Flow & Timeline Endpoints
==========================

Money flow tracing, Sankey diagrams, and transfer timelines.

This module handles all flow-related endpoints:
- Sankey flow visualization (✨ WITH FAST LINK GENERATION via LinkBuilder)
- Transfer timeline events
- Money flow path tracing

✨ NEW IN v2.0:
- Uses LinkBuilder for fast link generation
- 5-minute caching
- Discovery data + transaction fallback
- No more slow _create_links_from_transactions()
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from typing import Optional, List, Dict
from datetime import datetime, timedelta

from .dependencies import (
    get_db,
    get_transaction_extractor,
    get_price_oracle,
    get_link_builder,  # ✨ NEW
    flow_tracer,
    ensure_registry_wallets_in_db
)

from app.core.otc_analysis.api.validators import validate_ethereum_address, FlowTraceRequest
from app.core.otc_analysis.models.wallet import Wallet as OTCWallet

logger = logging.getLogger(__name__)

# Flow router - no prefix, URLs added via /api/otc in main.py
flow_router = APIRouter(prefix="", tags=["Flow"])


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _get_category_from_wallet(wallet: OTCWallet) -> str:
    """
    Determine display category from wallet entity type
    
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
# FLOW ENDPOINTS
# ============================================================================

@flow_router.get("/sankey")
async def get_sankey_flow(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    min_flow_size: float = Query(100000),
    generate_links: bool = Query(True, description="Generate links from data (discovery + blockchain)"),
    use_discovery: bool = Query(True, description="Use discovery data for fast link generation"),
    use_transactions: bool = Query(False, description="Use blockchain transactions (slower but complete)"),
    db: Session = Depends(get_db),
    link_builder = Depends(get_link_builder)  # ✨ NEW
):
    """
    Get Sankey flow diagram data WITH FAST LINK GENERATION.
    
    GET /api/otc/sankey?start_date=2024-12-01&end_date=2024-12-28&min_flow_size=100000&generate_links=true
    
    ✨ NEW IN v2.0:
    - Uses LinkBuilder for fast, cached link generation
    - Multiple data sources: discovery (fast) + blockchain (slow)
    - 5-minute caching for performance
    - Returns meaningful links even with 0 blockchain TXs (from discovery data)
    
    Parameters:
        start_date: ISO format date (e.g., '2024-12-01')
        end_date: ISO format date (e.g., '2024-12-28')
        min_flow_size: Minimum flow value in USD (default: 100000)
        generate_links: If true, generates links between wallets (default: true)
        use_discovery: Use discovery data for fast link generation (default: true)
        use_transactions: Use blockchain transactions for accurate flows (default: false)
    
    Returns:
        nodes: List of wallet nodes with volume data
        links: List of flow connections between wallets (✨ NOW GENERATED!)
        metadata: Period info, statistics, and data sources
    
    The Sankey diagram visualizes money flows between wallets:
    - Node size = Total wallet volume
    - Link width = Transaction volume between wallets
    - Links created from discovery data (fast) or blockchain (accurate)
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
        
        logger.info(f"💱 GET /sankey: {start.date()} to {end.date()}, min_flow=${min_flow_size:,.0f}")
        logger.info(f"   • generate_links={generate_links}, use_discovery={use_discovery}, use_transactions={use_transactions}")
        
        # Query wallets with significant volume in the time range
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end,
            OTCWallet.total_volume >= min_flow_size
        ).order_by(OTCWallet.total_volume.desc()).limit(20).all()
        
        logger.info(f"   Found {len(wallets)} wallets matching criteria")
        
        # Format nodes for Sankey diagram
        nodes = [
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
        # ✨ NEW: USE LINKBUILDER FOR FAST LINK GENERATION
        # ====================================================================
        
        links = []
        link_metadata = {}
        
        if generate_links and len(wallets) > 1:
            try:
                logger.info(f"   🔗 Building links using LinkBuilder...")
                
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
                
                # Extract Sankey links
                links = result.get("sankey_links", [])
                link_metadata = result.get("metadata", {})
                
                logger.info(
                    f"   ✅ LinkBuilder: {len(links)} links "
                    f"(source: {link_metadata.get('source', 'unknown')}, "
                    f"cached: {link_metadata.get('cached', False)})"
                )
                
            except Exception as link_error:
                logger.error(f"   ⚠️ Error generating links: {link_error}", exc_info=True)
                logger.info(f"   Continuing with nodes only...")
        
        # ====================================================================
        # RETURN RESULT
        # ====================================================================
        
        logger.info(f"✅ Sankey: {len(nodes)} nodes, {len(links)} links")
        
        return {
            "nodes": nodes,
            "links": links,
            "metadata": {
                "node_count": len(nodes),
                "link_count": len(links),
                "min_flow_size": min_flow_size,
                "links_generated": generate_links,
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                },
                # ✨ NEW: Link generation metadata
                "link_source": link_metadata.get("source", "none"),
                "link_cached": link_metadata.get("cached", False),
                "discovery_used": link_metadata.get("discovery_used", False),
                "transactions_used": link_metadata.get("transactions_used", False)
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
    db: Session = Depends(get_db),
    tx_extractor = Depends(get_transaction_extractor)
):
    """
    Get transfer timeline data WITH actual transaction events.
    
    GET /api/otc/timeline?start_date=2024-12-17&end_date=2024-12-24&min_confidence=0.5
    
    Returns:
        events: Chronological list of transfer events from blockchain
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
        ).order_by(OTCWallet.last_active.desc()).limit(20).all()
        
        logger.info(f"   Found {len(wallets)} wallets for timeline")
        
        # ✅ GET ACTUAL TRANSACTIONS for timeline
        events = []
        
        for wallet in wallets[:10]:  # Limit to 10 wallets to avoid slowness
            try:
                logger.info(f"   📡 Fetching timeline for {wallet.label or wallet.address[:10]}...")
                
                transactions = tx_extractor.extract_wallet_transactions(
                    wallet.address,
                    include_internal=True,
                    include_tokens=True
                )
                
                # Filter by date and format as events
                for tx in transactions[:20]:  # Top 20 transactions per wallet
                    try:
                        # Parse timestamp
                        tx_time = tx.get('timestamp')
                        if isinstance(tx_time, str):
                            tx_time = datetime.fromisoformat(tx_time.replace('Z', '+00:00'))
                        elif isinstance(tx_time, int):
                            tx_time = datetime.fromtimestamp(tx_time)
                        
                        # Skip if outside date range
                        if tx_time and (tx_time < start or tx_time > end):
                            continue
                        
                        # Get USD value
                        value_usd = tx.get('value_usd', 0) or tx.get('valueUSD', 0)
                        if not value_usd and tx.get('value'):
                            eth_value = float(tx.get('value', 0)) / 1e18
                            value_usd = eth_value * 2000
                        
                        events.append({
                            "timestamp": tx_time.isoformat() if tx_time else datetime.now().isoformat(),
                            "from_address": tx.get('from', wallet.address),
                            "to_address": tx.get('to', ''),
                            "amount_usd": value_usd,
                            "token": tx.get('tokenSymbol') or 'ETH',
                            "tx_hash": tx.get('hash') or tx.get('tx_hash'),
                            "confidence_score": float(wallet.confidence_score or 0),
                            "entity_type": wallet.entity_type,
                            "label": wallet.label
                        })
                        
                    except Exception as tx_error:
                        logger.debug(f"      Skipping transaction: {tx_error}")
                        continue
                        
            except Exception as wallet_error:
                logger.warning(f"   ⚠️ Error fetching timeline for {wallet.address[:10]}: {wallet_error}")
                continue
        
        # Sort by timestamp (newest first)
        events.sort(key=lambda x: x['timestamp'], reverse=True)
        
        logger.info(f"✅ Timeline: {len(events)} events")
        
        return {
            "events": events[:100],  # Return top 100 events
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
