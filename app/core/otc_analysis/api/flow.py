"""
Flow & Timeline Endpoints
==========================

Money flow tracing, Sankey diagrams, and transfer timelines.

This module handles all flow-related endpoints:
- Sankey flow visualization (WITH LINK GENERATION)
- Transfer timeline events
- Money flow path tracing
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from typing import Optional, List, Dict
from datetime import datetime, timedelta
from collections import defaultdict

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
# HELPER FUNCTIONS
# ============================================================================

async def _create_links_from_transactions(
    db: Session,
    otc_wallets: List[OTCWallet],
    tx_extractor,
    start_date: datetime,
    end_date: datetime,
    min_flow_size: float = 100000
) -> List[Dict]:
    """
    🔗 CREATE LINKS from actual blockchain transactions between OTC desks
    
    This function:
    1. Gets all transactions for each OTC desk
    2. Finds transactions where BOTH from AND to are OTC desks
    3. Aggregates flows between desk pairs
    4. Returns formatted Sankey links
    
    Args:
        db: Database session
        otc_wallets: List of OTC wallet objects
        tx_extractor: Transaction extractor service
        start_date: Filter transactions after this date
        end_date: Filter transactions before this date
        min_flow_size: Minimum flow volume in USD
        
    Returns:
        List of Sankey links with source, target, value
    """
    logger.info(f"🔗 Creating links from blockchain transactions...")
    
    # Create mapping of address -> desk info
    desk_map = {
        wallet.address.lower(): {
            "name": wallet.label or f"{wallet.address[:8]}...",
            "address": wallet.address,
            "entity_type": wallet.entity_type
        }
        for wallet in otc_wallets
    }
    
    otc_addresses = set(desk_map.keys())
    logger.info(f"   Analyzing {len(otc_addresses)} OTC desk addresses")
    
    # Aggregate flows between desks
    flows = defaultdict(lambda: {"value": 0, "count": 0, "transactions": []})
    
    # Extract transactions for each OTC desk
    for wallet in otc_wallets[:10]:  # Limit to 10 desks to avoid API rate limits
        try:
            logger.info(f"   📡 Fetching transactions for {wallet.label or wallet.address[:10]}...")
            
            transactions = tx_extractor.extract_wallet_transactions(
                wallet.address,
                include_internal=True,
                include_tokens=True
            )
            
            logger.info(f"      Found {len(transactions)} transactions")
            
            # Filter and analyze transactions
            for tx in transactions:
                try:
                    # Parse timestamp
                    tx_time = tx.get('timestamp')
                    if isinstance(tx_time, str):
                        tx_time = datetime.fromisoformat(tx_time.replace('Z', '+00:00'))
                    elif isinstance(tx_time, int):
                        tx_time = datetime.fromtimestamp(tx_time)
                    
                    # Skip if outside date range
                    if tx_time and (tx_time < start_date or tx_time > end_date):
                        continue
                    
                    # Get addresses
                    from_addr = tx.get('from', '').lower()
                    to_addr = tx.get('to', '').lower()
                    
                    # Check if both addresses are OTC desks
                    if from_addr in otc_addresses and to_addr in otc_addresses and from_addr != to_addr:
                        # Get USD value
                        value_usd = tx.get('value_usd', 0) or tx.get('valueUSD', 0)
                        
                        if not value_usd and tx.get('value'):
                            # Try to calculate USD value
                            eth_value = float(tx.get('value', 0)) / 1e18
                            value_usd = eth_value * 2000  # Rough ETH price estimate
                        
                        if value_usd >= min_flow_size:
                            # Create flow key
                            from_desk = desk_map[from_addr]["name"]
                            to_desk = desk_map[to_addr]["name"]
                            flow_key = (from_desk, to_desk)
                            
                            # Aggregate
                            flows[flow_key]["value"] += value_usd
                            flows[flow_key]["count"] += 1
                            flows[flow_key]["transactions"].append({
                                "hash": tx.get('hash') or tx.get('tx_hash'),
                                "value_usd": value_usd,
                                "timestamp": tx_time.isoformat() if tx_time else None
                            })
                            
                            logger.info(f"      🔗 Flow: {from_desk} → {to_desk}: ${value_usd:,.0f}")
                
                except Exception as tx_error:
                    logger.debug(f"      Skipping transaction: {tx_error}")
                    continue
                    
        except Exception as e:
            logger.warning(f"   ⚠️ Error fetching transactions for {wallet.address[:10]}: {e}")
            continue
    
    # Convert to Sankey links format
    links = []
    for (source, target), data in flows.items():
        if data["value"] >= min_flow_size:
            links.append({
                "source": source,
                "target": target,
                "value": data["value"],
                "transaction_count": data["count"]
            })
    
    logger.info(f"✅ Created {len(links)} links (min_flow_size: ${min_flow_size:,.0f})")
    
    # Log some examples
    for link in links[:3]:
        logger.info(f"   📊 {link['source']} → {link['target']}: ${link['value']:,.0f} ({link['transaction_count']} txs)")
    
    return links


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
    generate_links: bool = Query(True, description="Generate links from blockchain data (slower but shows actual flows)"),
    db: Session = Depends(get_db),
    tx_extractor = Depends(get_transaction_extractor)
):
    """
    Get Sankey flow diagram data WITH LINKS.
    
    GET /api/otc/sankey?start_date=2024-12-01&end_date=2024-12-28&min_flow_size=100000&generate_links=true
    
    Parameters:
        start_date: ISO format date (e.g., '2024-12-01')
        end_date: ISO format date (e.g., '2024-12-28')
        min_flow_size: Minimum flow value in USD (default: 100000)
        generate_links: If true, analyzes blockchain transactions to create actual links (default: true)
    
    Returns:
        nodes: List of wallet nodes with volume data
        links: List of flow connections between wallets (from blockchain transactions)
        metadata: Period info and statistics
    
    The Sankey diagram visualizes money flows between wallets:
    - Node size = Total wallet volume
    - Link width = Transaction volume between wallets
    - Links are created by analyzing actual blockchain transactions
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
        
        logger.info(f"💱 GET /sankey: {start.date()} to {end.date()}, min_flow=${min_flow_size:,.0f}, generate_links={generate_links}")
        
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
        
        # ✅ GENERATE LINKS from blockchain transactions
        links = []
        if generate_links and len(wallets) > 1:
            try:
                links = await _create_links_from_transactions(
                    db=db,
                    otc_wallets=wallets,
                    tx_extractor=tx_extractor,
                    start_date=start,
                    end_date=end,
                    min_flow_size=min_flow_size
                )
            except Exception as link_error:
                logger.error(f"⚠️ Error generating links: {link_error}", exc_info=True)
                logger.info(f"   Continuing with nodes only...")
        
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
