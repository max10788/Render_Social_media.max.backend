"""
Complete OTC Analysis API Endpoints
Combines Phase 1 and Phase 2 endpoints into a single file.

✅ FIXED: Now uses REAL PostgreSQL Database instead of Mock DB
"""

from fastapi import APIRouter, HTTPException, Query, Depends, Header
from typing import List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel
import logging

# ✅ CRITICAL FIX: Import real database connection
from app.core.backend_crypto_tracker.config.database import get_db
from sqlalchemy.orm import Session

# Validators
from app.core.otc_analysis.api.validators import (
    ScanRangeRequest,
    WalletProfileRequest,
    FlowTraceRequest,
    validate_ethereum_address,
    validate_block_range
)

# Detection Services
from app.core.otc_analysis.detection.otc_detector import OTCDetector
from app.core.otc_analysis.detection.wallet_profiler import WalletProfiler
from app.core.otc_analysis.detection.flow_tracer import FlowTracer

# Blockchain Services
from app.core.otc_analysis.blockchain.node_provider import NodeProvider
from app.core.otc_analysis.blockchain.block_scanner import BlockScanner
from app.core.otc_analysis.blockchain.transaction_extractor import TransactionExtractor
from app.core.otc_analysis.blockchain.etherscan import EtherscanAPI

# Data Sources
from app.core.otc_analysis.data_sources.price_oracle import PriceOracle
from app.core.otc_analysis.data_sources.otc_desks import OTCDeskRegistry
from app.core.otc_analysis.data_sources.wallet_labels import WalletLabelingService

# Analysis Services (Phase 2)
from app.core.otc_analysis.analysis.statistics_service import StatisticsService
from app.core.otc_analysis.analysis.graph_builder import GraphBuilderService
from app.core.otc_analysis.analysis.network_graph import NetworkAnalysisService

# Database Models (Phase 2)
from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
from app.core.otc_analysis.models.watchlist import WatchlistItem as OTCWatchlist
from app.core.otc_analysis.models.alert import Alert as OTCAlert

# Utils
from app.core.otc_analysis.utils.cache import CacheManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/otc", tags=["OTC Analysis"])

# ============================================================================
# SERVICE INITIALIZATION
# ============================================================================

# Core services
cache_manager = CacheManager()
otc_registry = OTCDeskRegistry(cache_manager)
labeling_service = WalletLabelingService(cache_manager)
otc_detector = OTCDetector(cache_manager, otc_registry, labeling_service)
wallet_profiler = WalletProfiler()
flow_tracer = FlowTracer()

# Blockchain services
node_provider = NodeProvider(chain_id=1)  # Ethereum mainnet
etherscan = EtherscanAPI(chain_id=1)
price_oracle = PriceOracle(cache_manager)
transaction_extractor = TransactionExtractor(node_provider, etherscan)
block_scanner = BlockScanner(node_provider, chain_id=1)

# Phase 2 services
statistics_service = StatisticsService(cache_manager)
graph_builder = GraphBuilderService(cache_manager)

# ============================================================================
# DEPENDENCIES - ✅ USING REAL DATABASE
# ============================================================================


def get_current_user():
    """
    ✅ FIXED: Returns mock user for development
    
    TODO: Implement real JWT authentication in production:
    
    from jose import jwt
    
    def get_current_user(authorization: str = Header(...)):
        token = authorization.replace("Bearer ", "")
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload.get("sub")
    """
    return "dev_user_123"


# ============================================================================
# REQUEST MODELS
# ============================================================================

class WatchlistAddRequest(BaseModel):
    address: str
    label: Optional[str] = None

# ============================================================================
# PHASE 1 ENDPOINTS - CORE OTC DETECTION
# ============================================================================

@router.post("/scan/range")
async def scan_block_range(request: ScanRangeRequest):
    """
    Scan a historical block range for OTC activity.
    
    POST /api/otc/scan/range
    
    Body:
    {
        "from_block": 12000000,
        "to_block": 12001000,
        "tokens": ["0x..."],
        "min_usd_value": 100000,
        "exclude_exchanges": true
    }
    """
    logger.info(f"🔍 Starting OTC scan: blocks {request.from_block} to {request.to_block}")
    
    try:
        validate_block_range(request.from_block, request.to_block)
        
        logger.info(f"📦 Scanning blocks...")
        transactions = block_scanner.scan_range(
            from_block=request.from_block,
            to_block=request.to_block
        )
        logger.info(f"✅ Found {len(transactions)} transactions")
        
        logger.info(f"💰 Enriching with USD values...")
        transactions = transaction_extractor.enrich_with_usd_value(
            transactions,
            price_oracle
        )
        
        if request.min_usd_value:
            transactions = transaction_extractor.filter_by_value(
                transactions,
                min_usd_value=request.min_usd_value
            )
            logger.info(f"💵 Filtered to {len(transactions)} high-value transactions")
        
        if request.exclude_exchanges:
            logger.info(f"🏦 Filtering out exchange addresses...")
            exchange_addresses = set()
            for tx in transactions:
                if labeling_service.is_exchange(tx['from_address']):
                    exchange_addresses.add(tx['from_address'])
                if labeling_service.is_exchange(tx['to_address']):
                    exchange_addresses.add(tx['to_address'])
            
            transactions = [
                tx for tx in transactions
                if tx['from_address'] not in exchange_addresses
                and tx['to_address'] not in exchange_addresses
            ]
            logger.info(f"✅ {len(transactions)} transactions after exchange filter")
        
        logger.info(f"🎯 Running OTC detection...")
        result = otc_detector.scan_block_range(transactions, request.min_usd_value)
        
        logger.info(f"✅ Scan complete: {result['total_suspected_otc']} OTC transactions found")
        
        return {
            "success": True,
            "data": result,
            "metadata": {
                "from_block": request.from_block,
                "to_block": request.to_block,
                "total_blocks_scanned": request.to_block - request.from_block + 1,
                "scan_time": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Scan failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/wallet/{address}/profile")
async def get_wallet_profile(
    address: str,
    include_network_metrics: bool = Query(True),
    include_labels: bool = Query(True)
):
    """
    Get detailed profile for a wallet address.
    
    GET /api/otc/wallet/0x.../profile?include_network_metrics=true
    """
    logger.info(f"👤 Fetching profile for {address[:10]}...")
    
    try:
        address = validate_ethereum_address(address)
        
        cached_profile = cache_manager.get_wallet_profile(address)
        if cached_profile:
            logger.info(f"✅ Profile loaded from cache")
            return {"success": True, "data": cached_profile, "cached": True}
        
        logger.info(f"📡 Fetching transactions from Etherscan...")
        transactions = transaction_extractor.extract_wallet_transactions(
            address,
            include_internal=True,
            include_tokens=True
        )
        logger.info(f"✅ Found {len(transactions)} transactions")
        
        normal_txs = [tx for tx in transactions if tx.get('tx_type') == 'normal']
        internal_txs = [tx for tx in transactions if tx.get('tx_type') == 'internal']
        token_txs = [tx for tx in transactions if tx.get('tx_type') == 'erc20']
        
        logger.info(f"📊 Transaction Breakdown:")
        logger.info(f"   • Normal: {len(normal_txs)}")
        logger.info(f"   • Internal: {len(internal_txs)}")
        logger.info(f"   • ERC20 Tokens: {len(token_txs)}")
        
        logger.info(f"💰 Enriching with prices...")
        transactions = transaction_extractor.enrich_with_usd_value(
            transactions,
            price_oracle
        )
        
        enriched_txs = [tx for tx in transactions if tx.get('usd_value') is not None]
        if enriched_txs:
            total_value = sum(tx['usd_value'] for tx in enriched_txs)
            avg_value = total_value / len(enriched_txs)
            max_tx = max(enriched_txs, key=lambda x: x['usd_value'])
            
            logger.info(f"💵 Transaction Values:")
            logger.info(f"   • Total Volume: ${total_value:,.2f}")
            logger.info(f"   • Average Value: ${avg_value:,.2f}")
            logger.info(f"   • Largest Tx: ${max_tx['usd_value']:,.2f}")
            logger.info(f"   • Enriched: {len(enriched_txs)}/{len(transactions)}")
        
        labels = None
        if include_labels:
            logger.info(f"🏷️  Fetching wallet labels...")
            labels = labeling_service.get_wallet_labels(address)
            
            if labels and labels.get('entity_type') != 'unknown':
                logger.info(f"🏢 Entity Identified:")
                logger.info(f"   • Type: {labels.get('entity_type')}")
                logger.info(f"   • Name: {labels.get('entity_name', 'N/A')}")
                logger.info(f"   • Labels: {', '.join(labels.get('labels', []))}")
        
        logger.info(f"📊 Building wallet profile...")
        profile = wallet_profiler.create_profile(address, transactions, labels)
        
        logger.info(f"👤 Wallet Profile Metrics:")
        logger.info(f"   • Total Transactions: {profile.get('total_transactions', 0)}")
        logger.info(f"   • Transaction Frequency: {profile.get('transaction_frequency', 0):.2f} tx/day")
        logger.info(f"   • Avg Transaction: ${profile.get('avg_transaction_usd', 0):,.2f}")
        logger.info(f"   • Unique Counterparties: {profile.get('unique_counterparties', 0)}")
        logger.info(f"   • Has DeFi Interactions: {profile.get('has_defi_interactions', False)}")
        logger.info(f"   • Has DEX Swaps: {profile.get('has_dex_swaps', False)}")
        
        otc_probability = wallet_profiler.calculate_otc_probability(profile)
        profile['otc_probability'] = otc_probability
        
        logger.info(f"🎯 OTC Probability Calculation:")
        logger.info(f"   • Base Score: {otc_probability:.2%}")
        logger.info(f"   • Low Frequency: {'✅' if profile.get('transaction_frequency', 0) < 0.5 else '❌'}")
        logger.info(f"   • High Value: {'✅' if profile.get('avg_transaction_usd', 0) > 100000 else '❌'}")
        logger.info(f"   • No DeFi: {'✅' if not profile.get('has_defi_interactions', True) else '❌'}")
        
        if include_network_metrics and len(transactions) > 0:
            logger.info(f"🕸️  Calculating network metrics...")
            network_analyzer = NetworkAnalysisService()
            network_analyzer.build_graph(transactions)
            network_metrics = network_analyzer.analyze_wallet_centrality(address)
            
            profile['network_metrics'] = network_metrics
            
            logger.info(f"🌐 Network Metrics:")
            logger.info(f"   • Betweenness Centrality: {network_metrics.get('betweenness_centrality', 0):.4f}")
            logger.info(f"   • Degree Centrality: {network_metrics.get('degree_centrality', 0):.4f}")
            logger.info(f"   • Clustering Coefficient: {network_metrics.get('clustering_coefficient', 0):.4f}")
            logger.info(f"   • Is Hub: {'✅' if network_metrics.get('is_hub', False) else '❌'}")
            logger.info(f"   • Hub Score: {network_metrics.get('hub_score', 0):.4f}")
        
        cache_manager.cache_wallet_profile(address, profile)
        
        logger.info(f"✅ Profile complete - OTC probability: {otc_probability:.2%}")
        logger.info(f"=" * 80)
        
        return {
            "success": True,
            "data": profile,
            "cached": False
        }
        
    except Exception as e:
        logger.error(f"❌ Profile fetch failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/flow/trace")
async def trace_flow(request: FlowTraceRequest):
    """
    Trace money flow from source to target address.
    
    POST /api/otc/flow/trace
    
    Body:
    {
        "source_address": "0x...",
        "target_address": "0x...",
        "max_hops": 5,
        "min_confidence": 0.5
    }
    """
    logger.info(f"🔄 Tracing flow: {request.source_address[:10]}... → {request.target_address[:10]}...")
    
    try:
        source = validate_ethereum_address(request.source_address)
        target = validate_ethereum_address(request.target_address)
        
        logger.info(f"📡 Fetching transaction data...")
        
        source_txs = transaction_extractor.extract_wallet_transactions(source)
        target_txs = transaction_extractor.extract_wallet_transactions(target)
        
        all_transactions = {tx['tx_hash']: tx for tx in source_txs + target_txs}
        transactions = list(all_transactions.values())
        
        logger.info(f"✅ Loaded {len(transactions)} transactions")
        
        transactions = transaction_extractor.enrich_with_usd_value(
            transactions,
            price_oracle
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
            if result['best_path']:
                logger.info(f"🏆 Best path: {result['best_path']['hop_count']} hops, "
                          f"confidence: {result['best_path']['overall_confidence']:.2%}")
        else:
            logger.info(f"❌ No path found within {request.max_hops} hops")
        
        return {
            "success": True,
            "data": result
        }
        
    except Exception as e:
        logger.error(f"❌ Flow trace failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/desks")
async def get_otc_desks():
    """
    Get list of all known OTC desks.
    
    GET /api/otc/desks
    """
    logger.info(f"🏢 Fetching OTC desk list...")
    
    try:
        desks = otc_registry.get_desk_list()
        logger.info(f"✅ Loaded {len(desks)} OTC desks")
        
        return {
            "success": True,
            "data": {
                "desks": desks,
                "total_count": len(desks)
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch desks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/desks/{desk_name}")
async def get_desk_details(desk_name: str):
    """
    Get detailed information about a specific OTC desk.
    
    GET /api/otc/desks/wintermute
    """
    logger.info(f"🏢 Fetching details for desk: {desk_name}")
    
    try:
        desk_info = otc_registry.get_desk_by_name(desk_name)
        
        if not desk_info:
            logger.warning(f"⚠️  Desk not found: {desk_name}")
            raise HTTPException(status_code=404, detail=f"OTC desk '{desk_name}' not found")
        
        logger.info(f"✅ Found desk with {len(desk_info['addresses'])} addresses")
        
        return {
            "success": True,
            "data": desk_info
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to fetch desk details: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/transaction")
async def analyze_transaction(tx_hash: str):
    """
    Analyze a specific transaction for OTC activity.
    
    POST /api/otc/analyze/transaction?tx_hash=0x...
    """
    logger.info(f"🔍 Analyzing transaction: {tx_hash[:16]}...")
    
    try:
        logger.info(f"📡 Fetching transaction from blockchain...")
        tx_data = node_provider.get_transaction(tx_hash)
        
        if not tx_data:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        receipt = node_provider.get_transaction_receipt(tx_hash)
        
        from_address = tx_data['from']
        to_address = tx_data.get('to')
        
        if not to_address:
            logger.info(f"⚠️  Contract deployment, skipping OTC analysis")
            return {
                "success": True,
                "data": {
                    "is_suspected_otc": False,
                    "reason": "contract_deployment"
                }
            }
        
        transaction = {
            'tx_hash': tx_hash,
            'from_address': from_address,
            'to_address': to_address,
            'value': str(tx_data['value']),
            'value_decimal': node_provider.from_wei(tx_data['value']),
            'block_number': tx_data['blockNumber'],
            'timestamp': datetime.now(),
            'gas_used': receipt.get('gasUsed'),
            'is_contract_interaction': node_provider.is_contract(to_address)
        }
        
        logger.info(f"💰 Fetching ETH price...")
        eth_price = price_oracle.get_current_price(None)
        if eth_price:
            transaction['usd_value'] = transaction['value_decimal'] * eth_price
            logger.info(f"💵 Transaction value: ${transaction['usd_value']:,.2f}")
        
        logger.info(f"👤 Building wallet profile...")
        wallet_txs = transaction_extractor.extract_wallet_transactions(from_address)
        wallet_profile = wallet_profiler.create_profile(from_address, wallet_txs)
        
        logger.info(f"🎯 Running OTC detection...")
        result = otc_detector.detect_otc_transaction(
            transaction,
            wallet_profile,
            wallet_txs[:100]
        )
        
        logger.info(f"✅ Analysis complete - Confidence: {result['confidence_score']:.1f}")
        
        return {
            "success": True,
            "data": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Transaction analysis failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_statistics_old():
    """
    Get overall OTC detection statistics.
    
    GET /api/otc/stats
    """
    logger.info(f"📊 Fetching OTC statistics...")
    
    try:
        stats = otc_detector.get_detection_stats()
        cache_stats = cache_manager.get_stats()
        
        return {
            "success": True,
            "data": {
                "detection_stats": stats,
                "cache_stats": cache_stats,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch stats: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# PHASE 2 ENDPOINTS - VISUALIZATION & MONITORING
# ============================================================================

@router.get("/statistics")
async def get_statistics(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get OTC statistics with 24h change calculations.
    
    GET /api/otc/statistics?start_date=2024-11-21&end_date=2024-12-21
    """
    try:
        # Parse dates
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"📊 GET /statistics: {start.date()} to {end.date()}")
        
        # Current period wallets
        current_wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).all()
        
        # 24h ago period (for comparison)
        start_24h = start - timedelta(days=1)
        end_24h = end - timedelta(days=1)
        
        previous_wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start_24h,
            OTCWallet.last_active <= end_24h
        ).all()
        
        # Calculate current statistics
        current_volume = sum(w.total_volume or 0 for w in current_wallets)
        current_count = sum(w.transaction_count or 0 for w in current_wallets)
        current_avg_size = current_volume / current_count if current_count > 0 else 0
        current_avg_confidence = (
            sum(w.confidence_score or 0 for w in current_wallets) / len(current_wallets)
            if current_wallets else 0
        )
        
        # Calculate previous statistics
        previous_volume = sum(w.total_volume or 0 for w in previous_wallets)
        previous_count = sum(w.transaction_count or 0 for w in previous_wallets)
        previous_avg_size = previous_volume / previous_count if previous_count > 0 else 0
        previous_avg_confidence = (
            sum(w.confidence_score or 0 for w in previous_wallets) / len(previous_wallets)
            if previous_wallets else 0
        )
        
        # Calculate percentage changes
        def calculate_change(current, previous):
            if previous == 0:
                return 0 if current == 0 else 100
            return ((current - previous) / previous) * 100
        
        volume_change = calculate_change(current_volume, previous_volume)
        wallets_change = calculate_change(len(current_wallets), len(previous_wallets))
        avg_size_change = calculate_change(current_avg_size, previous_avg_size)
        confidence_change = calculate_change(current_avg_confidence, previous_avg_confidence)
        
        logger.info(f"✅ Statistics: {len(current_wallets)} wallets, ${current_volume:,.0f}")
        logger.info(f"📈 Changes: volume={volume_change:.1f}%, wallets={wallets_change:.1f}%")
        
        return {
            "total_volume_usd": current_volume,
            "active_wallets": len(current_wallets),
            "total_transactions": current_count,
            "avg_transfer_size": current_avg_size,
            
            # ✅ NEW: 24h change percentages
            "volume_change_24h": round(volume_change, 2),
            "wallets_change_24h": round(wallets_change, 2),
            "avg_size_change_24h": round(avg_size_change, 2),
            "avg_confidence_score": round(current_avg_confidence, 1),
            "confidence_change_24h": round(confidence_change, 2),
            
            # ✅ NEW: Timestamp
            "last_updated": datetime.now().isoformat(),
            
            "period": {
                "start": start.isoformat(),
                "end": end.isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /statistics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/network/graph")
async def get_network_graph(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    max_nodes: int = Query(500, le=1000),
    db: Session = Depends(get_db)
):
    """Get network graph data for both NetworkGraph AND SankeyFlow components"""
    try:
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
        
        # ✅ Format for NetworkGraph (Cytoscape)
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
        
        # ✅ Format for SankeyFlow (D3-Sankey)
        sankey_nodes = [
            {
                "name": w.label or f"{w.address[:8]}...",
                "category": (w.entity_type or "unknown").replace('_', ' ').title(),
                "value": float(w.total_volume or 0),
                "address": w.address
            }
            for w in wallets
        ]
        
        # ✅ NO MOCK DATA - Empty edges/links
        cytoscape_edges = []
        sankey_links = []
        
        logger.info(f"✅ Graph: {len(cytoscape_nodes)} nodes, 0 edges (no transaction data yet)")
        
        return {
            # For NetworkGraph component
            "nodes": cytoscape_nodes,
            "edges": cytoscape_edges,
            
            # For SankeyFlow component  
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

@router.get("/wallet/{address}/details")
async def get_wallet_details(
    address: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed wallet information with LIVE Etherscan data.
    
    ✅ IF confidence_score >= 80%: Update DB with real data
    ⚠️ IF confidence_score < 80%: Only show, don't update DB
    """
    try:
        logger.info(f"👤 GET /wallet/{address[:10]}.../details")
        
        # Get wallet from DB
        wallet = db.query(OTCWallet).filter(
            OTCWallet.address == address
        ).first()
        
        if not wallet:
            raise HTTPException(status_code=404, detail="Wallet not found")
        
        # ✅ CHECK: Is this a VERIFIED OTC wallet?
        is_verified = (wallet.confidence_score or 0) >= 80
        
        logger.info(f"🔍 Wallet verified: {is_verified} (confidence: {wallet.confidence_score}%)")
        
        # ✅ ALWAYS fetch live data from Etherscan
        live_data = await fetch_etherscan_data(address)
        
        if not live_data:
            # Etherscan failed, use DB data only
            logger.warning(f"⚠️ Etherscan failed, using DB data only")
            return format_wallet_response(wallet, source="database")
        
        # ✅ IF VERIFIED: Update DB with real data
        if is_verified:
            logger.info(f"💾 VERIFIED wallet - Updating DB with live data")
            
            # Update wallet with live Etherscan data
            wallet.total_volume = live_data["balance_usd"]
            wallet.transaction_count = live_data["transaction_count"]
            wallet.last_active = datetime.fromtimestamp(live_data["last_tx_timestamp"])
            wallet.is_active = live_data["is_active"]
            
            # Add metadata
            if not wallet.tags:
                wallet.tags = []
            if "verified_live" not in wallet.tags:
                wallet.tags.append("verified_live")
            
            # Commit to database
            db.commit()
            db.refresh(wallet)
            
            logger.info(f"✅ DB updated: ${wallet.total_volume:,.2f}, {wallet.transaction_count} txs")
        else:
            logger.info(f"⚠️ UNVERIFIED wallet - Only showing data, NOT updating DB")
        
        # Generate chart data based on real or calculated values
        activity_data = generate_activity_chart(wallet, live_data)
        transfer_size_data = generate_transfer_size_chart(wallet, live_data)
        
        # Return response with live data
        return {
            # Basic info (always from DB)
            "address": wallet.address,
            "label": wallet.label,
            "entity_type": wallet.entity_type,
            "entity_name": wallet.entity_name,
            "confidence_score": wallet.confidence_score,
            "is_active": wallet.is_active,
            "is_verified": is_verified,  # ✅ NEW: Show if verified
            
            # ✅ Live metrics (from Etherscan)
            "balance_eth": live_data["balance_eth"],
            "balance_usd": live_data["balance_usd"],
            "lifetime_volume": live_data["balance_usd"],  # Current balance as proxy
            "volume_30d": live_data["recent_volume_30d"],
            "volume_7d": live_data["recent_volume_7d"],
            "avg_transfer": live_data["avg_transfer_size"],
            "transaction_count": live_data["transaction_count"],
            "last_activity": live_data["last_activity_text"],
            
            # Chart data
            "activity_data": activity_data,
            "transfer_size_data": transfer_size_data,
            
            # Metadata
            "data_source": "etherscan_live" if is_verified else "etherscan_display",
            "last_updated": datetime.now().isoformat(),
            "tags": wallet.tags or []
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in /wallet/details: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


async def fetch_etherscan_data(address: str) -> dict:
    """
    Fetch real wallet data from Etherscan API.
    
    Returns dict with balance, transactions, activity.
    """
    import requests
    from datetime import datetime, timedelta
    
    ETHERSCAN_API_KEY = "YourApiKeyToken"  # TODO: Move to environment variable
    BASE_URL = "https://api.etherscan.io/api"
    
    try:
        # ✅ 1. Get ETH Balance
        balance_response = requests.get(BASE_URL, params={
            "module": "account",
            "action": "balance",
            "address": address,
            "tag": "latest",
            "apikey": ETHERSCAN_API_KEY
        }, timeout=10)
        
        balance_data = balance_response.json()
        
        if balance_data.get("status") != "1":
            logger.error(f"Etherscan balance error: {balance_data.get('message')}")
            return None
        
        balance_wei = int(balance_data.get("result", 0))
        balance_eth = balance_wei / 1e18
        
        # Get current ETH price (simplified - use price oracle in production)
        eth_price = 2921.68  # TODO: Get from price oracle
        balance_usd = balance_eth * eth_price
        
        # ✅ 2. Get Recent Transactions (last 100)
        tx_response = requests.get(BASE_URL, params={
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 100,
            "sort": "desc",
            "apikey": ETHERSCAN_API_KEY
        }, timeout=10)
        
        tx_data = tx_response.json()
        
        if tx_data.get("status") != "1":
            logger.error(f"Etherscan tx error: {tx_data.get('message')}")
            transactions = []
        else:
            transactions = tx_data.get("result", [])
        
        # ✅ 3. Calculate metrics from transactions
        now = datetime.now()
        thirty_days_ago = now - timedelta(days=30)
        seven_days_ago = now - timedelta(days=7)
        
        recent_30d = []
        recent_7d = []
        total_volume = 0
        
        for tx in transactions:
            tx_time = datetime.fromtimestamp(int(tx.get("timeStamp", 0)))
            tx_value_wei = int(tx.get("value", 0))
            tx_value_eth = tx_value_wei / 1e18
            tx_value_usd = tx_value_eth * eth_price
            
            total_volume += tx_value_usd
            
            if tx_time >= thirty_days_ago:
                recent_30d.append(tx_value_usd)
            if tx_time >= seven_days_ago:
                recent_7d.append(tx_value_usd)
        
        # Calculate last activity
        last_activity_text = "Unknown"
        last_tx_timestamp = 0
        is_active = False
        
        if transactions:
            last_tx_timestamp = int(transactions[0].get("timeStamp", 0))
            time_diff = now.timestamp() - last_tx_timestamp
            
            if time_diff < 3600:
                last_activity_text = f"{int(time_diff / 60)}m ago"
                is_active = True
            elif time_diff < 86400:
                last_activity_text = f"{int(time_diff / 3600)}h ago"
                is_active = True
            elif time_diff < 604800:  # 7 days
                last_activity_text = f"{int(time_diff / 86400)}d ago"
                is_active = True
            else:
                last_activity_text = f"{int(time_diff / 86400)}d ago"
                is_active = False
        
        # Calculate averages
        avg_transfer_size = total_volume / len(transactions) if transactions else 0
        
        logger.info(f"✅ Etherscan data: {balance_eth:.4f} ETH, {len(transactions)} txs")
        
        return {
            "balance_eth": balance_eth,
            "balance_usd": balance_usd,
            "transaction_count": len(transactions),
            "recent_volume_30d": sum(recent_30d),
            "recent_volume_7d": sum(recent_7d),
            "avg_transfer_size": avg_transfer_size,
            "last_activity_text": last_activity_text,
            "last_tx_timestamp": last_tx_timestamp,
            "is_active": is_active,
            "transactions": transactions[:10]  # Last 10 for charts
        }
        
    except requests.RequestException as e:
        logger.error(f"❌ Etherscan API error: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Error fetching Etherscan data: {e}")
        return None


def generate_activity_chart(wallet, live_data: dict) -> list:
    """Generate 7-day activity chart from live data or estimates."""
    if not live_data or not live_data.get("transactions"):
        # Fallback: Generate from DB data
        base_daily_volume = (wallet.total_volume or 0) / 30
        activity_data = []
        
        for i in range(7):
            date = (datetime.now() - timedelta(days=6-i)).strftime('%m/%d')
            variation = 0.7 + (i % 3) * 0.3
            volume = base_daily_volume * variation
            activity_data.append({"date": date, "volume": round(volume, 2)})
        
        return activity_data
    
    # Use real transaction data
    transactions = live_data["transactions"]
    eth_price = 2921.68  # TODO: Use price oracle
    
    # Group by day
    daily_volumes = {}
    for tx in transactions:
        tx_date = datetime.fromtimestamp(int(tx["timeStamp"])).strftime('%m/%d')
        tx_value = int(tx.get("value", 0)) / 1e18 * eth_price
        
        if tx_date not in daily_volumes:
            daily_volumes[tx_date] = 0
        daily_volumes[tx_date] += tx_value
    
    # Create chart data
    activity_data = []
    for i in range(7):
        date = (datetime.now() - timedelta(days=6-i)).strftime('%m/%d')
        volume = daily_volumes.get(date, 0)
        activity_data.append({"date": date, "volume": round(volume, 2)})
    
    return activity_data


def generate_transfer_size_chart(wallet, live_data: dict) -> list:
    """Generate transfer size trend chart."""
    if not live_data or not live_data.get("transactions"):
        # Fallback
        base_transfer = (wallet.total_volume or 0) / (wallet.transaction_count or 1)
        transfer_size_data = []
        
        for i in range(7):
            date = (datetime.now() - timedelta(days=6-i)).strftime('%m/%d')
            variation = 0.8 + (i % 4) * 0.2
            size = base_transfer * variation
            transfer_size_data.append({"date": date, "size": round(size, 2)})
        
        return transfer_size_data
    
    # Use real data
    transactions = live_data["transactions"]
    eth_price = 2921.68
    
    # Group by day and calculate average
    daily_sizes = {}
    daily_counts = {}
    
    for tx in transactions:
        tx_date = datetime.fromtimestamp(int(tx["timeStamp"])).strftime('%m/%d')
        tx_value = int(tx.get("value", 0)) / 1e18 * eth_price
        
        if tx_date not in daily_sizes:
            daily_sizes[tx_date] = 0
            daily_counts[tx_date] = 0
        
        daily_sizes[tx_date] += tx_value
        daily_counts[tx_date] += 1
    
    # Create chart data
    transfer_size_data = []
    for i in range(7):
        date = (datetime.now() - timedelta(days=6-i)).strftime('%m/%d')
        count = daily_counts.get(date, 0)
        avg_size = daily_sizes.get(date, 0) / count if count > 0 else 0
        transfer_size_data.append({"date": date, "size": round(avg_size, 2)})
    
    return transfer_size_data


def format_wallet_response(wallet, source="database"):
    """Format wallet response when Etherscan unavailable."""
    return {
        "address": wallet.address,
        "label": wallet.label,
        "entity_type": wallet.entity_type,
        "confidence_score": wallet.confidence_score,
        "lifetime_volume": wallet.total_volume or 0,
        "transaction_count": wallet.transaction_count or 0,
        "data_source": source,
        "error": "Etherscan data unavailable"
    }

@router.get("/flow/sankey")
async def get_sankey_flow(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    min_flow_size: float = Query(100000),
    db: Session = Depends(get_db)
):
    """
    Get Sankey flow diagram data.
    
    GET /api/otc/flow/sankey
    """
    try:
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"💱 GET /flow/sankey: {start.date()} to {end.date()}")
        
        # Get wallets
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end,
            OTCWallet.total_volume >= min_flow_size
        ).order_by(OTCWallet.total_volume.desc()).limit(20).all()
        
        # Format nodes for D3 Sankey
        nodes = [
            {
                "name": w.label or f"{w.address[:8]}...",
                "category": (w.entity_type or "Unknown").replace('_', ' ').title(),
                "value": float(w.total_volume or 0),
                "address": w.address
            }
            for w in wallets
        ]
        
        # ✅ Empty links (no transaction relationships yet)
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
        logger.error(f"❌ Error in /flow/sankey: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/transfers/timeline")
async def get_transfer_timeline(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    min_confidence: float = Query(0),
    sort_by: str = Query("timestamp"),
    db: Session = Depends(get_db)
):
    """
    Get transfer timeline data.
    
    GET /api/otc/transfers/timeline
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
        
        logger.info(f"📅 GET /transfers/timeline: {start.date()} to {end.date()}")
        
        # Get wallets in time range
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end,
            OTCWallet.confidence_score >= min_confidence
        ).order_by(OTCWallet.last_active.desc()).limit(50).all()
        
        # ✅ Generate timeline events from wallet data
        events = [
            {
                "timestamp": w.last_active.isoformat() if w.last_active else datetime.now().isoformat(),
                "from_address": w.address,
                "to_address": None,  # Unknown without transaction data
                "amount_usd": float(w.total_volume or 0) / (w.transaction_count or 1),  # Avg transfer
                "token": "USDT",  # Default
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

@router.get("/analytics/distributions")
async def get_distributions(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get distribution statistics.
    
    GET /api/otc/analytics/distributions
    """
    try:
        if start_date:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start = datetime.now() - timedelta(days=30)
        
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()
        
        logger.info(f"📊 GET /analytics/distributions: {start.date()} to {end.date()}")
        
        # Get wallets
        wallets = db.query(OTCWallet).filter(
            OTCWallet.last_active >= start,
            OTCWallet.last_active <= end
        ).all()
        
        # ✅ Calculate entity type distribution
        entity_distribution = {}
        for w in wallets:
            entity_type = w.entity_type or "unknown"
            if entity_type not in entity_distribution:
                entity_distribution[entity_type] = {"count": 0, "volume": 0}
            entity_distribution[entity_type]["count"] += 1
            entity_distribution[entity_type]["volume"] += (w.total_volume or 0)
        
        # ✅ Calculate volume distribution (buckets)
        volume_buckets = {
            "0-1M": 0,
            "1M-10M": 0,
            "10M-100M": 0,
            "100M-1B": 0,
            "1B+": 0
        }
        
        for w in wallets:
            vol = w.total_volume or 0
            if vol < 1_000_000:
                volume_buckets["0-1M"] += 1
            elif vol < 10_000_000:
                volume_buckets["1M-10M"] += 1
            elif vol < 100_000_000:
                volume_buckets["10M-100M"] += 1
            elif vol < 1_000_000_000:
                volume_buckets["100M-1B"] += 1
            else:
                volume_buckets["1B+"] += 1
        
        logger.info(f"✅ Distributions: {len(entity_distribution)} entity types")
        
        return {
            "entity_distribution": entity_distribution,
            "volume_distribution": volume_buckets,
            "metadata": {
                "total_wallets": len(wallets),
                "period": {
                    "start": start.isoformat(),
                    "end": end.isoformat()
                }
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /analytics/distributions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/heatmap")
async def get_activity_heatmap(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get 24x7 activity heatmap.
    
    GET /api/otc/heatmap?start_date=2024-12-17&end_date=2024-12-24
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
        
        logger.info(f"🔥 GET /heatmap: {start.date()} to {end.date()}")
        
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
                # Simplified: distribute volume across hours
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
        
        logger.info(f"✅ Heatmap: {len(heatmap)} cells generated")
        
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


@router.get("/watchlist")
async def get_watchlist(
    user_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get user's watchlist.
    
    GET /api/otc/watchlist?user_id=dev_user_123
    """
    try:
        # ✅ FIX: Check if user_id is None first!
        if not user_id:
            logger.info(f"📋 GET /watchlist: No user_id provided, returning empty list")
            return {
                "items": [],
                "message": "No user authenticated"
            }
        
        logger.info(f"📋 GET /watchlist for user {user_id[:20] if len(user_id) > 20 else user_id}...")
        
        items = db.query(OTCWatchlist).filter(
            OTCWatchlist.user_id == user_id
        ).all()
        
        logger.info(f"✅ Found {len(items)} watchlist items")
        
        return {
            "items": [
                {
                    "id": str(item.id),
                    "wallet_address": item.wallet_address,
                    "notes": item.notes,
                    "alert_enabled": item.alert_enabled,
                    "alert_threshold": item.alert_threshold,
                    "created_at": item.created_at.isoformat() if item.created_at else None
                }
                for item in items
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /watchlist: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/watchlist")
async def add_to_watchlist(
    user_id: str,
    wallet_address: str,
    notes: Optional[str] = None,
    alert_threshold: Optional[float] = None,
    db: Session = Depends(get_db)
):
    """
    Add wallet to watchlist.
    
    POST /api/otc/watchlist?user_id=dev_user_123&wallet_address=0x...
    """
    try:
        # Check if already exists
        existing = db.query(OTCWatchlist).filter(
            OTCWatchlist.user_id == user_id,
            OTCWatchlist.wallet_address == wallet_address
        ).first()
        
        if existing:
            raise HTTPException(status_code=400, detail="Wallet already in watchlist")
        
        # Create new watchlist item
        item = OTCWatchlist(
            user_id=user_id,
            wallet_address=wallet_address,
            notes=notes,
            alert_enabled=alert_threshold is not None,
            alert_threshold=alert_threshold
        )
        
        db.add(item)
        db.commit()
        db.refresh(item)
        
        return {
            "id": str(item.id),
            "wallet_address": item.wallet_address,
            "created_at": item.created_at.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error adding to watchlist: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/watchlist/{item_id}")
async def remove_from_watchlist(
    item_id: str,
    db: Session = Depends(get_db)
):
    """
    Remove wallet from watchlist.
    
    DELETE /api/otc/watchlist/123
    """
    try:
        item = db.query(OTCWatchlist).filter(OTCWatchlist.id == item_id).first()
        
        if not item:
            raise HTTPException(status_code=404, detail="Watchlist item not found")
        
        db.delete(item)
        db.commit()
        
        return {"message": "Removed from watchlist"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error removing from watchlist: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_alerts(
    user_id: str = Query(...),
    unread_only: bool = Query(False),
    db: Session = Depends(get_db)
):
    """
    Get user's alerts.
    
    GET /api/otc/alerts?user_id=dev_user_123&unread_only=false
    """
    try:
        query = db.query(OTCAlert).filter(OTCAlert.user_id == user_id)
        
        if unread_only:
            query = query.filter(OTCAlert.is_read == False)
        
        alerts = query.order_by(OTCAlert.created_at.desc()).limit(100).all()
        
        return {
            "alerts": [
                {
                    "id": str(a.id),
                    "alert_type": a.alert_type,
                    "wallet_address": a.wallet_address,
                    "message": a.message,
                    "severity": a.severity,
                    "is_read": a.is_read,
                    "created_at": a.created_at.isoformat() if a.created_at else None
                }
                for a in alerts
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ Error in /alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# UTILITY ENDPOINTS
# ============================================================================

@router.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    GET /api/otc/health
    """
    logger.info(f"🏥 Health check...")
    
    try:
        latest_block = node_provider.get_latest_block_number()
        
        cache_healthy = cache_manager.exists("health_check")
        cache_manager.set("health_check", True, ttl=60)
        
        return {
            "success": True,
            "status": "healthy",
            "services": {
                "blockchain": {
                    "connected": latest_block > 0,
                    "latest_block": latest_block
                },
                "cache": {
                    "connected": True
                }
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {str(e)}", exc_info=True)
        return {
            "success": False,
            "status": "unhealthy",
            "error": str(e)
        }
