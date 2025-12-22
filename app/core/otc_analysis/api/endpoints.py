"""
Complete OTC Analysis API Endpoints
Combines Phase 1 and Phase 2 endpoints into a single file.

✅ FIXED: Mock Database implementation (no more 501 errors)
"""

from fastapi import APIRouter, HTTPException, Query, Depends, Header
from typing import List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel
import logging

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
from app.core.otc_analysis.models.wallet import Wallet
from app.core.otc_analysis.models.watchlist import WatchlistItem
from app.core.otc_analysis.models.alert import Alert

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
# DEPENDENCIES - ✅ FIXED WITH MOCK DB
# ============================================================================

def get_db():
    """
    ✅ FIXED: Mock Database for Development
    
    This mock implementation allows testing endpoints without database setup.
    Returns empty data for now.
    
    TODO: Replace with real database in production:
    
    from app.database import SessionLocal
    
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
    """
    
    class MockDB:
        """Mock Database Session"""
        
        def query(self, model):
            """Mock query method"""
            return MockQuery(model)
        
        def add(self, obj):
            """Mock add method - assigns fake ID"""
            if not hasattr(obj, 'id'):
                obj.id = 1
            pass
        
        def commit(self):
            """Mock commit method"""
            pass
        
        def rollback(self):
            """Mock rollback method"""
            pass
        
        def refresh(self, obj):
            """Mock refresh method"""
            pass
    
    class MockQuery:
        """Mock Query Builder"""
        
        def __init__(self, model):
            self.model = model
            self._filters = []
        
        def filter(self, *args):
            """Mock filter method"""
            self._filters.extend(args)
            return self
        
        def order_by(self, *args):
            """Mock order_by method"""
            return self
        
        def limit(self, n):
            """Mock limit method"""
            self._limit = n
            return self
        
        def all(self):
            """Mock all method - returns empty list"""
            # TODO: Return mock data for testing if needed
            return []
        
        def first(self):
            """Mock first method - returns None"""
            return None
    
    logger.info("⚠️  Using Mock Database (no real data)")
    return MockDB()


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
    from_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    to_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    db = Depends(get_db)
):
    """
    Get OTC statistics for OTCMetricsOverview component.
    
    GET /api/otc/statistics?from_date=2024-11-21&to_date=2024-12-21&entity_type=otc_desk
    
    Returns:
    {
        "total_volume_usd": 450000000,
        "active_wallets": 234,
        "avg_transfer_size": 1200000,
        "avg_confidence_score": 78.5,
        "volume_change_24h": 12.5,
        "wallets_change_24h": 5.2,
        "avg_size_change_24h": -3.1,
        "confidence_change_24h": 2.3,
        "last_updated": "2024-12-21T14:30:00Z"
    }
    """
    logger.info(f"📊 GET /statistics: {from_date} to {to_date}")
    
    try:
        from_dt = datetime.fromisoformat(from_date)
        to_dt = datetime.fromisoformat(to_date)
        
        if to_dt < from_dt:
            raise HTTPException(status_code=400, detail="to_date must be after from_date")
        
        if (to_dt - from_dt).days > 365:
            raise HTTPException(status_code=400, detail="Date range cannot exceed 1 year")
        
        stats = statistics_service.get_statistics(
            db=db,
            from_date=from_dt,
            to_date=to_dt,
            entity_type=entity_type
        )
        
        logger.info(f"✅ Statistics: {stats['active_wallets']} wallets, ${stats['total_volume_usd']:,.0f}")
        
        return stats
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        logger.error(f"❌ Failed to get statistics: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/network/graph")
async def get_network_graph(
    from_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    to_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    min_confidence: float = Query(0, ge=0, le=100, description="Minimum confidence score"),
    min_transfer_size: float = Query(0, ge=0, description="Minimum transfer size (USD)"),
    entity_types: Optional[str] = Query(None, description="Comma-separated entity types"),
    tokens: Optional[str] = Query(None, description="Comma-separated token symbols"),
    max_nodes: int = Query(500, ge=1, le=1000, description="Maximum nodes to return"),
    db = Depends(get_db)
):
    """
    Get network graph with ALL Phase 2 visualization data.
    
    GET /api/otc/network/graph?from_date=2024-11-21&to_date=2024-12-21&min_confidence=70&max_nodes=500
    
    Returns:
    {
        "nodes": [...],
        "edges": [...],
        "sankey_data": {...},
        "time_heatmap": {...},
        "timeline_data": {...},
        "distributions": {...},
        "metadata": {...}
    }
    """
    logger.info(f"🌐 GET /network/graph: {from_date} to {to_date}, max_nodes={max_nodes}")
    
    try:
        from_dt = datetime.fromisoformat(from_date)
        to_dt = datetime.fromisoformat(to_date)
        
        entity_type_list = None
        if entity_types:
            entity_type_list = [t.strip() for t in entity_types.split(',')]
        
        token_list = None
        if tokens:
            token_list = [t.strip().upper() for t in tokens.split(',')]
        
        graph_data = graph_builder.build_complete_graph(
            db=db,
            from_date=from_dt,
            to_date=to_dt,
            min_confidence=min_confidence,
            min_transfer_size=min_transfer_size,
            entity_types=entity_type_list,
            tokens=token_list,
            max_nodes=max_nodes
        )
        
        logger.info(f"✅ Graph: {len(graph_data['nodes'])} nodes, {len(graph_data['edges'])} edges")
        
        return graph_data
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(e)}")
    except Exception as e:
        logger.error(f"❌ Failed to build graph: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/watchlist")
async def get_watchlist(
    user_id: str = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get user's watchlist.
    
    GET /api/otc/watchlist
    Authorization: Bearer <token>
    
    Returns:
    {
        "watchlist": [
            {
                "id": 1,
                "address": "0x...",
                "label": "My Custom Label",
                "entity_type": "otc_desk",
                "entity_name": "Wintermute",
                "added_at": "2024-12-15T10:00:00Z"
            }
        ]
    }
    """
    logger.info(f"📋 GET /watchlist for user {user_id[:10]}...")
    
    try:
        watchlist_items = db.query(WatchlistItem).filter(
            WatchlistItem.user_id == user_id
        ).order_by(
            WatchlistItem.added_at.desc()
        ).all()
        
        logger.info(f"✅ Found {len(watchlist_items)} watchlist items")
        
        return {
            "watchlist": [item.to_dict() for item in watchlist_items]
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get watchlist: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/watchlist/add")
async def add_to_watchlist(
    request: WatchlistAddRequest,
    user_id: str = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Add address to watchlist.
    
    POST /api/otc/watchlist/add
    Authorization: Bearer <token>
    
    Body:
    {
        "address": "0x...",
        "label": "My Custom Label"
    }
    """
    logger.info(f"➕ POST /watchlist/add: {request.address[:10]}...")
    
    try:
        address = validate_ethereum_address(request.address)
        
        existing = db.query(WatchlistItem).filter(
            WatchlistItem.user_id == user_id,
            WatchlistItem.address == address
        ).first()
        
        if existing:
            raise HTTPException(status_code=400, detail="Address already in watchlist")
        
        wallet = db.query(Wallet).filter(Wallet.address == address).first()
        
        watchlist_item = WatchlistItem(
            user_id=user_id,
            address=address,
            label=request.label,
            entity_type=wallet.entity_type if wallet else None,
            entity_name=wallet.entity_name if wallet else None,
            added_at=datetime.utcnow()
        )
        
        db.add(watchlist_item)
        db.commit()
        db.refresh(watchlist_item)
        
        logger.info(f"✅ Added {address[:10]}... to watchlist")
        
        return {
            "success": True,
            "message": "Address added to watchlist",
            "watchlist_item": watchlist_item.to_dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Failed to add to watchlist: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/watchlist/{address}")
async def remove_from_watchlist(
    address: str,
    user_id: str = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Remove address from watchlist.
    
    DELETE /api/otc/watchlist/0x...
    Authorization: Bearer <token>
    """
    logger.info(f"➖ DELETE /watchlist/{address[:10]}...")
    
    try:
        address = validate_ethereum_address(address)
        
        item = db.query(WatchlistItem).filter(
            WatchlistItem.user_id == user_id,
            WatchlistItem.address == address
        ).first()
        
        if not item:
            raise HTTPException(status_code=404, detail="Address not in watchlist")
        
        db.delete(item)
        db.commit()
        
        logger.info(f"✅ Removed {address[:10]}... from watchlist")
        
        return {
            "success": True,
            "message": "Address removed from watchlist"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Failed to remove from watchlist: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_alerts(
    limit: int = Query(50, ge=1, le=100, description="Max alerts to return"),
    user_id: str = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get alerts for user.
    
    GET /api/otc/alerts?limit=50
    Authorization: Bearer <token>
    """
    logger.info(f"🔔 GET /alerts (limit={limit}) for user {user_id[:10]}...")
    
    try:
        alerts = db.query(Alert).filter(
            Alert.is_dismissed == False
        ).order_by(
            Alert.created_at.desc()
        ).limit(limit).all()
        
        logger.info(f"✅ Found {len(alerts)} alerts")
        
        return {
            "alerts": [alert.to_dict() for alert in alerts]
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get alerts: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/dismiss")
async def dismiss_alert(
    alert_id: int,
    user_id: str = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Dismiss an alert.
    
    POST /api/otc/alerts/123/dismiss
    Authorization: Bearer <token>
    """
    logger.info(f"✖️  POST /alerts/{alert_id}/dismiss")
    
    try:
        alert = db.query(Alert).filter(Alert.id == alert_id).first()
        
        if not alert:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        alert.is_dismissed = True
        alert.dismissed_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"✅ Alert {alert_id} dismissed")
        
        return {
            "success": True,
            "message": "Alert dismissed"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Failed to dismiss alert: {str(e)}", exc_info=True)
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
