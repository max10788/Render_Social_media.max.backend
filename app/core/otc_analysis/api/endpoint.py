from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from datetime import datetime
import logging
from pydantic import BaseModel

from app.core.otc_analysis.api.validators import (
    ScanRangeRequest,
    WalletProfileRequest,
    FlowTraceRequest,
    validate_ethereum_address,
    validate_block_range
)
from app.core.otc_analysis.detection.otc_detector import OTCDetector
from app.core.otc_analysis.detection.wallet_profiler import WalletProfiler
from app.core.otc_analysis.detection.flow_tracer import FlowTracer
from app.core.otc_analysis.blockchain.node_provider import NodeProvider
from app.core.otc_analysis.blockchain.block_scanner import BlockScanner
from app.core.otc_analysis.blockchain.transaction_extractor import TransactionExtractor
from app.core.otc_analysis.blockchain.etherscan import EtherscanAPI
from app.core.otc_analysis.data_sources.price_oracle import PriceOracle
from app.core.otc_analysis.data_sources.otc_desks import OTCDeskRegistry
from app.core.otc_analysis.data_sources.wallet_labels import WalletLabelingService
from app.core.otc_analysis.utils.cache import CacheManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/otc", tags=["OTC Analysis"])

# Initialize services (in production, use dependency injection)
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


@router.post("/scan/range")
async def scan_block_range(request: ScanRangeRequest):
    """
    Scan a historical block range for OTC activity.
    
    POST /api/otc/scan/range
    
    Body:
    {
        "from_block": 12000000,
        "to_block": 12001000,
        "tokens": ["0x..."],  // optional
        "min_usd_value": 100000,
        "exclude_exchanges": true
    }
    """
    logger.info(f"🔍 Starting OTC scan: blocks {request.from_block} to {request.to_block}")
    
    try:
        # Validate block range
        validate_block_range(request.from_block, request.to_block)
        
        # Scan blocks
        logger.info(f"📦 Scanning blocks...")
        transactions = block_scanner.scan_range(
            from_block=request.from_block,
            to_block=request.to_block
        )
        logger.info(f"✅ Found {len(transactions)} transactions")
        
        # Enrich with USD values
        logger.info(f"💰 Enriching with USD values...")
        transactions = transaction_extractor.enrich_with_usd_value(
            transactions,
            price_oracle
        )
        
        # Filter by value
        if request.min_usd_value:
            transactions = transaction_extractor.filter_by_value(
                transactions,
                min_usd_value=request.min_usd_value
            )
            logger.info(f"💵 Filtered to {len(transactions)} high-value transactions")
        
        # Exclude exchanges if requested
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
        
        # Detect OTC activity
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
        # Validate address
        address = validate_ethereum_address(address)
        
        # Check cache first
        cached_profile = cache_manager.get_wallet_profile(address)
        if cached_profile:
            logger.info(f"✅ Profile loaded from cache")
            return {"success": True, "data": cached_profile, "cached": True}
        
        # Fetch transactions
        logger.info(f"📡 Fetching transactions from Etherscan...")
        transactions = transaction_extractor.extract_wallet_transactions(
            address,
            include_internal=True,
            include_tokens=True
        )
        logger.info(f"✅ Found {len(transactions)} transactions")
        
        # Enrich with USD values
        logger.info(f"💰 Enriching with prices...")
        transactions = transaction_extractor.enrich_with_usd_value(
            transactions,
            price_oracle
        )
        
        # Get labels
        labels = None
        if include_labels:
            logger.info(f"🏷️  Fetching wallet labels...")
            labels = labeling_service.get_wallet_labels(address)
        
        # Create profile
        logger.info(f"📊 Building wallet profile...")
        profile = wallet_profiler.create_profile(address, transactions, labels)
        
        # Calculate OTC probability
        otc_probability = wallet_profiler.calculate_otc_probability(profile)
        profile['otc_probability'] = otc_probability
        
        # Network metrics if requested
        if include_network_metrics and len(transactions) > 0:
            logger.info(f"🕸️  Calculating network metrics...")
            from app.core.otc_analysis.analysis.network_graph import NetworkAnalysisService
            
            network_analyzer = NetworkAnalysisService()
            network_analyzer.build_graph(transactions)
            network_metrics = network_analyzer.analyze_wallet_centrality(address)
            
            profile['network_metrics'] = network_metrics
        
        # Cache the profile
        cache_manager.cache_wallet_profile(address, profile)
        
        logger.info(f"✅ Profile complete - OTC probability: {otc_probability:.2%}")
        
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
        # Validate addresses
        source = validate_ethereum_address(request.source_address)
        target = validate_ethereum_address(request.target_address)
        
        # Fetch transactions for both addresses
        logger.info(f"📡 Fetching transaction data...")
        
        source_txs = transaction_extractor.extract_wallet_transactions(source)
        target_txs = transaction_extractor.extract_wallet_transactions(target)
        
        # Combine and deduplicate
        all_transactions = {tx['tx_hash']: tx for tx in source_txs + target_txs}
        transactions = list(all_transactions.values())
        
        logger.info(f"✅ Loaded {len(transactions)} transactions")
        
        # Enrich with USD values
        transactions = transaction_extractor.enrich_with_usd_value(
            transactions,
            price_oracle
        )
        
        # Trace flow
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
        # Fetch transaction details
        logger.info(f"📡 Fetching transaction from blockchain...")
        tx_data = node_provider.get_transaction(tx_hash)
        
        if not tx_data:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        # Get receipt for additional info
        receipt = node_provider.get_transaction_receipt(tx_hash)
        
        # Format transaction
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
        
        # Build transaction dict
        transaction = {
            'tx_hash': tx_hash,
            'from_address': from_address,
            'to_address': to_address,
            'value': str(tx_data['value']),
            'value_decimal': node_provider.from_wei(tx_data['value']),
            'block_number': tx_data['blockNumber'],
            'timestamp': datetime.now(),  # Would need block timestamp
            'gas_used': receipt.get('gasUsed'),
            'is_contract_interaction': node_provider.is_contract(to_address)
        }
        
        # Get price and calculate USD value
        logger.info(f"💰 Fetching ETH price...")
        eth_price = price_oracle.get_current_price(None)  # None = ETH
        if eth_price:
            transaction['usd_value'] = transaction['value_decimal'] * eth_price
            logger.info(f"💵 Transaction value: ${transaction['usd_value']:,.2f}")
        
        # Get wallet profile
        logger.info(f"👤 Building wallet profile...")
        wallet_txs = transaction_extractor.extract_wallet_transactions(from_address)
        wallet_profile = wallet_profiler.create_profile(from_address, wallet_txs)
        
        # Detect OTC
        logger.info(f"🎯 Running OTC detection...")
        result = otc_detector.detect_otc_transaction(
            transaction,
            wallet_profile,
            wallet_txs[:100]  # Last 100 transactions
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
async def get_statistics():
    """
    Get overall OTC detection statistics.
    
    GET /api/otc/stats
    """
    logger.info(f"📊 Fetching OTC statistics...")
    
    try:
        stats = otc_detector.get_detection_stats()
        
        # Add cache stats
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


@router.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    GET /api/otc/health
    """
    logger.info(f"🏥 Health check...")
    
    try:
        # Check blockchain connection
        latest_block = node_provider.get_latest_block_number()
        
        # Check cache connection
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
