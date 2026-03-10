"""
FastAPI Endpoints für Orderbook Heatmap
FIXED VERSION - DEX Pools Route korrigiert
- Route geändert von /dex/pools/{dex}/{network}/{token0}/{token1}
  zu /dex/pools/{network}/{token0}/{token1}?dex=uniswap_v3
"""
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List, Any
from pydantic import BaseModel, Field, ValidationError
import asyncio
import json
import logging
from datetime import datetime
import aiohttp
import os

from app.core.orderbook_heatmap.models.orderbook import Exchange, AggregatedOrderbook
from app.core.orderbook_heatmap.models.heatmap import HeatmapConfig, HeatmapSnapshot, HeatmapTimeSeries

# CEX Imports
from app.core.orderbook_heatmap.exchanges.bitget_l2 import BitgetL2DataFetcher, L2_TOKEN_MAP
from app.core.orderbook_heatmap.exchanges.binance import BinanceExchange
from app.core.orderbook_heatmap.exchanges.bitget import BitgetExchange
from app.core.orderbook_heatmap.exchanges.kraken import KrakenExchange
from app.core.orderbook_heatmap.exchanges.bybit import BybitExchange
from app.core.orderbook_heatmap.exchanges.okx import OKXExchange
from app.core.orderbook_heatmap.exchanges.coinbase import CoinbaseExchange
from app.core.orderbook_heatmap.exchanges.deribit import DeribitExchange

# DEX Imports
from app.core.orderbook_heatmap.exchanges.dex.uniswap_v3 import UniswapV3Exchange
from app.core.orderbook_heatmap.exchanges.dex.curve_v2 import CurveV2Exchange
from app.core.orderbook_heatmap.exchanges.dex.pancakeswap import PancakeSwapExchange

from app.core.orderbook_heatmap.aggregator.orderbook_aggregator import OrderbookAggregator
from app.core.orderbook_heatmap.websocket.manager import WebSocketManager

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/orderbook-heatmap", tags=["Orderbook Heatmap"])

aggregator: Optional[any] = None
ws_manager: Optional[any] = None

class OHLCVBar(BaseModel):
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class HMMRegimeRequest(BaseModel):
    data: List[OHLCVBar] = Field(..., description="OHLCV-Bars (mind. train_bars+10 Einträge)")
    n_states: int = Field(default=3, ge=2, le=8, description="Anzahl Hidden States")
    train_bars: int = Field(default=120, ge=20, le=500, description="Mindest-Bars für Initial-Training")
    use_volume: bool = Field(default=True, description="Volume als Feature einbeziehen")
    retrain_every: int = Field(default=0, ge=0, description="Retraining alle N Bars (0=deaktiviert)")
    covariance_type: str = Field(default="diag", description="HMM Kovarianz-Typ: full|diag|tied|spherical")
    n_iter: int = Field(default=200, ge=50, le=1000, description="EM-Iterationen")
    max_signal_bars: int = Field(default=1000, ge=10, le=10000, description="Max. zurückgegebene Signal-Bars")


class StartHeatmapRequest(BaseModel):
    symbol: str = Field(..., description="Trading Pair (z.B. BTC/USDT)")
    exchanges: List[str] = Field(
        default=["binance", "bitget", "kraken"],
        description="Liste von Börsen"
    )
    dex_pools: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional dict mit DEX Pool Adressen"
    )
    price_bucket_size: float = Field(
        default=10.0,
        ge=0.1,
        description="Preis-Bucket-Größe"
    )
    time_window_seconds: int = Field(
        default=60,
        ge=1,
        description="Zeitfenster in Sekunden"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "symbol": "BTC/USDT",
                "exchanges": ["binance", "bybit", "okx", "uniswap_v3"],
                "dex_pools": {
                    "uniswap_v3": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
                },
                "price_bucket_size": 10.0,
                "time_window_seconds": 60
            }
        }

@router.post("/start")
async def start_heatmap(request: Request, data: StartHeatmapRequest):
    global aggregator, ws_manager
    
    logger.info("=" * 80)
    logger.info("🚀 START HEATMAP REQUEST RECEIVED")
    logger.info("=" * 80)
    
    try:
        body = await request.body()
        logger.info(f"📥 Raw request body: {body.decode('utf-8')}")
        
        logger.info(f"📊 Parsed request data:")
        logger.info(f"  ✓ symbol: {data.symbol}")
        logger.info(f"  ✓ exchanges: {data.exchanges}")
        logger.info(f"  ✓ dex_pools: {data.dex_pools}")
        logger.info(f"  ✓ price_bucket_size: {data.price_bucket_size}")
        logger.info(f"  ✓ time_window_seconds: {data.time_window_seconds}")
        
        logger.info("🔍 Validating symbol format...")
        if "/" not in data.symbol:
            error_msg = f"Invalid symbol format: '{data.symbol}'. Expected format: BASE/QUOTE (e.g., BTC/USDT)"
            logger.error(f"❌ {error_msg}")
            raise HTTPException(status_code=422, detail=error_msg)
        logger.info(f"  ✅ Symbol format valid: {data.symbol}")
        
        logger.info("🔍 Validating exchanges...")
        valid_exchanges = [
            # CEX
            "binance", "bitget", "kraken", "bybit", "okx", "coinbase", "deribit",
            # DEX
            "uniswap_v3", "curve_v2", "pancakeswap", "raydium"
        ]
        logger.info(f"  Valid options: {valid_exchanges}")
        
        invalid_exchanges = [e for e in data.exchanges if e not in valid_exchanges]
        if invalid_exchanges:
            error_msg = f"Invalid exchanges: {invalid_exchanges}. Valid options: {valid_exchanges}"
            logger.error(f"❌ {error_msg}")
            raise HTTPException(status_code=422, detail=error_msg)
        logger.info(f"  ✅ All exchanges valid: {data.exchanges}")
        
        logger.info("⚙️ Creating HeatmapConfig...")
        config = HeatmapConfig(
            price_bucket_size=data.price_bucket_size,
            time_window_seconds=data.time_window_seconds,
            exchanges=data.exchanges
        )
        logger.info(f"  ✅ Config created: {config}")
        
        try:
            logger.info("🔧 Attempting to initialize OrderbookAggregator...")
            
            aggregator = OrderbookAggregator(config)
            logger.info("  ✅ OrderbookAggregator initialized")
            
            logger.info("📡 Initializing exchanges...")
            exchange_map = {}
            
            for exchange_name in data.exchanges:
                try:
                    # CEX Exchanges
                    if exchange_name == "binance":
                        exchange_map[exchange_name] = BinanceExchange()
                        logger.info(f"  ✅ BinanceExchange loaded")
                    elif exchange_name == "bitget":
                        exchange_map[exchange_name] = BitgetExchange()
                        logger.info(f"  ✅ BitgetExchange loaded")
                    elif exchange_name == "kraken":
                        exchange_map[exchange_name] = KrakenExchange()
                        logger.info(f"  ✅ KrakenExchange loaded")
                    elif exchange_name == "bybit":
                        exchange_map[exchange_name] = BybitExchange()
                        logger.info(f"  ✅ BybitExchange loaded")
                    elif exchange_name == "okx":
                        exchange_map[exchange_name] = OKXExchange()
                        logger.info(f"  ✅ OKXExchange loaded")
                    elif exchange_name == "coinbase":
                        exchange_map[exchange_name] = CoinbaseExchange()
                        logger.info(f"  ⚠️ CoinbaseExchange loaded (note: uses USD not USDT)")
                    elif exchange_name == "deribit":
                        exchange_map[exchange_name] = DeribitExchange()
                        logger.info(f"  ✅ DeribitExchange loaded")
                    
                    # DEX Exchanges
                    elif exchange_name == "uniswap_v3":
                        exchange_map[exchange_name] = UniswapV3Exchange()
                        logger.info(f"  ✅ UniswapV3Exchange loaded")
                    elif exchange_name == "curve_v2":
                        exchange_map[exchange_name] = CurveV2Exchange()
                        logger.info(f"  ✅ CurveV2Exchange loaded")
                    elif exchange_name == "pancakeswap":
                        exchange_map[exchange_name] = PancakeSwapExchange()
                        logger.info(f"  ✅ PancakeSwapExchange loaded")
                    
                except ImportError as ie:
                    logger.warning(f"  ⚠️ Could not import {exchange_name}: {ie}")
            
            for exchange_name, exchange_instance in exchange_map.items():
                aggregator.add_exchange(exchange_instance)
                logger.info(f"  ✅ Added {exchange_name} to aggregator")
            
            logger.info("🔌 Connecting to exchanges...")
            await aggregator.connect_all(data.symbol, data.dex_pools)
            logger.info("  ✅ Connected to all exchanges")
            
            if ws_manager is None:
                ws_manager = WebSocketManager()
                logger.info("  ✅ WebSocketManager initialized")
            
            async def async_broadcast_callback():
                try:
                    await ws_manager.broadcast_update(aggregator)
                except Exception as e:
                    logger.error(f"Broadcast callback error: {e}")
            
            aggregator.add_update_callback(async_broadcast_callback)
            logger.info("  ✅ WebSocket callback set (async)")
            
        except ImportError as e:
            logger.warning(f"⚠️ Could not initialize full aggregator (missing modules): {e}")
            logger.info("  ℹ️ Running in mock mode")
        
        response = {
            "status": "started",
            "symbol": data.symbol,
            "exchanges": data.exchanges,
            "config": {
                "price_bucket_size": data.price_bucket_size,
                "time_window_seconds": data.time_window_seconds
            },
            "timestamp": datetime.utcnow().isoformat(),
            "mode": "live" if aggregator else "mock"
        }
        
        logger.info("=" * 80)
        logger.info("✅ START HEATMAP - SUCCESS")
        logger.info(f"📤 Response: {response}")
        logger.info("=" * 80)
        
        return response
        
    except HTTPException:
        raise
    except ValidationError as e:
        logger.error("=" * 80)
        logger.error("❌ VALIDATION ERROR")
        logger.error("=" * 80)
        logger.error(f"Validation errors: {e.errors()}")
        logger.error(f"Validation JSON: {e.json()}")
        
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Validation Error",
                "details": e.errors()
            }
        )
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ UNEXPECTED ERROR")
        logger.error("=" * 80)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error("Traceback:", exc_info=True)
        
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.post("/stop")
async def stop_heatmap():
    global aggregator
    
    logger.info("🛑 STOP HEATMAP REQUEST")
    
    if not aggregator:
        logger.warning("  ⚠️ No aggregator running")
        raise HTTPException(status_code=400, detail="Heatmap not running")
    
    try:
        logger.info("  🔌 Disconnecting from exchanges...")
        await aggregator.disconnect_all()
        aggregator = None
        logger.info("  ✅ Disconnected successfully")
        
        response = {
            "status": "stopped",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        logger.info(f"✅ STOP SUCCESS: {response}")
        return response
        
    except Exception as e:
        logger.error(f"❌ Failed to stop heatmap: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status")
async def get_status():
    global aggregator
    
    logger.info("📊 STATUS REQUEST")
    
    if not aggregator:
        response = {
            "status": "not_running",
            "timestamp": datetime.utcnow().isoformat()
        }
        logger.info(f"  ℹ️ {response}")
        return response
    
    try:
        status = aggregator.get_status()
        response = {
            "status": "running",
            **status,
            "timestamp": datetime.utcnow().isoformat()
        }
        logger.info(f"  ✅ {response}")
        return response
    except Exception as e:
        logger.error(f"❌ Error getting status: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@router.get("/snapshot/{symbol}")
async def get_snapshot(symbol: str):
    global aggregator
    
    normalized_symbol = symbol.replace(".", "/")
    logger.info(f"📸 SNAPSHOT REQUEST for {symbol} (normalized: {normalized_symbol})")
    
    if not aggregator:
        logger.warning("  ⚠️ No aggregator running")
        raise HTTPException(status_code=400, detail="Heatmap not running. Start it first with POST /start")
    
    try:
        snapshot = await aggregator.get_latest_heatmap(normalized_symbol)
        
        # FIX 3: Wait up to 10 seconds for first snapshot generation
        if not snapshot:
            logger.info(f"  ⏳ Waiting for first snapshot generation...")
            for i in range(10):
                await asyncio.sleep(1)
                snapshot = await aggregator.get_latest_heatmap(normalized_symbol)
                if snapshot:
                    logger.info(f"  ✅ Snapshot generated after {i+1} second(s)")
                    break
            
            if not snapshot:
                logger.warning(f"  ⚠️ No snapshot available for {normalized_symbol} after 10s wait")
                raise HTTPException(
                    status_code=503,
                    detail=f"Snapshot generation in progress. Please try again in a few seconds."
                )
        
        exchanges = list(aggregator.exchanges.keys())
        matrix_data = snapshot.to_matrix(exchanges)
        
        logger.info(f"  ✅ Snapshot retrieved for {normalized_symbol}")
        return matrix_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting snapshot: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/timeseries/{symbol}")
async def get_timeseries(symbol: str):
    global aggregator
    
    normalized_symbol = symbol.replace(".", "/")
    logger.info(f"📈 TIMESERIES REQUEST for {normalized_symbol}")
    
    if not aggregator:
        logger.warning("  ⚠️ No aggregator running")
        raise HTTPException(status_code=400, detail="Heatmap not running")
    
    try:
        timeseries = await aggregator.get_heatmap_timeseries(normalized_symbol)
        
        if not timeseries:
            logger.warning(f"  ⚠️ No timeseries available for {normalized_symbol}")
            raise HTTPException(status_code=404, detail="No timeseries available")
        
        exchanges = list(aggregator.exchanges.keys())
        matrix_data = timeseries.to_3d_matrix(exchanges)
        
        logger.info(f"  ✅ Timeseries retrieved for {normalized_symbol}")
        return matrix_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting timeseries: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/orderbook/{symbol}")
async def get_orderbook(symbol: str):
    global aggregator
    
    normalized_symbol = symbol.replace(".", "/")
    logger.info(f"📖 ORDERBOOK REQUEST for {normalized_symbol}")
    
    if not aggregator:
        logger.warning("  ⚠️ No aggregator running")
        raise HTTPException(status_code=400, detail="Heatmap not running")
    
    try:
        orderbook = await aggregator.get_aggregated_orderbook(normalized_symbol)
        
        logger.info(f"  ✅ Orderbook retrieved for {normalized_symbol}")
        
        # FIX 1: Use .dict() for Pydantic v1 (not .model_dump() which is v2)
        return orderbook.dict()
        
    except Exception as e:
        logger.error(f"❌ Error getting orderbook: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    global ws_manager, aggregator
    
    if ws_manager is None:
        try:
            from app.core.orderbook_heatmap.websocket.manager import WebSocketManager
            ws_manager = WebSocketManager()
        except ImportError:
            logger.error("❌ WebSocketManager not available")
            await websocket.close(code=1011, reason="WebSocket not available")
            return
    
    normalized_symbol = symbol.replace(".", "/")
    logger.info(f"🔌 WebSocket connection request for {normalized_symbol}")
    
    await ws_manager.connect(websocket, symbol)
    logger.info(f"  ✅ WebSocket connected for {normalized_symbol}")
    
    # Background task to send updates
    async def send_updates():
        """Send heatmap updates every second"""
        while True:
            try:
                if aggregator:
                    snapshot = await aggregator.get_latest_heatmap(normalized_symbol)
                    if snapshot:
                        exchanges = list(aggregator.exchanges.keys())
                        matrix_data = snapshot.to_matrix(exchanges)
                        
                        # CRITICAL: Use CURRENT time, not cached snapshot time
                        matrix_data['timestamp'] = datetime.utcnow().isoformat()
                        
                        message = {
                            "type": "heatmap_update",
                            "symbol": normalized_symbol,
                            "data": matrix_data,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        
                        await websocket.send_json(message)
                        logger.debug(f"📊 Sent heatmap update: {len(matrix_data.get('prices', []))} price levels")
                    else:
                        logger.debug(f"⚠️ No snapshot available yet for {normalized_symbol}")
                else:
                    logger.debug(f"⚠️ No aggregator available")
                
                await asyncio.sleep(1)  # Update every second
                
            except asyncio.CancelledError:
                logger.info("Update task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in heatmap update loop: {e}", exc_info=True)
                break
    
    # Start update task
    update_task = asyncio.create_task(send_updates())
    
    try:
        while True:
            data = await websocket.receive_text()
            logger.debug(f"  📥 WebSocket message: {data}")
            
    except WebSocketDisconnect:
        update_task.cancel()
        ws_manager.disconnect(websocket)
        logger.info(f"  🔌 WebSocket disconnected for {normalized_symbol}")
    except Exception as e:
        update_task.cancel()
        logger.error(f"❌ WebSocket error: {e}", exc_info=True)
        try:
            await websocket.close(code=1011, reason=str(e))
        except:
            pass

@router.get("/exchanges")
async def get_available_exchanges():
    """Liste aller verfügbaren Börsen"""
    logger.info("📋 EXCHANGES LIST REQUEST")
    
    exchanges_list = {
        "exchanges": [
            # CEX
            {
                "name": "binance",
                "type": "cex",
                "description": "Binance Spot & Futures",
                "websocket": True,
                "max_levels": 1000
            },
            {
                "name": "bitget",
                "type": "cex",
                "description": "Bitget Spot & Futures",
                "websocket": True,
                "max_levels": 100
            },
            {
                "name": "kraken",
                "type": "cex",
                "description": "Kraken Spot",
                "websocket": True,
                "max_levels": 500
            },
            {
                "name": "bybit",
                "type": "cex",
                "description": "Bybit Spot & Derivatives",
                "websocket": True,
                "max_levels": 200
            },
            {
                "name": "okx",
                "type": "cex",
                "description": "OKX Spot & Derivatives",
                "websocket": True,
                "max_levels": 400
            },
            {
                "name": "coinbase",
                "type": "cex",
                "description": "Coinbase Exchange (Note: Uses USD not USDT)",
                "websocket": True,
                "max_levels": 50,
                "note": "Automatically converts USDT to USD pairs"
            },
            {
                "name": "deribit",
                "type": "cex",
                "description": "Deribit Spot & Derivatives",
                "websocket": True,
                "max_levels": 10000,
                "note": "Primarily derivatives exchange"
            },
            
            # DEX
            {
                "name": "uniswap_v3",
                "type": "dex",
                "description": "Uniswap v3 (Ethereum, Polygon, Arbitrum, Optimism, Base)",
                "requires_pool_address": True,
                "websocket": False,
                "polling": True
            },
            {
                "name": "curve_v2",
                "type": "dex",
                "description": "Curve v2 (Ethereum, Polygon, Arbitrum, Optimism)",
                "requires_pool_address": True,
                "websocket": False,
                "polling": True,
                "note": "Bonding curve based, continuous liquidity"
            },
            {
                "name": "pancakeswap",
                "type": "dex",
                "description": "PancakeSwap v3 (BSC, Ethereum, Arbitrum)",
                "requires_pool_address": True,
                "websocket": False,
                "polling": True
            }
        ],
        "notes": {
            "dex_requirements": "DEX integrations require THE_GRAPH_API_KEY environment variable",
            "coinbase": "Coinbase automatically converts BTC/USDT to BTC/USD",
            "deribit": "Deribit uses specific symbol formats (e.g., BTC_USDC for spot, BTC-PERPETUAL for perpetuals)"
        }
    }
    
    logger.info(f"  ✅ Returned {len(exchanges_list['exchanges'])} exchanges")
    return exchanges_list

@router.get("/health")
async def health_check():
    logger.debug("💚 HEALTH CHECK")
    
    # Check if THE_GRAPH_API_KEY is set
    graph_api_key = os.getenv("THE_GRAPH_API_KEY", "")
    
    response = {
        "status": "healthy",
        "service": "orderbook-heatmap",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "features": {
            "cex_count": 7,  # binance, bitget, kraken, bybit, okx, coinbase, deribit
            "dex_count": 3,  # uniswap_v3, curve_v2, pancakeswap
            "graph_api_configured": bool(graph_api_key)
        }
    }
    
    if not graph_api_key:
        response["warnings"] = [
            "THE_GRAPH_API_KEY not set - DEX integrations will use slower fallback"
        ]
    
    logger.debug(f"  ✅ {response}")
    return response

# ============================================================================
# DEX POOLS ENDPOINT
# ============================================================================

# FIX 4: Updated Subgraph IDs (aktuell aus The Graph, Dezember 2024)
UNISWAP_V3_SUBGRAPH_IDS = {
    "ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    "polygon": "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
    "arbitrum": "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    "optimism": "Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj",
    "base": "43Hwfi3dJSoGpyas9VwNoDAv55yjgGrPpNSmbQZArzMG"
}

CURVE_V2_SUBGRAPH_IDS = {
    # NOTE: Curve v2 doesn't have a dedicated Uniswap v3-style subgraph with 'pools' query
    # Curve uses a different architecture (meta pools, factory pools, etc.)
    # For now, Curve v2 is not supported - would require custom implementation
    # Alternative: Use Messari standardized subgraph but different schema
}

# FIX 4: Korrekte PancakeSwap Subgraph IDs (offizielle v3 Exchange IDs, Dezember 2024)
PANCAKESWAP_SUBGRAPH_IDS = {
    "bsc": "Hv1GncLY5docZoGtXjo4kwbTvxm3MAhVZqBZE4sUT9eZ",  # ✅ BSC V3 Exchange (official)
    "ethereum": "CJYGNhb7RvnhfBDjqpRnD3oxgyhibzc7fkAMa38YV3oS",  # ✅ ETH V3 Exchange (official)
    "arbitrum": "251MHFNN1rwjErXD2efWMpNS73SANZN8Ua192zw6iXve"   # ✅ ARB V3 Exchange (official)
}

# Token Addresses
TOKEN_ADDRESSES = {
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "UNI": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
        "LINK": "0x514910771AF9Ca656af840dff83E8264EcF986CA",
        "MATIC": "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0",
        "AAVE": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
        "CRV": "0xD533a949740bb3306d119CC777fa900bA034cd52",
    },
    "polygon": {
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "USDC": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
    },
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
    },
    "optimism": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
        "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "OP": "0x4200000000000000000000000000000000000042",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    },
    "bsc": {
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "BUSD": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "CAKE": "0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82",
    }
}

def resolve_token_address(network: str, symbol: str) -> Optional[str]:
    """Löst Token-Symbol zu Contract-Adresse auf"""
    network_tokens = TOKEN_ADDRESSES.get(network.lower(), {})
    return network_tokens.get(symbol.upper())

def get_subgraph_url(network: str, dex: str = "uniswap_v3") -> Optional[str]:
    """Holt Subgraph URL für spezifischen DEX"""
    network = network.lower()
    api_key = os.getenv("THE_GRAPH_API_KEY", "")
    
    if not api_key:
        logger.warning("⚠️ THE_GRAPH_API_KEY not set! Using fallback Subgraph (slower)")
        logger.warning("💡 Get free API key at: https://thegraph.com/studio/")
        
        # Fallback URLs
        messari_urls = {
            "uniswap_v3": {
                "ethereum": "https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-ethereum",
                "polygon": "https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-polygon",
                "arbitrum": "https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-arbitrum",
            }
        }
        return messari_urls.get(dex, {}).get(network)
    
    # Select subgraph ID based on DEX
    if dex == "uniswap_v3":
        subgraph_ids = UNISWAP_V3_SUBGRAPH_IDS
    elif dex == "curve_v2":
        subgraph_ids = CURVE_V2_SUBGRAPH_IDS
    elif dex == "pancakeswap":
        subgraph_ids = PANCAKESWAP_SUBGRAPH_IDS
    else:
        logger.error(f"Unknown DEX: {dex}")
        return None
    
    subgraph_id = subgraph_ids.get(network)
    if subgraph_id:
        return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
    
    logger.error(f"No subgraph ID for {dex} on {network}")
    return None

def sqrt_price_x96_to_price(sqrt_price_x96: str, decimals0: int, decimals1: int) -> float:
    """Konvertiert sqrtPriceX96 zu human-readable Preis"""
    try:
        sqrt_price = int(sqrt_price_x96) / (2 ** 96)
        price = sqrt_price ** 2
        price = price * (10 ** decimals0) / (10 ** decimals1)
        return price
    except Exception as e:
        logger.warning(f"Failed to calculate price from sqrtPriceX96: {e}")
        return 0.0

async def search_pools_subgraph(
    network: str,
    token0_address: str,
    token1_address: str,
    fee_tier: Optional[int] = None,
    dex: str = "uniswap_v3"
) -> List[Dict]:
    """Sucht Pools mit neuem Query Format"""
    subgraph_url = get_subgraph_url(network, dex)
    if not subgraph_url:
        logger.error(f"No subgraph URL for {dex} on {network}")
        return []
    
    query = """
    query($token0: String!, $token1: String!) {
        pools0: pools(
            first: 5
            orderBy: totalValueLockedUSD
            orderDirection: desc
            where: {
                token0: $token0
                token1: $token1
            }
        ) {
            id
            token0 {
                id
                symbol
                decimals
                name
            }
            token1 {
                id
                symbol
                decimals
                name
            }
            feeTier
            liquidity
            sqrtPrice
            tick
            token0Price
            token1Price
            volumeUSD
            totalValueLockedUSD
            totalValueLockedToken0
            totalValueLockedToken1
        }
        pools1: pools(
            first: 5
            orderBy: totalValueLockedUSD
            orderDirection: desc
            where: {
                token0: $token1
                token1: $token0
            }
        ) {
            id
            token0 {
                id
                symbol
                decimals
                name
            }
            token1 {
                id
                symbol
                decimals
                name
            }
            feeTier
            liquidity
            sqrtPrice
            tick
            token0Price
            token1Price
            volumeUSD
            totalValueLockedUSD
            totalValueLockedToken0
            totalValueLockedToken1
        }
    }
    """
    
    variables = {
        "token0": token0_address.lower(),
        "token1": token1_address.lower()
    }
    
    try:
        logger.info(f"  📡 Querying {dex}: {subgraph_url[:60]}...")
        
        # FIX 5: Properly close aiohttp session with async context manager
        async with aiohttp.ClientSession() as session:
            # FIX 2: Timeout erhöht von 15s auf 60s für The Graph API
            async with session.post(
                subgraph_url,
                json={"query": query, "variables": variables},
                timeout=aiohttp.ClientTimeout(total=60),  # ← FIXED: 60 Sekunden statt 15
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Subgraph returned status {resp.status}")
                    return []
                
                result = await resp.json()
                
                if "errors" in result:
                    error_messages = [e.get("message", str(e)) for e in result["errors"]]
                    logger.error(f"Subgraph query errors: {error_messages}")
                    return []
                
                data = result.get("data", {})
                pools = data.get("pools0", []) + data.get("pools1", [])
                
                # Remove duplicates
                seen = set()
                unique_pools = []
                for pool in pools:
                    pool_id = pool.get("id", "").lower()
                    if pool_id not in seen:
                        seen.add(pool_id)
                        unique_pools.append(pool)
                
                # Filter by fee tier
                if fee_tier is not None:
                    unique_pools = [p for p in unique_pools if int(p.get("feeTier", 0)) == fee_tier]
                
                logger.info(f"  ✅ Found {len(unique_pools)} unique pools")
                return unique_pools
                
    except Exception as e:
        logger.error(f"  ❌ Subgraph request failed: {e}", exc_info=True)
        return []

def format_pool_response(pool_data: Dict, network: str, dex: str = "uniswap_v3") -> Dict:
    """Formatiert Subgraph Pool-Daten für API Response"""
    try:
        token0 = pool_data.get("token0", {})
        token1 = pool_data.get("token1", {})
        
        sqrt_price_x96 = pool_data.get("sqrtPrice", "0")
        decimals0 = int(token0.get("decimals", 18))
        decimals1 = int(token1.get("decimals", 18))
        current_price = sqrt_price_x96_to_price(sqrt_price_x96, decimals0, decimals1)
        
        tvl_usd = float(pool_data.get("totalValueLockedUSD", 0))
        volume_usd = float(pool_data.get("volumeUSD", 0))
        liquidity = float(pool_data.get("liquidity", 0))
        
        fee_tier = int(pool_data.get("feeTier", 0))
        tick_spacing_map = {500: 10, 3000: 60, 10000: 200}
        tick_spacing = tick_spacing_map.get(fee_tier, 60)
        
        return {
            "address": pool_data.get("id"),
            "dex": dex,
            "network": network,
            "fee_tier": fee_tier,
            "tvl_usd": tvl_usd,
            "volume_24h": volume_usd,
            "liquidity": liquidity,
            "tick_spacing": tick_spacing,
            "current_tick": int(pool_data.get("tick", 0)),
            "current_price": current_price,
            "token0": {
                "address": token0.get("id"),
                "symbol": token0.get("symbol"),
                "decimals": decimals0,
                "name": token0.get("name", "")
            },
            "token1": {
                "address": token1.get("id"),
                "symbol": token1.get("symbol"),
                "decimals": decimals1,
                "name": token1.get("name", "")
            }
        }
    except Exception as e:
        logger.error(f"Failed to format pool response: {e}")
        return {}

# ============================================================================
# FIXED DEX POOLS ENDPOINT - Route korrigiert
# ============================================================================
@router.get("/dex/pools/{network}/{token0}/{token1}")
async def get_dex_pools(
    network: str,
    token0: str,
    token1: str,
    dex: str = Query("uniswap_v3", description="DEX to query (uniswap_v3, pancakeswap, curve_v2)"),
    fee_tier: Optional[int] = Query(None, description="Filter by fee tier (500, 3000, 10000)")
):
    """
    Liste verfügbare Pools für ein Trading Pair
    
    FIXED: Route changed from /dex/pools/{dex}/{network}/{token0}/{token1}
                        to /dex/pools/{network}/{token0}/{token1}?dex=uniswap_v3
    
    Example: GET /dex/pools/ethereum/WETH/USDC?dex=uniswap_v3&fee_tier=3000
    """
    logger.info("=" * 80)
    logger.info(f"🔍 DEX POOLS REQUEST - {dex.upper()}")
    logger.info("=" * 80)
    logger.info(f"📥 Parameters:")
    logger.info(f"  - network: {network}")
    logger.info(f"  - token0: {token0}")
    logger.info(f"  - token1: {token1}")
    logger.info(f"  - dex: {dex}")
    logger.info(f"  - fee_tier: {fee_tier}")
    
    try:
        # Validate DEX
        valid_dexes = ["uniswap_v3", "curve_v2", "pancakeswap"]
        if dex.lower() not in valid_dexes:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid DEX: {dex}. Valid options: {valid_dexes}"
            )
        
        # Validate Network based on DEX
        if dex.lower() == "pancakeswap":
            valid_networks = ["bsc", "ethereum", "arbitrum"]
        else:
            valid_networks = ["ethereum", "polygon", "arbitrum", "optimism", "base"]
        
        if network.lower() not in valid_networks:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid network for {dex}: {network}. Valid options: {valid_networks}"
            )
        
        # Resolve Token Addresses
        token0_address = resolve_token_address(network, token0)
        token1_address = resolve_token_address(network, token1)
        
        if not token0_address:
            raise HTTPException(
                status_code=422,
                detail=f"Unknown token: {token0} on {network}. Supported: {list(TOKEN_ADDRESSES.get(network, {}).keys())}"
            )
        
        if not token1_address:
            raise HTTPException(
                status_code=422,
                detail=f"Unknown token: {token1} on {network}. Supported: {list(TOKEN_ADDRESSES.get(network, {}).keys())}"
            )
        
        logger.info(f"  ✅ Token0 ({token0}): {token0_address}")
        logger.info(f"  ✅ Token1 ({token1}): {token1_address}")
        
        # Search pools
        pools_raw = await search_pools_subgraph(
            network=network,
            token0_address=token0_address,
            token1_address=token1_address,
            fee_tier=fee_tier,
            dex=dex.lower()
        )
        
        if not pools_raw:
            return {
                "dex": dex,
                "network": network,
                "pair": f"{token0}/{token1}",
                "pools": [],
                "timestamp": datetime.utcnow().isoformat(),
                "_note": "No pools found."
            }
        
        # Format pools
        pools = []
        for pool_raw in pools_raw:
            formatted = format_pool_response(pool_raw, network, dex.lower())
            if formatted:
                pools.append(formatted)
        
        # Sort by TVL
        pools.sort(key=lambda p: p.get("tvl_usd", 0), reverse=True)
        
        response = {
            "dex": dex,
            "network": network,
            "pair": f"{token0}/{token1}",
            "pools": pools,
            "timestamp": datetime.utcnow().isoformat(),
            "_data_source": f"{dex}_subgraph",
            "_total_pools": len(pools)
        }
        
        logger.info("=" * 80)
        logger.info(f"✅ DEX POOLS - SUCCESS ({len(pools)} pools)")
        logger.info("=" * 80)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ DEX POOLS - ERROR")
        logger.error("=" * 80)
        logger.error(f"Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dex/liquidity/{pool_address}")
async def get_pool_liquidity(
    pool_address: str,
    bucket_size: float = Query(50.0, description="Price bucket size in USD"),
    range_multiplier: float = Query(2.0, description="Price range multiplier (2.0 = ±100%)")
):
    """Holt aktuelle Liquiditätsverteilung für einen spezifischen Pool"""
    logger.info("💧 POOL LIQUIDITY REQUEST")
    
    uniswap = None
    try:
        if not pool_address.startswith("0x") or len(pool_address) != 42:
            raise HTTPException(status_code=422, detail="Invalid pool address format")
        
        from app.core.orderbook_heatmap.exchanges.dex.uniswap_v3 import UniswapV3Exchange
        
        uniswap = UniswapV3Exchange()
        
        pool_info = await uniswap.get_pool_info(pool_address)
        
        if not pool_info:
            raise HTTPException(status_code=404, detail=f"Pool not found: {pool_address}")
        
        ticks = await uniswap.get_liquidity_ticks(pool_address)
        
        sqrt_price_x96 = int(pool_info.get("sqrtPrice", 0))
        current_price = uniswap._sqrt_price_to_price(sqrt_price_x96)
        
        # Build liquidity distribution
        liquidity_distribution = []
        price_lower_bound = current_price / range_multiplier
        price_upper_bound = current_price * range_multiplier
        
        for tick in ticks:
            if price_lower_bound <= tick.price_lower <= price_upper_bound:
                liquidity_distribution.append({
                    "price_lower": tick.price_lower,
                    "price_upper": tick.price_upper,
                    "tick_lower": tick.tick_index,
                    "tick_upper": tick.tick_index + 1,
                    "liquidity": tick.liquidity,
                    "liquidity_usd": tick.liquidity * current_price,
                })
        
        total_liquidity = sum(t.liquidity for t in ticks)
        
        def calc_concentration(tolerance_pct: float) -> float:
            lower = current_price * (1 - tolerance_pct / 100)
            upper = current_price * (1 + tolerance_pct / 100)
            concentrated = sum(t.liquidity for t in ticks if lower <= t.price_lower <= upper)
            return (concentrated / total_liquidity * 100) if total_liquidity > 0 else 0.0
        
        response = {
            "pool_address": pool_address,
            "pair": f"{pool_info.get('token0', {}).get('symbol', 'TOKEN0')}/{pool_info.get('token1', {}).get('symbol', 'TOKEN1')}",
            "current_price": current_price,
            "total_liquidity": total_liquidity,
            "tvl_usd": total_liquidity * current_price,
            "liquidity_distribution": liquidity_distribution,
            "concentration_metrics": {
                "within_1_percent": calc_concentration(1.0),
                "within_2_percent": calc_concentration(2.0),
                "within_5_percent": calc_concentration(5.0)
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        logger.info("✅ POOL LIQUIDITY - SUCCESS")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # CRITICAL: Close the session to prevent leaks
        if uniswap is not None:
            try:
                await uniswap.disconnect()
            except Exception as e:
                logger.warning(f"Error closing UniswapV3Exchange session: {e}")

@router.get("/dex/virtual-orderbook/{pool_address}")
async def get_virtual_orderbook(
    pool_address: str,
    depth: int = Query(100, ge=10, le=500, description="Number of price levels per side")
):
    """Generiert CEX-Style Orderbook aus DEX Liquiditätskurve"""
    logger.info("📖 VIRTUAL ORDERBOOK REQUEST")
    
    uniswap = None
    try:
        from app.core.orderbook_heatmap.exchanges.dex.uniswap_v3 import UniswapV3Exchange
        
        uniswap = UniswapV3Exchange()
        
        orderbook = await uniswap.get_orderbook_snapshot(pool_address, limit=depth)
        
        if not orderbook:
            raise HTTPException(status_code=404, detail=f"Could not generate orderbook for pool: {pool_address}")
        
        response = {
            "exchange": "uniswap_v3",
            "symbol": orderbook.symbol,
            "source_type": "DEX",
            "is_virtual": True,
            "pool_address": pool_address,
            "bids": [[level.price, level.quantity] for level in orderbook.bids.levels],
            "asks": [[level.price, level.quantity] for level in orderbook.asks.levels],
            "timestamp": orderbook.timestamp.isoformat()
        }
        
        logger.info("✅ VIRTUAL ORDERBOOK - SUCCESS")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # CRITICAL: Close the session to prevent leaks
        if uniswap is not None:
            try:
                await uniswap.disconnect()
            except Exception as e:
                logger.warning(f"Error closing UniswapV3Exchange session: {e}")

# Price helpers
async def get_current_price_from_aggregator(symbol: str) -> float:
    global aggregator
    if not aggregator:
        return 0.0
    try:
        agg_orderbook = await aggregator.get_aggregated_orderbook(symbol)
        if not agg_orderbook or not agg_orderbook.orderbooks:
            return 0.0
        best_bid = 0.0
        best_ask = float('inf')
        for exchange_name, orderbook in agg_orderbook.orderbooks.items():
            if orderbook.bids.levels:
                exchange_best_bid = max(level.price for level in orderbook.bids.levels)
                best_bid = max(best_bid, exchange_best_bid)
            if orderbook.asks.levels:
                exchange_best_ask = min(level.price for level in orderbook.asks.levels)
                best_ask = min(best_ask, exchange_best_ask)
        if best_bid > 0 and best_ask < float('inf'):
            return (best_bid + best_ask) / 2
        return 0.0
    except Exception as e:
        logger.debug(f"Aggregator price not available: {e}")
        return 0.0

async def get_current_price_from_binance(symbol: str) -> float:
    try:
        binance_symbol = symbol.replace("/", "").upper()
        url = "https://api.binance.com/api/v3/ticker/price"
        
        # FIX 5: Properly close aiohttp session
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={"symbol": binance_symbol}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["price"])
                return 0.0
    except Exception as e:
        logger.error(f"Error getting price from Binance: {e}")
        return 0.0

async def get_current_price(symbol: str) -> float:
    price = await get_current_price_from_aggregator(symbol)
    if price > 0:
        return price
    price = await get_current_price_from_binance(symbol)
    if price > 0:
        return price
    return 0.0

@router.websocket("/ws/price/{symbol}")
async def price_websocket_endpoint(websocket: WebSocket, symbol: str):
    normalized_symbol = symbol.replace(".", "/")
    await websocket.accept()
    try:
        while True:
            try:
                price = await get_current_price(normalized_symbol)
                source = "aggregator" if aggregator else "binance"
                message = {
                    "type": "price_update",
                    "symbol": normalized_symbol,
                    "price": price,
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": source
                }
                await websocket.send_json(message)
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in price update loop: {e}")
                break
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"Price WS error: {e}")
        try:
            await websocket.close(code=1011, reason=str(e))
        except:
            pass

# ============================================================================
# CEX LAYER 2 HEATMAP ENDPOINTS (Bitget public API)
# ============================================================================

_bitget_l2 = BitgetL2DataFetcher()


@router.get("/cex/l2/networks")
async def get_l2_networks():
    """
    Gibt alle unterstützten L2-Netzwerke und ihre Token zurück.
    """
    return {
        "source": "bitget_cex",
        "networks": {
            net: list(tokens.keys())
            for net, tokens in L2_TOKEN_MAP.items()
        },
        "total_networks": len(L2_TOKEN_MAP),
    }


@router.get("/cex/l2/heatmap")
async def get_cex_l2_heatmap(
    limit: int = Query(50, ge=5, le=150, description="Orderbook-Tiefe pro Token"),
):
    """
    Holt CEX Layer-2-Liquiditätsdaten von Bitget für alle unterstützten L2-Netzwerke.

    Gibt zurück:
    - Orderbook-Snapshots (bids/asks) für jeden L2-Token
    - Ticker-Daten (Preis, 24h-Vol, Change)
    - Aggregierte Heatmap-Matrix: [Netzwerk × Metrik]
      Metriken: bid_depth_usd | ask_depth_usd | volume_24h (USD)

    Verwendung im Frontend:
    - matrix[i][0] = Bid-Tiefe des i-ten Netzwerks in USD
    - matrix[i][1] = Ask-Tiefe des i-ten Netzwerks in USD
    - matrix[i][2] = 24h-Handelsvolumen in USD
    """
    logger.info("=" * 60)
    logger.info("📊 CEX L2 HEATMAP REQUEST (Bitget)")
    logger.info("=" * 60)

    try:
        data = await _bitget_l2.get_all_l2_heatmap_data(limit=limit)
        data["timestamp"] = datetime.utcnow().isoformat()
        logger.info(
            f"✅ CEX L2 Heatmap: {len(data.get('networks', {}))} networks fetched"
        )
        return data

    except Exception as e:
        logger.error(f"❌ CEX L2 Heatmap error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cex/l2/{network}")
async def get_cex_l2_network(
    network: str,
    limit: int = Query(50, ge=5, le=150, description="Orderbook-Tiefe pro Token"),
):
    """
    Holt CEX L2-Orderbook-Daten für ein einzelnes Netzwerk von Bitget.

    Beispiel: GET /cex/l2/arbitrum
    Gibt Orderbook-Snapshots für ARB/USDT zurück.
    """
    network = network.lower()
    if network not in L2_TOKEN_MAP:
        raise HTTPException(
            status_code=422,
            detail=f"Netzwerk '{network}' nicht unterstützt. Verfügbar: {list(L2_TOKEN_MAP.keys())}",
        )

    logger.info(f"📊 CEX L2 Network Request: {network}")

    try:
        result = await _bitget_l2.get_l2_orderbooks_for_network(network, limit=limit)

        if not result:
            return {
                "network":   network,
                "tokens":    {},
                "timestamp": datetime.utcnow().isoformat(),
                "_note":     "Keine Daten von Bitget erhalten.",
            }

        # Bid/Ask-Tiefe in USD berechnen
        enriched: dict = {}
        for token, ob in result.items():
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            best_bid = float(bids[0][0]) if bids else 0.0
            best_ask = float(asks[0][0]) if asks else 0.0
            mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0.0
            enriched[token] = {
                **ob,
                "mid_price":     round(mid, 6),
                "bid_depth_usd": round(sum(float(p) * float(q) for p, q in bids if p and q), 2),
                "ask_depth_usd": round(sum(float(p) * float(q) for p, q in asks if p and q), 2),
            }

        return {
            "source":    "bitget_cex",
            "network":   network,
            "tokens":    enriched,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"❌ CEX L2 {network} error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# MARKOV L2 SIMULATOR ENDPOINT
# ===========================================================================

@router.post("/markov/l2-simulate")
async def markov_l2_simulate(
    token: str = Query("ARB", description="Token-Symbol, z.B. ARB, OP, POL, IMX"),
    network: str = Query("arbitrum", description="L2-Netzwerk, z.B. arbitrum, optimism, polygon"),
    n_snapshots: int = Query(20, ge=5, le=80, description="Anzahl Live-Snapshots für Training"),
    n_paths: int = Query(300, ge=50, le=2000, description="Anzahl Monte-Carlo-Pfade"),
    n_steps: int = Query(50, ge=10, le=500, description="Schritte pro Simulationspfad"),
    price_step_std: Optional[float] = Query(None, ge=0.0, description="Absolute Volatilität in USD (None = auto-kalibriert)"),
    price_step_pct: Optional[float] = Query(None, ge=0.0001, le=0.5, description="Volatilität als Anteil vom Preis, z.B. 0.005 = 0.5% (None = auto)"),
    volatility_multiplier: float = Query(1.0, ge=0.1, le=10.0, description="Multiplikator auf auto-berechnete Volatilität (1.0 = neutral, >1 aggressiver)"),
    wall_bounce_factor: float = Query(0.7, ge=0.1, le=1.0, description="Reflexionswahrscheinlichkeits-Faktor"),
    persistence_window: int = Query(5, ge=2, le=20, description="Min. Snapshots für Wall-Aktivierung"),
    interval_seconds: float = Query(0.5, ge=0.1, le=5.0, description="Pause zwischen Snapshots in Sekunden"),
    seed: Optional[int] = Query(None, description="Random Seed für Reproduzierbarkeit"),
    limit: int = Query(50, ge=5, le=150, description="Orderbook-Tiefe pro Abruf"),
):
    """
    Markov-Monte-Carlo-Simulation basierend auf Live-L2-Orderbuch-Daten von Bitget.

    Workflow:
    1. Sammelt n_snapshots Orderbook-Snapshots vom Bitget CEX für das angegebene L2-Token
    2. Trainiert eine Markov-Übergangsmatrix auf den Snapshots (Wall-Erkennung, Zonen-Klassifikation)
    3. Führt n_paths Monte-Carlo-Simulationen durch (n_steps Schritte je Pfad)
    4. Gibt Preis-Fan (5/25/50/75/95-Perzentil), Statistiken und erkannte Liquiditätswände zurück

    Hinweis: Die Snapshot-Sammlung dauert ca. n_snapshots × interval_seconds Sekunden.
    """
    from app.core.orderbook_heatmap.markov.l2_simulator import (
        run_l2_markov_simulation,
        check_imports,
    )

    # Import-Check
    ok, err = check_imports()
    if not ok:
        raise HTTPException(
            status_code=503,
            detail=f"Backtest-Simulator nicht verfügbar: {err}",
        )

    # Token/Netzwerk validieren
    network_lower = network.lower()
    token_upper = token.upper()
    if network_lower not in L2_TOKEN_MAP:
        raise HTTPException(
            status_code=422,
            detail=f"Netzwerk '{network}' nicht unterstützt. Verfügbar: {list(L2_TOKEN_MAP.keys())}",
        )
    if token_upper not in L2_TOKEN_MAP[network_lower]:
        available_tokens = list(L2_TOKEN_MAP[network_lower].keys())
        raise HTTPException(
            status_code=422,
            detail=f"Token '{token}' für Netzwerk '{network}' nicht verfügbar. Verfügbar: {available_tokens}",
        )

    bitget_symbol = L2_TOKEN_MAP[network_lower][token_upper]
    display_symbol = f"{token_upper}/USDT"

    logger.info(
        f"📊 Markov L2 Simulation: {display_symbol} ({network}) | "
        f"snapshots={n_snapshots}, paths={n_paths}, steps={n_steps}"
    )

    # Snapshots sammeln
    raw_snapshots: List[Dict[str, Any]] = []
    for i in range(n_snapshots):
        ob = await _bitget_l2.get_orderbook(bitget_symbol, limit=limit)
        if ob is not None:
            raw_snapshots.append(ob)
        if i < n_snapshots - 1:
            await asyncio.sleep(interval_seconds)

    if len(raw_snapshots) < persistence_window + 2:
        raise HTTPException(
            status_code=502,
            detail=(
                f"Bitget lieferte nur {len(raw_snapshots)} verwertbare Snapshots "
                f"(Minimum: {persistence_window + 2}). Bitte später erneut versuchen."
            ),
        )

    logger.info(f"✅ {len(raw_snapshots)} Snapshots gesammelt, starte Simulation...")

    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: run_l2_markov_simulation(
                snapshots=raw_snapshots,
                symbol=display_symbol,
                n_paths=n_paths,
                n_steps=n_steps,
                price_step_std=price_step_std,
                price_step_pct=price_step_pct,
                volatility_multiplier=volatility_multiplier,
                wall_bounce_factor=wall_bounce_factor,
                persistence_window=persistence_window,
                seed=seed,
            ),
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except ImportError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Markov Simulation Fehler: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "success": True,
        "token": token_upper,
        "network": network_lower,
        "bitget_symbol": bitget_symbol,
        **result,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ===========================================================================
# MARKOV L2 WEBSOCKET STREAM
# ===========================================================================

class MarkovStreamBuffer:
    """
    Thread-safe Rolling Buffer of raw Bitget orderbook snapshots.
    Shared across concurrent WebSocket connections for the same (token, network).
    """
    def __init__(self, max_size: int = 120):
        self._buffers: Dict[str, List[Dict]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self.max_size = max_size

    def _key(self, token: str, network: str) -> str:
        return f"{token.upper()}:{network.lower()}"

    def _ensure_key(self, key: str):
        if key not in self._buffers:
            self._buffers[key] = []
            self._locks[key] = asyncio.Lock()

    async def push(self, token: str, network: str, snapshot: Dict):
        key = self._key(token, network)
        self._ensure_key(key)
        async with self._locks[key]:
            self._buffers[key].append(snapshot)
            if len(self._buffers[key]) > self.max_size:
                self._buffers[key].pop(0)

    async def get(self, token: str, network: str) -> List[Dict]:
        key = self._key(token, network)
        self._ensure_key(key)
        async with self._locks[key]:
            return list(self._buffers[key])

    def size(self, token: str, network: str) -> int:
        key = self._key(token, network)
        return len(self._buffers.get(key, []))


_markov_stream_buffer = MarkovStreamBuffer(max_size=120)


async def collect_snapshots_task(
    token: str,
    network: str,
    bitget_symbol: str,
    interval_seconds: float,
    limit: int,
    stop_event: asyncio.Event,
):
    while not stop_event.is_set():
        try:
            ob = await _bitget_l2.get_orderbook(bitget_symbol, limit=limit)
            if ob:
                await _markov_stream_buffer.push(token, network, ob)
                logger.debug(
                    f"📦 Markov buffer [{token}/{network}]: "
                    f"{_markov_stream_buffer.size(token, network)} snapshots"
                )
        except Exception as e:
            logger.warning(f"⚠️ Snapshot collection error [{token}/{network}]: {e}")

        try:
            await asyncio.wait_for(
                asyncio.shield(stop_event.wait()),
                timeout=interval_seconds,
            )
        except asyncio.TimeoutError:
            pass


async def simulate_and_stream_task(
    websocket: WebSocket,
    token: str,
    network: str,
    symbol: str,
    n_paths: int,
    n_steps: int,
    volatility_multiplier: float,
    wall_bounce_factor: float,
    retrain_every: int,
    min_snapshots: int,
    stop_event: asyncio.Event,
):
    from app.core.orderbook_heatmap.markov.l2_simulator import run_l2_markov_simulation
    loop = asyncio.get_event_loop()

    # Wait until enough snapshots are collected
    while not stop_event.is_set():
        buf_size = _markov_stream_buffer.size(token, network)
        if buf_size >= min_snapshots:
            break
        await websocket.send_json({
            "type": "markov_collecting",
            "token": token,
            "network": network,
            "snapshots_collected": buf_size,
            "snapshots_needed": min_snapshots,
            "message": f"Collecting snapshots... {buf_size}/{min_snapshots}",
        })
        await asyncio.sleep(2)

    if stop_event.is_set():
        return

    # Main loop: simulate + stream
    while not stop_event.is_set():
        try:
            snapshots = await _markov_stream_buffer.get(token, network)

            result = await loop.run_in_executor(
                None,
                lambda: run_l2_markov_simulation(
                    snapshots=snapshots,
                    symbol=symbol,
                    n_paths=n_paths,
                    n_steps=n_steps,
                    volatility_multiplier=volatility_multiplier,
                    wall_bounce_factor=wall_bounce_factor,
                ),
            )

            if result:
                await websocket.send_json({
                    "type": "markov_update",
                    "token": token,
                    "network": network,
                    **result,
                    "buffer_size": len(snapshots),
                    "timestamp": datetime.utcnow().isoformat(),
                })
                logger.info(
                    f"📡 Markov stream sent [{token}/{network}] "
                    f"fan_steps={len(result.get('price_fan', {}).get('p50', []))}"
                )

        except Exception as e:
            logger.error(f"❌ Markov simulation error [{token}/{network}]: {e}", exc_info=True)
            await websocket.send_json({
                "type": "markov_error",
                "token": token,
                "network": network,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            })

        try:
            await asyncio.wait_for(
                asyncio.shield(stop_event.wait()),
                timeout=retrain_every,
            )
        except asyncio.TimeoutError:
            pass


@router.websocket("/markov/l2-stream")
async def markov_l2_stream(
    websocket: WebSocket,
    token: str = Query("ARB"),
    network: str = Query("arbitrum"),
    retrain_every: int = Query(30, ge=10, le=300),
    min_snapshots: int = Query(15, ge=5, le=80),
    interval_seconds: float = Query(1.0, ge=0.5, le=5.0),
    n_paths: int = Query(200, ge=50, le=1000),
    n_steps: int = Query(50, ge=10, le=200),
    volatility_multiplier: float = Query(1.0, ge=0.1, le=5.0),
    wall_bounce_factor: float = Query(0.7, ge=0.1, le=1.0),
    limit: int = Query(50, ge=5, le=150),
):
    token_upper = token.upper()
    network_lower = network.lower()

    if network_lower not in L2_TOKEN_MAP or token_upper not in L2_TOKEN_MAP[network_lower]:
        await websocket.accept()
        await websocket.send_json({
            "type": "markov_error",
            "error": f"Unknown token/network: {token_upper}/{network_lower}",
            "valid_networks": list(L2_TOKEN_MAP.keys()),
        })
        await websocket.close(code=1008)
        return

    bitget_symbol = L2_TOKEN_MAP[network_lower][token_upper]
    display_symbol = f"{token_upper}/USDT"

    await websocket.accept()
    logger.info(f"🔌 Markov WS connected: {token_upper}/{network_lower} retrain={retrain_every}s")

    await websocket.send_json({
        "type": "markov_connected",
        "token": token_upper,
        "network": network_lower,
        "bitget_symbol": bitget_symbol,
        "retrain_every": retrain_every,
        "min_snapshots": min_snapshots,
        "n_paths": n_paths,
        "n_steps": n_steps,
        "message": f"Connected. Collecting {min_snapshots} snapshots before first simulation...",
    })

    stop_event = asyncio.Event()

    collector_task = asyncio.create_task(
        collect_snapshots_task(
            token=token_upper,
            network=network_lower,
            bitget_symbol=bitget_symbol,
            interval_seconds=interval_seconds,
            limit=limit,
            stop_event=stop_event,
        )
    )

    simulator_task = asyncio.create_task(
        simulate_and_stream_task(
            websocket=websocket,
            token=token_upper,
            network=network_lower,
            symbol=display_symbol,
            n_paths=n_paths,
            n_steps=n_steps,
            volatility_multiplier=volatility_multiplier,
            wall_bounce_factor=wall_bounce_factor,
            retrain_every=retrain_every,
            min_snapshots=min_snapshots,
            stop_event=stop_event,
        )
    )

    try:
        while True:
            msg = await websocket.receive_text()
            try:
                cmd = json.loads(msg)
                if cmd.get("action") == "ping":
                    await websocket.send_json({"type": "pong"})
                elif cmd.get("action") == "force_retrain":
                    logger.info(f"🔄 Force retrain requested [{token_upper}/{network_lower}]")
            except json.JSONDecodeError:
                pass

    except WebSocketDisconnect:
        logger.info(f"🔌 Markov WS disconnected: {token_upper}/{network_lower}")
    except Exception as e:
        logger.error(f"❌ Markov WS error: {e}", exc_info=True)
    finally:
        stop_event.set()
        collector_task.cancel()
        simulator_task.cancel()
        try:
            await asyncio.gather(collector_task, simulator_task, return_exceptions=True)
        except Exception:
            pass
        try:
            await websocket.close()
        except Exception:
            pass


# ===========================================================================
# HMM MARKOV REGIME DETECTOR ENDPOINT
# ===========================================================================

@router.post("/markov/regime-signals")
async def hmm_regime_signals(request: HMMRegimeRequest):
    """
    HMM-basierte Marktregime-Erkennung via Gaussian Hidden Markov Model.

    Klassifiziert OHLCV-Bars in N diskrete Hidden States (z.B. Bär/Seitwärts/Bulle).
    Identifiziert automatisch den "Bull State" anhand des höchsten mittleren Log-Returns.

    Workflow:
    1. GaussianHMM auf den ersten train_bars Bars trainieren
    2. Für jeden Bar: Viterbi-Dekodierung → Hidden State
    3. Bull-State-Detektion: State mit höchstem mean log-return
    4. Signale: +1 (Einstieg), -1 (Ausstieg), 0 (Halten)

    Hinweis: CPU-intensiv — läuft im ThreadPoolExecutor (non-blocking).
    Benötigt hmmlearn: pip install hmmlearn
    """
    from app.core.orderbook_heatmap.markov.hmm_regime import (
        run_hmm_regime,
        check_imports,
    )
    from concurrent.futures import ThreadPoolExecutor

    # Import-Check
    ok, err = check_imports()
    if not ok:
        raise HTTPException(
            status_code=503,
            detail=(
                f"hmmlearn nicht verfügbar: {err}. "
                f"Installiere mit: pip install hmmlearn>=0.3.0"
            ),
        )

    n_bars = len(request.data)
    min_bars = request.train_bars + 10
    if n_bars < min_bars:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Zu wenig Bars: {n_bars} übergeben, mindestens {min_bars} erforderlich "
                f"(train_bars={request.train_bars} + 10 Puffer)."
            ),
        )

    ohlcv_list = [bar.dict() for bar in request.data]
    params = {
        "n_states":        request.n_states,
        "train_bars":      request.train_bars,
        "use_volume":      request.use_volume,
        "retrain_every":   request.retrain_every,
        "covariance_type": request.covariance_type,
        "n_iter":          request.n_iter,
    }

    logger.info(
        f"📊 HMM Regime: {n_bars} Bars | n_states={request.n_states} | "
        f"train_bars={request.train_bars} | use_volume={request.use_volume}"
    )

    try:
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(
                pool,
                lambda: run_hmm_regime(ohlcv_list, params, request.max_signal_bars),
            )
    except ImportError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"❌ HMM Regime Fehler: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    return {"success": True, **result}


@router.get("/price/{symbol}")
async def get_price_endpoint(symbol: str):
    normalized_symbol = symbol.replace(".", "/")
    try:
        price = await get_current_price(normalized_symbol)
        return {
            "success": True,
            "symbol": normalized_symbol,
            "price": price,
            "timestamp": datetime.utcnow().isoformat(),
            "source": "aggregator" if aggregator else "binance"
        }
    except Exception as e:
        logger.error(f"Error getting price: {e}")
        raise HTTPException(status_code=500, detail=str(e))
