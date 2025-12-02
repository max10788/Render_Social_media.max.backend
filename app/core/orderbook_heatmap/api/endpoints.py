"""
FastAPI Endpoints für Orderbook Heatmap
"""
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List
from pydantic import BaseModel, Field, ValidationError
import asyncio
import json
import logging
from datetime import datetime
import aiohttp

from app.core.orderbook_heatmap.models.orderbook import Exchange, AggregatedOrderbook
from app.core.orderbook_heatmap.models.heatmap import HeatmapConfig, HeatmapSnapshot, HeatmapTimeSeries
from app.core.orderbook_heatmap.exchanges.binance import BinanceExchange
from app.core.orderbook_heatmap.exchanges.bitget import BitgetExchange
from app.core.orderbook_heatmap.exchanges.kraken import KrakenExchange
from app.core.orderbook_heatmap.exchanges.dex.uniswap_v3 import UniswapV3Exchange
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
        json_schema_extra = {
            "example": {
                "symbol": "BTC/USDT",
                "exchanges": ["binance", "bitget"],
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
        valid_exchanges = ["binance", "bitget", "kraken", "uniswap_v3", "raydium"]
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
            from app.core.orderbook_heatmap.aggregator.orderbook_aggregator import OrderbookAggregator
            from app.core.orderbook_heatmap.websocket.manager import WebSocketManager
            
            aggregator = OrderbookAggregator(config)
            logger.info("  ✅ OrderbookAggregator initialized")
            
            logger.info("📡 Initializing exchanges...")
            exchange_map = {}
            
            for exchange_name in data.exchanges:
                try:
                    if exchange_name == "binance":
                        from app.core.orderbook_heatmap.exchanges.binance import BinanceExchange
                        exchange_map[exchange_name] = BinanceExchange()
                        logger.info(f"  ✅ BinanceExchange loaded")
                    elif exchange_name == "bitget":
                        from app.core.orderbook_heatmap.exchanges.bitget import BitgetExchange
                        exchange_map[exchange_name] = BitgetExchange()
                        logger.info(f"  ✅ BitgetExchange loaded")
                    elif exchange_name == "kraken":
                        from app.core.orderbook_heatmap.exchanges.kraken import KrakenExchange
                        exchange_map[exchange_name] = KrakenExchange()
                        logger.info(f"  ✅ KrakenExchange loaded")
                    elif exchange_name == "uniswap_v3":
                        from app.core.orderbook_heatmap.exchanges.dex.uniswap_v3 import UniswapV3Exchange
                        exchange_map[exchange_name] = UniswapV3Exchange()
                        logger.info(f"  ✅ UniswapV3Exchange loaded")
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
        
        if not snapshot:
            logger.warning(f"  ⚠️ No snapshot available for {normalized_symbol}")
            raise HTTPException(status_code=404, detail=f"No snapshot available for {normalized_symbol}")
        
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
        return orderbook.model_dump()
        
    except Exception as e:
        logger.error(f"❌ Error getting orderbook: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    global ws_manager
    
    if ws_manager is None:
        try:
            from app.core.orderbook_heatmap.websocket.manager import WebSocketManager
            ws_manager = WebSocketManager()
        except ImportError:
            logger.error("❌ WebSocketManager not available")
            await websocket.close(code=1011, reason="WebSocket not available")
            return
    
    logger.info(f"🔌 WebSocket connection request for {symbol}")
    
    await ws_manager.connect(websocket, symbol)
    logger.info(f"  ✅ WebSocket connected for {symbol}")
    
    try:
        while True:
            data = await websocket.receive_text()
            logger.debug(f"  📥 WebSocket message: {data}")
            
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
        logger.info(f"  🔌 WebSocket disconnected for {symbol}")

@router.get("/exchanges")
async def get_available_exchanges():
    logger.info("📋 EXCHANGES LIST REQUEST")
    
    exchanges_list = {
        "exchanges": [
            {
                "name": "binance",
                "type": "cex",
                "description": "Binance Spot & Futures"
            },
            {
                "name": "bitget",
                "type": "cex",
                "description": "Bitget Spot & Futures"
            },
            {
                "name": "kraken",
                "type": "cex",
                "description": "Kraken Spot"
            },
            {
                "name": "uniswap_v3",
                "type": "dex",
                "description": "Uniswap v3 (Ethereum)",
                "requires_pool_address": True
            }
        ]
    }
    
    logger.info(f"  ✅ Returned {len(exchanges_list['exchanges'])} exchanges")
    return exchanges_list

@router.get("/health")
async def health_check():
    logger.debug("💚 HEALTH CHECK")
    
    response = {
        "status": "healthy",
        "service": "orderbook-heatmap",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    logger.debug(f"  ✅ {response}")
    return response

# ============================================================================
# PRICE HELPER FUNCTIONS
# ============================================================================

async def get_current_price_from_aggregator(symbol: str) -> float:
    global aggregator
    
    if not aggregator:
        return 0.0
    
    try:
        agg_orderbook = await aggregator.get_aggregated_orderbook(symbol)
        
        if not agg_orderbook or not agg_orderbook.orderbooks:
            return 0.0
        
        # Get best bid/ask across all exchanges
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
            mid_price = (best_bid + best_ask) / 2
            logger.debug(f"Mid-Price for {symbol}: ${mid_price:.2f} (bid: ${best_bid:.2f}, ask: ${best_ask:.2f})")
            return mid_price
        
        return 0.0
            
    except Exception as e:
        logger.debug(f"Aggregator price not available: {e}")
        return 0.0

async def get_current_price_from_binance(symbol: str) -> float:
    try:
        binance_symbol = symbol.replace("/", "").upper()
        url = "https://api.binance.com/api/v3/ticker/price"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={"symbol": binance_symbol}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = float(data["price"])
                    logger.debug(f"Binance price for {symbol}: ${price:.2f}")
                    return price
                return 0.0
                    
    except Exception as e:
        logger.error(f"Error getting price from Binance: {e}")
        return 0.0

async def get_current_price(symbol: str) -> float:
    price = await get_current_price_from_aggregator(symbol)
    if price > 0:
        return price
    
    logger.info(f"Aggregator unavailable, using Binance API for {symbol}")
    price = await get_current_price_from_binance(symbol)
    if price > 0:
        return price
    
    logger.error(f"Could not get price for {symbol} from any source")
    return 0.0

# ============================================================================
# PRICE WEBSOCKET
# ============================================================================

@router.websocket("/ws/price/{symbol}")
async def price_websocket_endpoint(websocket: WebSocket, symbol: str):
    normalized_symbol = symbol.replace(".", "/")
    logger.info(f"🔌 Price WS connection request for {normalized_symbol}")
    
    await websocket.accept()
    logger.info(f"✅ Price WS connected for {normalized_symbol}")
    
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
                logger.debug(f"💰 Price update sent: ${price:.2f} ({source})")
                
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("Price WS task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in price update loop: {e}")
                await asyncio.sleep(2)
                
    except WebSocketDisconnect:
        logger.info(f"🔌 Price WS disconnected for {normalized_symbol}")
    except Exception as e:
        logger.error(f"❌ Price WS error for {normalized_symbol}: {e}")
        await websocket.close(code=1011, reason=str(e))

# ============================================================================
# PRICE REST ENDPOINT (Testing)
# ============================================================================

@router.get("/price/{symbol}")
async def get_price_endpoint(symbol: str):
    normalized_symbol = symbol.replace(".", "/")
    logger.info(f"💰 Price request for {normalized_symbol}")
    
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
