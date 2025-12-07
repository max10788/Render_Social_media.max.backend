"""
FastAPI Endpoints für Orderbook Heatmap
FIXED VERSION - Neue The Graph URLs (Dezember 2024)
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
# DEX POOLS ENDPOINT - FIXED VERSION mit neuen Subgraph URLs
# ============================================================================

# ============================================================================
# ⚡ WICHTIGE ÄNDERUNG: NEUE SUBGRAPH IDS (Dezember 2024)
# Die alten URLs (api.thegraph.com/subgraphs/name/...) funktionieren nicht mehr!
# Neue IDs von: https://docs.uniswap.org/api/subgraph/overview
# ============================================================================

UNISWAP_V3_SUBGRAPH_IDS = {
    "ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    "polygon": "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
    "arbitrum": "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    "optimism": "Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj",
    "base": "43Hwfi3dJSoGpyas9VwNoDAv55yjgGrPpNSmbQZArzMG"
}

# Bekannte Token-Adressen für verschiedene Netzwerke
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
    }
}

# ============================================================================
# HELPER FUNCTIONS (FIXED)
# ============================================================================

def resolve_token_address(network: str, symbol: str) -> Optional[str]:
    """Löst Token-Symbol zu Contract-Adresse auf"""
    network_tokens = TOKEN_ADDRESSES.get(network.lower(), {})
    return network_tokens.get(symbol.upper())


def get_subgraph_url(network: str) -> Optional[str]:
    """
    🆕 NEUE FUNKTION - Holt Subgraph URL mit neuem Format
    
    Nutzt The Graph API Key falls vorhanden, sonst Fallback zu Messari
    """
    network = network.lower()
    
    # Hole API Key aus Environment
    api_key = os.getenv("THE_GRAPH_API_KEY", "")
    
    if not api_key:
        logger.warning("⚠️ THE_GRAPH_API_KEY not set! Using fallback Subgraph (slower)")
        logger.warning("💡 Get free API key at: https://thegraph.com/studio/")
        
        # Fallback: Messari Subgraphs (public, aber langsamer)
        messari_urls = {
            "ethereum": "https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-ethereum",
            "polygon": "https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-polygon",
            "arbitrum": "https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-arbitrum",
        }
        return messari_urls.get(network)
    
    # Offizielle Graph Network URLs mit API Key
    subgraph_id = UNISWAP_V3_SUBGRAPH_IDS.get(network)
    if subgraph_id:
        return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
    
    logger.error(f"No subgraph ID for network: {network}")
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
    fee_tier: Optional[int] = None
) -> List[Dict]:
    """
    🔄 UPDATED - Sucht Pools mit neuem Query Format
    
    Nutzt separates Query für beide Token-Reihenfolgen (pools0 + pools1)
    """
    subgraph_url = get_subgraph_url(network)
    if not subgraph_url:
        logger.error(f"No subgraph URL for network: {network}")
        return []
    
    # GraphQL Query - Kompatibel mit The Graph Network
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
            tickSpacing
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
            tickSpacing
        }
    }
    """
    
    variables = {
        "token0": token0_address.lower(),
        "token1": token1_address.lower()
    }
    
    try:
        logger.info(f"  📡 Querying: {subgraph_url[:60]}...")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                subgraph_url,
                json={"query": query, "variables": variables},
                timeout=aiohttp.ClientTimeout(total=15),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Subgraph returned status {resp.status}")
                    resp_text = await resp.text()
                    logger.error(f"Response: {resp_text[:200]}")
                    return []
                
                result = await resp.json()
                
                # Check for errors
                if "errors" in result:
                    error_messages = [e.get("message", str(e)) for e in result["errors"]]
                    logger.error(f"Subgraph query errors: {error_messages}")
                    
                    if any("endpoint has been removed" in msg.lower() for msg in error_messages):
                        logger.error("  ❌ OLD SUBGRAPH URL - Endpoint deprecated!")
                        logger.error("  💡 Solution: Set THE_GRAPH_API_KEY environment variable")
                    
                    return []
                
                # Kombiniere pools0 und pools1
                data = result.get("data", {})
                pools = data.get("pools0", []) + data.get("pools1", [])
                
                # Entferne Duplikate
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
                
    except asyncio.TimeoutError:
        logger.error("  ⏱️ Subgraph request timed out (15s)")
        return []
    except aiohttp.ClientError as e:
        logger.error(f"  🌐 Network error: {e}")
        return []
    except Exception as e:
        logger.error(f"  ❌ Subgraph request failed: {e}", exc_info=True)
        return []


def format_pool_response(pool_data: Dict, network: str) -> Dict:
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
        
        return {
            "address": pool_data.get("id"),
            "dex": "uniswap_v3",
            "network": network,
            "fee_tier": int(pool_data.get("feeTier", 0)),
            "tvl_usd": tvl_usd,
            "volume_24h": volume_usd,
            "liquidity": liquidity,
            "tick_spacing": int(pool_data.get("tickSpacing", 0)),
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
# ENDPOINT IMPLEMENTATION
# ============================================================================

@router.get("/dex/pools/{network}/{token0}/{token1}")
async def get_dex_pools(
    network: str,
    token0: str,
    token1: str,
    fee_tier: Optional[int] = Query(None, description="Filter by fee tier (500, 3000, 10000)")
):
    """
    Liste verfügbare Pools für ein Trading Pair auf einem bestimmten Network
    
    **FIXED VERSION** mit neuen The Graph URLs (Dezember 2024)
    """
    logger.info("=" * 80)
    logger.info("🔍 DEX POOLS REQUEST (FIXED VERSION)")
    logger.info("=" * 80)
    logger.info(f"📥 Parameters:")
    logger.info(f"  - network: {network}")
    logger.info(f"  - token0: {token0}")
    logger.info(f"  - token1: {token1}")
    logger.info(f"  - fee_tier: {fee_tier}")
    
    try:
        # Validiere Network
        valid_networks = ["ethereum", "polygon", "arbitrum", "optimism", "base"]
        logger.info(f"🔍 Validating network...")
        
        if network.lower() not in valid_networks:
            error_msg = f"Invalid network: {network}. Valid options: {valid_networks}"
            logger.error(f"❌ {error_msg}")
            raise HTTPException(status_code=422, detail=error_msg)
        logger.info(f"  ✅ Network valid: {network}")
        
        # Resolve Token Addresses
        logger.info("🔍 Resolving token addresses...")
        token0_address = resolve_token_address(network, token0)
        token1_address = resolve_token_address(network, token1)
        
        if not token0_address:
            error_msg = f"Unknown token: {token0} on {network}"
            logger.error(f"❌ {error_msg}")
            raise HTTPException(
                status_code=422,
                detail=f"{error_msg}. Supported tokens: {list(TOKEN_ADDRESSES.get(network, {}).keys())}"
            )
        
        if not token1_address:
            error_msg = f"Unknown token: {token1} on {network}"
            logger.error(f"❌ {error_msg}")
            raise HTTPException(
                status_code=422,
                detail=f"{error_msg}. Supported tokens: {list(TOKEN_ADDRESSES.get(network, {}).keys())}"
            )
        
        logger.info(f"  ✅ Token0 ({token0}): {token0_address}")
        logger.info(f"  ✅ Token1 ({token1}): {token1_address}")
        
        # Suche Pools im Subgraph
        logger.info("🔄 Querying Uniswap v3 Subgraph...")
        subgraph_url = get_subgraph_url(network)
        logger.info(f"  Subgraph URL: {subgraph_url[:60] if subgraph_url else 'None'}...")
        
        pools_raw = await search_pools_subgraph(
            network=network,
            token0_address=token0_address,
            token1_address=token1_address,
            fee_tier=fee_tier
        )
        
        if not pools_raw:
            logger.warning(f"⚠️ No pools found for {token0}/{token1} on {network}")
            return {
                "network": network,
                "pair": f"{token0}/{token1}",
                "pools": [],
                "timestamp": datetime.utcnow().isoformat(),
                "_note": "No pools found."
            }
        
        logger.info(f"📊 Found {len(pools_raw)} pools from Subgraph")
        
        # Formatiere Pools
        logger.info("🔄 Formatting pool data...")
        pools = []
        for pool_raw in pools_raw:
            formatted = format_pool_response(pool_raw, network)
            if formatted:
                pools.append(formatted)
        
        logger.info(f"  ✅ Formatted {len(pools)} pools")
        
        # Sortiere nach TVL
        pools.sort(key=lambda p: p.get("tvl_usd", 0), reverse=True)
        
        # Log top pool
        if pools:
            top_pool = pools[0]
            logger.info(f"  🏆 Top Pool:")
            logger.info(f"     Address: {top_pool['address']}")
            logger.info(f"     Fee: {top_pool['fee_tier']/10000}%")
            logger.info(f"     TVL: ${top_pool['tvl_usd']/1_000_000:.2f}M")
            logger.info(f"     Volume 24h: ${top_pool['volume_24h']/1_000_000:.2f}M")
            logger.info(f"     Price: ${top_pool['current_price']:.2f}")
        
        response = {
            "network": network,
            "pair": f"{token0}/{token1}",
            "pools": pools,
            "timestamp": datetime.utcnow().isoformat(),
            "_data_source": "uniswap_v3_subgraph",
            "_total_pools": len(pools)
        }
        
        logger.info("=" * 80)
        logger.info("✅ DEX POOLS - SUCCESS")
        logger.info(f"📤 Response: {len(pools)} pools for {token0}/{token1} on {network}")
        logger.info("=" * 80)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ DEX POOLS - ERROR")
        logger.error("=" * 80)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error("Traceback:", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/dex/liquidity/{pool_address}")
async def get_pool_liquidity(
    pool_address: str,
    bucket_size: float = Query(50.0, description="Price bucket size in USD"),
    range_multiplier: float = Query(2.0, description="Price range multiplier (2.0 = ±100%)")
):
    """Holt aktuelle Liquiditätsverteilung für einen spezifischen Pool"""
    logger.info("=" * 80)
    logger.info("💧 POOL LIQUIDITY REQUEST")
    logger.info("=" * 80)
    logger.info(f"📥 Parameters:")
    logger.info(f"  - pool_address: {pool_address}")
    logger.info(f"  - bucket_size: {bucket_size}")
    logger.info(f"  - range_multiplier: {range_multiplier}")
    
    global aggregator
    
    try:
        # Validiere Pool Address Format
        logger.info("🔍 Validating pool address format...")
        if not pool_address.startswith("0x") or len(pool_address) != 42:
            error_msg = "Invalid pool address format. Expected 0x + 40 hex chars"
            logger.error(f"❌ {error_msg}")
            raise HTTPException(status_code=422, detail=error_msg)
        logger.info(f"  ✅ Pool address format valid")
        
        # Hole Pool Info via Uniswap Integration
        logger.info("🔄 Initializing Uniswap v3 Exchange...")
        from app.core.orderbook_heatmap.exchanges.dex.uniswap_v3 import UniswapV3Exchange
        
        uniswap = UniswapV3Exchange()
        logger.info("  ✅ UniswapV3Exchange initialized")
        
        logger.info("📡 Fetching pool info from Subgraph...")
        pool_info = await uniswap.get_pool_info(pool_address)
        
        if not pool_info:
            error_msg = f"Pool not found: {pool_address}"
            logger.error(f"❌ {error_msg}")
            raise HTTPException(status_code=404, detail=error_msg)
        
        logger.info("  ✅ Pool info retrieved")
        logger.info(f"  - Token0: {pool_info.get('token0', {}).get('symbol', 'UNKNOWN')}")
        logger.info(f"  - Token1: {pool_info.get('token1', {}).get('symbol', 'UNKNOWN')}")
        
        logger.info("📊 Fetching liquidity ticks...")
        ticks = await uniswap.get_liquidity_ticks(pool_address)
        logger.info(f"  ✅ Retrieved {len(ticks)} ticks")
        
        # Berechne aktuellen Preis
        logger.info("💱 Calculating current price...")
        sqrt_price_x96 = int(pool_info.get("sqrtPrice", 0))
        current_price = uniswap._sqrt_price_to_price(sqrt_price_x96)
        logger.info(f"  ✅ Current price: ${current_price:.2f}")
        
        # Gruppiere Ticks zu Liquiditätsverteilung
        logger.info("🔄 Building liquidity distribution...")
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
                    "provider_count": 1,
                    "concentration_pct": 0.0
                })
        
        logger.info(f"  ✅ Built {len(liquidity_distribution)} liquidity ranges")
        
        # Berechne Concentration Metrics
        total_liquidity = sum(t.liquidity for t in ticks)
        
        def calc_concentration(tolerance_pct: float) -> float:
            lower = current_price * (1 - tolerance_pct / 100)
            upper = current_price * (1 + tolerance_pct / 100)
            concentrated = sum(t.liquidity for t in ticks if lower <= t.price_lower <= upper)
            return (concentrated / total_liquidity * 100) if total_liquidity > 0 else 0.0
        
        concentration_metrics = {
            "within_1_percent": calc_concentration(1.0),
            "within_2_percent": calc_concentration(2.0),
            "within_5_percent": calc_concentration(5.0)
        }
        
        response = {
            "pool_address": pool_address,
            "pair": f"{pool_info.get('token0', {}).get('symbol', 'TOKEN0')}/{pool_info.get('token1', {}).get('symbol', 'TOKEN1')}",
            "current_price": current_price,
            "current_tick": int(pool_info.get("tick", 0)),
            "total_liquidity": total_liquidity,
            "tvl_usd": total_liquidity * current_price,
            "liquidity_distribution": liquidity_distribution,
            "concentration_metrics": concentration_metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        logger.info("=" * 80)
        logger.info("✅ POOL LIQUIDITY - SUCCESS")
        logger.info("=" * 80)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ POOL LIQUIDITY - ERROR")
        logger.error("=" * 80)
        logger.error(f"Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dex/virtual-orderbook/{pool_address}")
async def get_virtual_orderbook(
    pool_address: str,
    depth: int = Query(100, ge=10, le=500, description="Number of price levels per side")
):
    """Generiert CEX-Style Orderbook aus DEX Liquiditätskurve"""
    logger.info("=" * 80)
    logger.info("📖 VIRTUAL ORDERBOOK REQUEST")
    logger.info("=" * 80)
    
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


class StartDEXHeatmapRequest(BaseModel):
    """Request Body für DEX Heatmap Start"""
    network: str = Field(..., description="ethereum, polygon, arbitrum, etc.")
    pools: List[Dict[str, Any]] = Field(..., description="Liste von Pools")
    bucket_size: float = Field(default=50.0)
    refresh_interval: int = Field(default=2000)


@router.post("/heatmap/start-dex")
async def start_dex_heatmap(data: StartDEXHeatmapRequest):
    """Startet DEX Orderbook Heatmap"""
    logger.info("🚀 START DEX HEATMAP REQUEST")
    
    global aggregator, ws_manager
    
    try:
        dex_pool_addresses = {
            f"{pool['dex']}_{i}": pool['address']
            for i, pool in enumerate(data.pools)
        }
        
        from app.core.orderbook_heatmap.models.heatmap import HeatmapConfig
        config = HeatmapConfig(
            price_bucket_size=data.bucket_size,
            time_window_seconds=int(data.refresh_interval / 1000),
            exchanges=list(dex_pool_addresses.keys())
        )
        
        if aggregator is None:
            from app.core.orderbook_heatmap.aggregator.orderbook_aggregator import OrderbookAggregator
            aggregator = OrderbookAggregator(config)
        
        from app.core.orderbook_heatmap.exchanges.dex.uniswap_v3 import UniswapV3Exchange
        for pool_name in dex_pool_addresses.keys():
            uniswap = UniswapV3Exchange()
            aggregator.add_exchange(uniswap)
        
        first_pool = data.pools[0]
        symbol = f"{first_pool.get('token0', 'TOKEN0')}/{first_pool.get('token1', 'TOKEN1')}"
        await aggregator.connect_all(symbol, dex_pool_addresses)
        
        import uuid
        session_id = str(uuid.uuid4())[:8]
        
        response = {
            "status": "started",
            "session_id": f"dex_heatmap_{session_id}",
            "pools_connected": len(data.pools),
            "websocket_url": f"ws://localhost:8000/ws/dex-heatmap/{session_id}"
        }
        
        logger.info("✅ START DEX HEATMAP - SUCCESS")
        return response
        
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dex/tvl-history/{pool_address}")
async def get_tvl_history(
    pool_address: str,
    start_time: int = Query(..., description="Unix timestamp"),
    end_time: int = Query(..., description="Unix timestamp"),
    interval: str = Query("1h", description="1m, 5m, 15m, 1h, 4h, 1d")
):
    """Historische TVL und Liquiditätsverteilung"""
    logger.info("📈 TVL HISTORY REQUEST")
    
    try:
        valid_intervals = ["1m", "5m", "15m", "1h", "4h", "1d"]
        if interval not in valid_intervals:
            raise HTTPException(status_code=422, detail=f"Invalid interval: {interval}")
        
        # TODO: Query Subgraph für historische Daten
        data = [{
            "timestamp": start_time,
            "tvl_usd": 285000000,
            "liquidity": 125000.5,
            "volume_usd": 450000000,
            "fees_usd": 1350000,
            "price": 2850.45,
            "tick": 201234,
            "concentration_1pct": 35.5,
            "lp_count": 1234
        }]
        
        response = {
            "pool_address": pool_address,
            "interval": interval,
            "data": data
        }
        
        logger.info("✅ TVL HISTORY - SUCCESS")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

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

# ============================================================================
# PRICE WEBSOCKET
# ============================================================================

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
