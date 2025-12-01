"""
FastAPI Endpoints für Orderbook Heatmap
"""
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List
import asyncio
import json
import logging

from app.core.orderbook_heatmap.models.orderbook import Exchange, AggregatedOrderbook
from app.core.orderbook_heatmap.models.heatmap import HeatmapConfig, HeatmapSnapshot, HeatmapTimeSeries
from app.core.orderbook_heatmap.exchanges.binance import BinanceExchange
from app.core.orderbook_heatmap.exchanges.bitget import BitgetExchange
from app.core.orderbook_heatmap.exchanges.kraken import KrakenExchange
from app.core.orderbook_heatmap.exchanges.dex.uniswap_v3 import UniswapV3Exchange
from app.core.orderbook_heatmap.aggregator import OrderbookAggregator
from app.core.orderbook_heatmap.websocket import WebSocketManager


logger = logging.getLogger(__name__)


# Router
router = APIRouter(prefix="/api/v1/orderbook-heatmap", tags=["Orderbook Heatmap"])


# Global Aggregator & WebSocket Manager
aggregator: Optional[OrderbookAggregator] = None
ws_manager = WebSocketManager()


@router.post("/start")
async def start_heatmap(
    symbol: str = Query(..., description="Trading Pair (z.B. BTC/USDT)"),
    exchanges: List[str] = Query(
        default=["binance", "bitget", "kraken"],
        description="Liste von Börsen"
    ),
    dex_pools: Optional[Dict[str, str]] = None,
    price_bucket_size: float = Query(default=10.0, description="Preis-Bucket-Größe"),
    time_window_seconds: int = Query(default=60, description="Zeitfenster in Sekunden")
):
    """
    Startet Live-Heatmap-Streaming
    
    Args:
        symbol: Trading Pair (z.B. "BTC/USDT")
        exchanges: Liste von Börsen-Namen
        dex_pools: Optional dict mit DEX Pool Adressen
            z.B. {"uniswap_v3": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"}
        price_bucket_size: Größe der Preis-Buckets
        time_window_seconds: Zeitfenster für Aggregation
    
    Returns:
        Status-Dict
    """
    global aggregator
    
    try:
        # Erstelle Config
        config = HeatmapConfig(
            price_bucket_size=price_bucket_size,
            time_window_seconds=time_window_seconds,
            exchanges=exchanges
        )
        
        # Erstelle neuen Aggregator
        aggregator = OrderbookAggregator(config)
        
        # Füge Börsen hinzu
        exchange_map = {
            "binance": BinanceExchange(),
            "bitget": BitgetExchange(),
            "kraken": KrakenExchange(),
            "uniswap_v3": UniswapV3Exchange(),
        }
        
        for exchange_name in exchanges:
            if exchange_name in exchange_map:
                aggregator.add_exchange(exchange_map[exchange_name])
            else:
                logger.warning(f"Unknown exchange: {exchange_name}")
        
        # Verbinde zu allen Börsen
        await aggregator.connect_all(symbol, dex_pools)
        
        # Setze Callback für WebSocket Broadcasts
        aggregator.add_update_callback(lambda: ws_manager.broadcast_update(aggregator))
        
        return {
            "status": "started",
            "symbol": symbol,
            "exchanges": exchanges,
            "config": {
                "price_bucket_size": price_bucket_size,
                "time_window_seconds": time_window_seconds
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to start heatmap: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop")
async def stop_heatmap():
    """Stoppt Live-Heatmap-Streaming"""
    global aggregator
    
    if not aggregator:
        raise HTTPException(status_code=400, detail="Heatmap not running")
    
    try:
        await aggregator.disconnect_all()
        aggregator = None
        
        return {"status": "stopped"}
        
    except Exception as e:
        logger.error(f"Failed to stop heatmap: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_status():
    """Holt aktuellen Status"""
    global aggregator
    
    if not aggregator:
        return {"status": "not_running"}
    
    return {
        "status": "running",
        **aggregator.get_status()
    }


@router.get("/snapshot/{symbol}")
async def get_snapshot(symbol: str):
    """
    Holt aktuellen Heatmap-Snapshot
    
    Args:
        symbol: Trading Pair
        
    Returns:
        HeatmapSnapshot
    """
    global aggregator
    
    if not aggregator:
        raise HTTPException(status_code=400, detail="Heatmap not running")
    
    snapshot = await aggregator.get_latest_heatmap(symbol)
    
    if not snapshot:
        raise HTTPException(status_code=404, detail="No snapshot available")
    
    # Konvertiere zu Matrix-Format
    exchanges = list(aggregator.exchanges.keys())
    matrix_data = snapshot.to_matrix(exchanges)
    
    return matrix_data


@router.get("/timeseries/{symbol}")
async def get_timeseries(symbol: str):
    """
    Holt Heatmap-TimeSeries
    
    Args:
        symbol: Trading Pair
        
    Returns:
        HeatmapTimeSeries als 3D-Matrix
    """
    global aggregator
    
    if not aggregator:
        raise HTTPException(status_code=400, detail="Heatmap not running")
    
    timeseries = await aggregator.get_heatmap_timeseries(symbol)
    
    if not timeseries:
        raise HTTPException(status_code=404, detail="No timeseries available")
    
    # Konvertiere zu 3D-Matrix
    exchanges = list(aggregator.exchanges.keys())
    matrix_data = timeseries.to_3d_matrix(exchanges)
    
    return matrix_data


@router.get("/orderbook/{symbol}")
async def get_orderbook(symbol: str):
    """
    Holt aggregiertes Orderbuch
    
    Args:
        symbol: Trading Pair
        
    Returns:
        AggregatedOrderbook
    """
    global aggregator
    
    if not aggregator:
        raise HTTPException(status_code=400, detail="Heatmap not running")
    
    orderbook = await aggregator.get_aggregated_orderbook(symbol)
    
    return orderbook.model_dump()


@router.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    """
    WebSocket Endpoint für Live-Updates
    
    Args:
        websocket: WebSocket connection
        symbol: Trading Pair
    """
    global aggregator
    
    await ws_manager.connect(websocket, symbol)
    
    try:
        while True:
            # Warte auf Nachrichten vom Client
            data = await websocket.receive_text()
            
            # Hier könnten Client-Commands verarbeitet werden
            # z.B. Subscribe/Unsubscribe zu bestimmten Exchanges
            
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
        logger.info(f"WebSocket disconnected for {symbol}")


@router.get("/exchanges")
async def get_available_exchanges():
    """
    Holt Liste verfügbarer Börsen
    
    Returns:
        Liste von Exchange-Informationen
    """
    return {
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


@router.get("/health")
async def health_check():
    """Health Check Endpoint"""
    return {"status": "healthy", "service": "orderbook-heatmap"}
