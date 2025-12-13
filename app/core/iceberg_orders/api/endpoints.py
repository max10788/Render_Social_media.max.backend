"""
FastAPI endpoints for iceberg order detection
"""
from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from typing import Optional, List
from datetime import datetime, timedelta
import asyncio

from ..exchanges.binance import BinanceExchange
from ..exchanges.coinbase import CoinbaseExchange
from ..exchanges.kraken import KrakenExchange
from ..detector.iceberg_detector import IcebergDetector
from ..models.iceberg import IcebergDetectionResult

router = APIRouter(prefix="/api/iceberg-orders", tags=["iceberg-orders"])

# Exchange instances cache
exchanges = {}
detectors = {}


def get_exchange(exchange_name: str):
    """Get or create exchange instance"""
    if exchange_name not in exchanges:
        if exchange_name.lower() == 'binance':
            exchanges[exchange_name] = BinanceExchange()
        elif exchange_name.lower() == 'coinbase':
            exchanges[exchange_name] = CoinbaseExchange()
        elif exchange_name.lower() == 'kraken':
            exchanges[exchange_name] = KrakenExchange()
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported exchange: {exchange_name}")
    
    return exchanges[exchange_name]


def get_detector(threshold: float = 0.05) -> IcebergDetector:
    """Get or create detector instance"""
    key = f"detector_{threshold}"
    if key not in detectors:
        detectors[key] = IcebergDetector(threshold=threshold)
    return detectors[key]


@router.get("")
async def detect_iceberg_orders(
    exchange: str = Query(..., description="Exchange name (e.g., binance, coinbase)"),
    symbol: str = Query(..., description="Trading symbol (e.g., BTC/USDT)"),
    timeframe: str = Query("1h", description="Timeframe for analysis"),
    threshold: float = Query(0.05, ge=0.01, le=0.5, description="Detection threshold")
):
    """
    Detect iceberg orders for a specific symbol on an exchange
    
    Returns:
        IcebergDetectionResult with detected icebergs and statistics
    """
    try:
        # Get exchange
        exchange_instance = get_exchange(exchange)
        
        # Fetch data
        orderbook = await exchange_instance.fetch_orderbook(symbol, limit=100)
        trades = await exchange_instance.fetch_trades(symbol, limit=100)
        
        # Detect icebergs
        detector = get_detector(threshold)
        result = await detector.detect(
            orderbook=orderbook,
            trades=trades,
            exchange=exchange,
            symbol=symbol
        )
        
        return JSONResponse(content=result.to_dict())
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_historical_icebergs(
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Trading symbol"),
    start: str = Query(..., description="Start date (ISO format)"),
    end: str = Query(..., description="End date (ISO format)")
):
    """
    Get historical iceberg order detections
    
    Note: This requires a database implementation to store historical detections
    """
    try:
        # Parse dates
        start_date = datetime.fromisoformat(start.replace('Z', '+00:00'))
        end_date = datetime.fromisoformat(end.replace('Z', '+00:00'))
        
        # TODO: Implement database query for historical data
        # For now, return empty result
        
        return JSONResponse(content={
            "history": [],
            "metadata": {
                "exchange": exchange,
                "symbol": symbol,
                "start": start,
                "end": end,
                "dataPoints": 0
            }
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analyze-depth")
async def analyze_orderbook_depth(
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Trading symbol"),
    depth: int = Query(100, ge=10, le=500, description="Order book depth")
):
    """
    Analyze order book depth for iceberg patterns
    
    Returns detailed analysis of order book structure
    """
    try:
        exchange_instance = get_exchange(exchange)
        orderbook = await exchange_instance.fetch_orderbook(symbol, limit=depth)
        
        # Calculate metrics
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])
        
        bid_volumes = [float(vol) for _, vol in bids]
        ask_volumes = [float(vol) for _, vol in asks]
        
        analysis = {
            "bidSide": {
                "totalVolume": sum(bid_volumes),
                "avgOrderSize": sum(bid_volumes) / len(bid_volumes) if bid_volumes else 0,
                "largestOrder": max(bid_volumes) if bid_volumes else 0,
                "levels": len(bids)
            },
            "askSide": {
                "totalVolume": sum(ask_volumes),
                "avgOrderSize": sum(ask_volumes) / len(ask_volumes) if ask_volumes else 0,
                "largestOrder": max(ask_volumes) if ask_volumes else 0,
                "levels": len(asks)
            },
            "spread": float(asks[0][0]) - float(bids[0][0]) if bids and asks else 0,
            "spreadPercent": ((float(asks[0][0]) - float(bids[0][0])) / float(bids[0][0]) * 100) if bids and asks else 0
        }
        
        return JSONResponse(content=analysis)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_iceberg_statistics(
    exchange: str = Query(..., description="Exchange name"),
    period: str = Query("24h", description="Time period (e.g., 24h, 7d, 30d)")
):
    """
    Get iceberg detection statistics for a time period
    
    Note: Requires database implementation
    """
    try:
        # TODO: Implement database aggregation
        
        return JSONResponse(content={
            "totalDetections": 0,
            "byExchange": {},
            "bySymbol": {},
            "avgHiddenVolume": 0,
            "avgConfidence": 0,
            "period": period
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.subscriptions = {}
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def send_personal_message(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)


manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time iceberg order updates
    
    Client sends:
        {"action": "subscribe", "exchange": "binance", "symbol": "BTC/USDT", "threshold": 0.05}
    
    Server sends:
        Periodic updates with detected icebergs
    """
    await manager.connect(websocket)
    
    try:
        while True:
            # Receive subscription message
            data = await websocket.receive_json()
            
            if data.get('action') == 'subscribe':
                exchange_name = data.get('exchange')
                symbol = data.get('symbol')
                threshold = data.get('threshold', 0.05)
                
                # Start monitoring task
                asyncio.create_task(
                    monitor_and_send_updates(
                        websocket,
                        exchange_name,
                        symbol,
                        threshold
                    )
                )
            
            elif data.get('action') == 'unsubscribe':
                # Stop monitoring (implementation needed)
                pass
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)


async def monitor_and_send_updates(
    websocket: WebSocket,
    exchange_name: str,
    symbol: str,
    threshold: float
):
    """Monitor for iceberg orders and send updates"""
    try:
        exchange_instance = get_exchange(exchange_name)
        detector = get_detector(threshold)
        
        while True:
            # Fetch fresh data
            orderbook = await exchange_instance.fetch_orderbook(symbol)
            trades = await exchange_instance.fetch_trades(symbol)
            
            # Detect icebergs
            result = await detector.detect(
                orderbook=orderbook,
                trades=trades,
                exchange=exchange_name,
                symbol=symbol
            )
            
            # Send update if icebergs detected
            if result.icebergs:
                await manager.send_personal_message(result.to_dict(), websocket)
            
            # Wait before next check
            await asyncio.sleep(5)  # Update every 5 seconds
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"Error in monitor task: {e}")


@router.get("/exchanges/{exchange}/symbols")
async def get_exchange_symbols(exchange: str):
    """
    Get available trading symbols for an exchange
    """
    try:
        exchange_instance = get_exchange(exchange)
        symbols = await exchange_instance.get_available_symbols()
        
        return JSONResponse(content={
            "symbols": symbols[:100],  # Limit to 100 symbols
            "total": len(symbols)
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
