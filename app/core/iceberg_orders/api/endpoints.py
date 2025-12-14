"""
FastAPI endpoints for iceberg order detection
UPDATED for improved detector and exchanges
"""
from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from typing import Optional, List
from datetime import datetime, timedelta
import asyncio
import logging

from app.core.iceberg_orders.exchanges.binance import BinanceExchangeImproved
from app.core.iceberg_orders.exchanges.coinbase import CoinbaseExchange
from app.core.iceberg_orders.exchanges.kraken import KrakenExchange
from app.core.iceberg_orders.detector.iceberg_detector import IcebergDetector 

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/iceberg-orders", tags=["iceberg-orders"])

# Exchange instances cache
exchanges = {}
detectors = {}


def get_exchange(exchange_name: str):
    """Get or create exchange instance"""
    if exchange_name not in exchanges:
        if exchange_name.lower() == 'binance':
            exchanges[exchange_name] = BinanceExchangeImproved()
        elif exchange_name.lower() == 'coinbase':
            exchanges[exchange_name] = CoinbaseExchange()
        elif exchange_name.lower() == 'kraken':
            exchanges[exchange_name] = KrakenExchange()
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported exchange: {exchange_name}")
    
    return exchanges[exchange_name]


def get_detector(threshold: float = 0.05) -> IcebergDetector:
    """
    Get or create detector instance
    
    UPDATED: Uses improved detector with larger lookback window
    """
    key = f"detector_{threshold}"
    if key not in detectors:
        # Improved detector with better parameters
        detectors[key] = IcebergDetector(
            threshold=threshold,
            lookback_window=200  # Increased from 100 for better analysis
        )
        logger.info(f"Created new detector with threshold={threshold}, lookback_window=200")
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
    
    IMPROVED:
    - Uses enhanced detector with dynamic tolerance
    - Better confidence scoring
    - More detailed metadata
    
    Returns:
        IcebergDetectionResult with detected icebergs and statistics
    """
    try:
        logger.info(f"Detection request: {exchange}/{symbol} threshold={threshold}")
        
        # Get exchange
        exchange_instance = get_exchange(exchange)
        
        # Fetch data
        logger.debug(f"Fetching orderbook for {symbol}")
        orderbook = await exchange_instance.fetch_orderbook(symbol, limit=100)
        
        logger.debug(f"Fetching trades for {symbol}")
        trades = await exchange_instance.fetch_trades(symbol, limit=100)
        
        # Log data quality
        logger.info(f"Data fetched - Orderbook: {len(orderbook.get('bids', []))} bids, "
                   f"{len(orderbook.get('asks', []))} asks, Trades: {len(trades)}")
        
        # Detect icebergs
        detector = get_detector(threshold)
        result = await detector.detect(
            orderbook=orderbook,
            trades=trades,
            exchange=exchange,
            symbol=symbol
        )
        
        # Log results
        logger.info(f"Detection complete - Found {result['statistics']['totalDetected']} icebergs, "
                   f"Avg confidence: {result['statistics']['averageConfidence']:.2%}")
        
        return JSONResponse(content=result)
        
    except Exception as e:
        logger.error(f"Detection error: {str(e)}", exc_info=True)
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
        
        logger.info(f"Historical request: {exchange}/{symbol} from {start} to {end}")
        
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
        logger.error(f"History error: {str(e)}")
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
    
    IMPROVED: Better spread calculation and volume analysis
    """
    try:
        logger.info(f"Depth analysis: {exchange}/{symbol} depth={depth}")
        
        exchange_instance = get_exchange(exchange)
        orderbook = await exchange_instance.fetch_orderbook(symbol, limit=depth)
        
        # Calculate metrics
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])
        
        # Extract volumes based on data structure
        if bids and isinstance(bids[0], dict):
            bid_volumes = [float(b.get('volume', 0)) for b in bids]
            ask_volumes = [float(a.get('volume', 0)) for a in asks]
            best_bid_price = float(bids[0].get('price', 0)) if bids else 0
            best_ask_price = float(asks[0].get('price', 0)) if asks else 0
        else:
            # List format [[price, volume], ...]
            bid_volumes = [float(vol) for _, vol in bids]
            ask_volumes = [float(vol) for _, vol in asks]
            best_bid_price = float(bids[0][0]) if bids else 0
            best_ask_price = float(asks[0][0]) if asks else 0
        
        # Calculate spread
        spread = best_ask_price - best_bid_price if best_bid_price and best_ask_price else 0
        spread_percent = (spread / best_bid_price * 100) if best_bid_price > 0 else 0
        
        analysis = {
            "bidSide": {
                "totalVolume": sum(bid_volumes),
                "avgOrderSize": sum(bid_volumes) / len(bid_volumes) if bid_volumes else 0,
                "largestOrder": max(bid_volumes) if bid_volumes else 0,
                "smallestOrder": min(bid_volumes) if bid_volumes else 0,
                "levels": len(bids),
                "bestPrice": best_bid_price
            },
            "askSide": {
                "totalVolume": sum(ask_volumes),
                "avgOrderSize": sum(ask_volumes) / len(ask_volumes) if ask_volumes else 0,
                "largestOrder": max(ask_volumes) if ask_volumes else 0,
                "smallestOrder": min(ask_volumes) if ask_volumes else 0,
                "levels": len(asks),
                "bestPrice": best_ask_price
            },
            "spread": spread,
            "spreadPercent": spread_percent,
            "imbalance": {
                "volumeRatio": sum(bid_volumes) / sum(ask_volumes) if sum(ask_volumes) > 0 else 0,
                "interpretation": "bullish" if sum(bid_volumes) > sum(ask_volumes) else "bearish"
            }
        }
        
        logger.info(f"Depth analysis complete - Spread: {spread_percent:.3f}%, "
                   f"Bid/Ask ratio: {analysis['imbalance']['volumeRatio']:.2f}")
        
        return JSONResponse(content=analysis)
        
    except Exception as e:
        logger.error(f"Depth analysis error: {str(e)}")
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
        logger.info(f"Stats request: {exchange} period={period}")
        
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
        logger.error(f"Stats error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detector-health")
async def detector_health():
    """
    NEW ENDPOINT: Monitor detector performance and health
    
    Returns detector status and recent performance metrics
    """
    try:
        detector_count = len(detectors)
        exchange_count = len(exchanges)
        
        # Get detector info
        detector_info = []
        for key, detector in detectors.items():
            detector_info.append({
                "key": key,
                "threshold": detector.threshold,
                "lookback_window": detector.lookback_window,
                "min_confidence": detector.min_confidence,
                "orderbook_history_size": len(detector.orderbook_history),
                "trade_history_size": len(detector.trade_history)
            })
        
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "detectors": {
                "count": detector_count,
                "instances": detector_info
            },
            "exchanges": {
                "count": exchange_count,
                "active": list(exchanges.keys())
            },
            "version": "improved_v2.0"
        }
        
        logger.info(f"Health check - Detectors: {detector_count}, Exchanges: {exchange_count}")
        
        return JSONResponse(content=health_status)
        
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "unhealthy",
                "error": str(e)
            }
        )


@router.get("/compare-detections")
async def compare_detection_methods(
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Trading symbol"),
    threshold: float = Query(0.05, description="Detection threshold")
):
    """
    NEW ENDPOINT: Compare different detection methods
    
    Useful for understanding which method is most effective
    """
    try:
        logger.info(f"Method comparison: {exchange}/{symbol}")
        
        exchange_instance = get_exchange(exchange)
        
        # Fetch data
        orderbook = await exchange_instance.fetch_orderbook(symbol, limit=100)
        trades = await exchange_instance.fetch_trades(symbol, limit=100)
        
        # Run detection
        detector = get_detector(threshold)
        result = await detector.detect(
            orderbook=orderbook,
            trades=trades,
            exchange=exchange,
            symbol=symbol
        )
        
        # Group by method
        by_method = {}
        for iceberg in result.get('icebergs', []):
            method = iceberg.get('detection_method', 'unknown')
            if method not in by_method:
                by_method[method] = []
            by_method[method].append(iceberg)
        
        # Calculate method stats
        method_stats = {}
        for method, icebergs in by_method.items():
            method_stats[method] = {
                "count": len(icebergs),
                "avg_confidence": sum(i['confidence'] for i in icebergs) / len(icebergs),
                "total_hidden_volume": sum(i.get('hidden_volume', 0) for i in icebergs),
                "buy_count": len([i for i in icebergs if i['side'] == 'buy']),
                "sell_count": len([i for i in icebergs if i['side'] == 'sell'])
            }
        
        comparison = {
            "overview": {
                "total_detections": len(result.get('icebergs', [])),
                "methods_used": len(by_method)
            },
            "by_method": method_stats,
            "detections": result.get('icebergs', [])
        }
        
        logger.info(f"Comparison complete - {len(by_method)} methods, "
                   f"{len(result.get('icebergs', []))} total detections")
        
        return JSONResponse(content=comparison)
        
    except Exception as e:
        logger.error(f"Comparison error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.subscriptions = {}
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected - Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected - Total: {len(self.active_connections)}")
    
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
                
                logger.info(f"WebSocket subscription: {exchange_name}/{symbol}")
                
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
                logger.info("WebSocket unsubscribe request")
                # Stop monitoring (implementation needed)
                pass
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("WebSocket disconnected by client")


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
        
        update_count = 0
        
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
            if result.get('icebergs'):
                update_count += 1
                logger.debug(f"WebSocket update #{update_count} - "
                           f"{len(result['icebergs'])} icebergs")
                await manager.send_personal_message(result, websocket)
            
            # Wait before next check
            await asyncio.sleep(5)  # Update every 5 seconds
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info(f"WebSocket monitoring stopped after {update_count} updates")
    except Exception as e:
        logger.error(f"Error in monitor task: {e}", exc_info=True)


@router.get("/exchanges/{exchange}/symbols")
async def get_exchange_symbols(exchange: str):
    """
    Get available trading symbols for an exchange
    """
    try:
        logger.info(f"Fetching symbols for {exchange}")
        
        exchange_instance = get_exchange(exchange)
        symbols = await exchange_instance.get_available_symbols()
        
        logger.info(f"Found {len(symbols)} symbols on {exchange}")
        
        return JSONResponse(content={
            "exchange": exchange,
            "symbols": symbols[:100],  # Limit to 100 symbols
            "total": len(symbols)
        })
        
    except Exception as e:
        logger.error(f"Symbols fetch error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/test-logging")
async def test_real_data_logging(
    duration: int = Query(60, description="Duration in seconds"),
    interval: int = Query(10, description="Interval in seconds")
):
    """Test logging endpoint - logs detections over time"""
    import asyncio
    
    results = []
    exchange_name = "binance"
    symbol = "BTC/USDT"
    threshold = 0.05
    
    start = datetime.now()
    
    for i in range(duration // interval):
        # Get exchange and detector
        exchange_instance = get_exchange(exchange_name)
        detector = get_detector(threshold)
        
        # Fetch and detect
        orderbook = await exchange_instance.fetch_orderbook(symbol)
        trades = await exchange_instance.fetch_trades(symbol)
        result = await detector.detect(orderbook, trades, exchange_name, symbol)
        
        # Store
        results.append({
            'iteration': i + 1,
            'timestamp': datetime.now().isoformat(),
            'total_detected': result['statistics']['totalDetected'],
            'avg_confidence': result['statistics']['averageConfidence']
        })
        
        await asyncio.sleep(interval)
    
    # Find persistent (appeared in >50% of snapshots)
    return JSONResponse(content={
        'duration_seconds': duration,
        'iterations': len(results),
        'results': results,
        'summary': {
            'avg_detections': sum(r['total_detected'] for r in results) / len(results),
            'avg_confidence': sum(r['avg_confidence'] for r in results) / len(results)
        }
    })
