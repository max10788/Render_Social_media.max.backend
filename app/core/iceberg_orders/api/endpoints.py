"""
FastAPI endpoints for iceberg order detection
UPDATED with JSON logging functionality
"""
from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from typing import Optional, List
from datetime import datetime, timedelta
from pathlib import Path
import asyncio
import logging
import json

from app.core.iceberg_orders.exchanges.binance import BinanceExchangeImproved
from app.core.iceberg_orders.exchanges.coinbase import CoinbaseExchange
from app.core.iceberg_orders.exchanges.kraken import KrakenExchange
from app.core.iceberg_orders.detector.iceberg_detector import IcebergDetector
from app.core.iceberg_orders.clustering.iceberg_clusterer import IcebergClusterer, AdaptiveClusterer

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/iceberg-orders", tags=["iceberg-orders"])

# Exchange instances cache
exchanges = {}
detectors = {}

# Clustering instances
clusterer = IcebergClusterer()
adaptive_clusterer = AdaptiveClusterer()

# JSON logging configuration
ICEBERG_LOG_DIR = Path("./logs/icebergs")
ICEBERG_LOG_DIR.mkdir(parents=True, exist_ok=True)


class IcebergLogger:
    """JSON logger for iceberg detections"""
    
    def __init__(self, log_dir: Path = ICEBERG_LOG_DIR):
        self.log_dir = log_dir
        self.current_date = None
        self.log_file = None
        
    def _get_log_filename(self, exchange: str, symbol: str) -> Path:
        """Generate log filename based on date, exchange and symbol"""
        date_str = datetime.now().strftime("%Y-%m-%d")
        safe_symbol = symbol.replace("/", "_")
        return self.log_dir / f"{date_str}_{exchange}_{safe_symbol}.jsonl"
    
    def log_detection(self, detection_result: dict, exchange: str, symbol: str):
        """
        Log detection result to JSON Lines file
        
        Format: One JSON object per line, allowing easy appending
        """
        try:
            log_file = self._get_log_filename(exchange, symbol)
            
            # Prepare log entry
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "exchange": exchange,
                "symbol": symbol,
                "detection_count": len(detection_result.get('icebergs', [])),
                "icebergs": detection_result.get('icebergs', []),
                "statistics": detection_result.get('statistics', {}),
                "metadata": detection_result.get('metadata', {})
            }
            
            # Append to file (JSONL format)
            with open(log_file, 'a', encoding='utf-8') as f:
                json.dump(log_entry, f, ensure_ascii=False)
                f.write('\n')
            
            logger.info(f"Logged {len(log_entry['icebergs'])} icebergs to {log_file.name}")
            
        except Exception as e:
            logger.error(f"Failed to log detection: {e}", exc_info=True)
    
    def log_single_iceberg(self, iceberg: dict, exchange: str, symbol: str, additional_info: dict = None):
        """Log a single iceberg detection with optional additional info"""
        try:
            log_file = self._get_log_filename(exchange, symbol)
            
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "exchange": exchange,
                "symbol": symbol,
                "iceberg": iceberg,
                **(additional_info or {})
            }
            
            with open(log_file, 'a', encoding='utf-8') as f:
                json.dump(log_entry, f, ensure_ascii=False)
                f.write('\n')
                
        except Exception as e:
            logger.error(f"Failed to log single iceberg: {e}")
    
    def get_daily_summary(self, exchange: str, symbol: str, date: str = None) -> dict:
        """Get summary of detections for a specific day"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        safe_symbol = symbol.replace("/", "_")
        log_file = self.log_dir / f"{date}_{exchange}_{safe_symbol}.jsonl"
        
        if not log_file.exists():
            return {
                "date": date,
                "exchange": exchange,
                "symbol": symbol,
                "total_detections": 0,
                "entries": []
            }
        
        entries = []
        total_icebergs = 0
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        entry = json.loads(line)
                        entries.append(entry)
                        total_icebergs += entry.get('detection_count', 0)
            
            return {
                "date": date,
                "exchange": exchange,
                "symbol": symbol,
                "total_entries": len(entries),
                "total_icebergs": total_icebergs,
                "entries": entries
            }
            
        except Exception as e:
            logger.error(f"Failed to read log file: {e}")
            return {
                "error": str(e),
                "date": date,
                "exchange": exchange,
                "symbol": symbol
            }
    
    def get_all_logs(self, exchange: str = None, symbol: str = None) -> List[dict]:
        """Get all log files, optionally filtered by exchange/symbol"""
        logs = []
        
        for log_file in self.log_dir.glob("*.jsonl"):
            # Parse filename: YYYY-MM-DD_exchange_symbol.jsonl
            parts = log_file.stem.split('_')
            if len(parts) >= 3:
                date = parts[0]
                file_exchange = parts[1]
                file_symbol = '_'.join(parts[2:])
                
                # Apply filters
                if exchange and file_exchange != exchange:
                    continue
                if symbol and file_symbol != symbol.replace("/", "_"):
                    continue
                
                logs.append({
                    "filename": log_file.name,
                    "date": date,
                    "exchange": file_exchange,
                    "symbol": file_symbol.replace("_", "/"),
                    "size_bytes": log_file.stat().st_size,
                    "path": str(log_file)
                })
        
        return sorted(logs, key=lambda x: x['date'], reverse=True)


# Global logger instance
iceberg_logger = IcebergLogger()


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
    """Get or create detector instance"""
    key = f"detector_{threshold}"
    if key not in detectors:
        detectors[key] = IcebergDetector(
            threshold=threshold,
            lookback_window=200
        )
        logger.info(f"Created new detector with threshold={threshold}, lookback_window=200")
    return detectors[key]


@router.get("")
async def detect_iceberg_orders(
    exchange: str = Query(..., description="Exchange name (e.g., binance, coinbase)"),
    symbol: str = Query(..., description="Trading symbol (e.g., BTC/USDT)"),
    timeframe: str = Query("1h", description="Timeframe for analysis"),
    threshold: float = Query(0.05, ge=0.01, le=0.5, description="Detection threshold"),
    log_results: bool = Query(True, description="Log results to JSON file"),
    enable_clustering: bool = Query(True, description="Enable parent order clustering"),
    adaptive_clustering: bool = Query(False, description="Use adaptive clustering based on market conditions")
):
    """
    Detect iceberg orders for a specific symbol on an exchange
    
    NEW: Now includes parent order clustering!
    - Individual detections are grouped into parent orders
    - Shows both individual icebergs and parent orders
    """
    try:
        logger.info(f"Detection request: {exchange}/{symbol} threshold={threshold}, clustering={enable_clustering}")
        
        # Get exchange
        exchange_instance = get_exchange(exchange)
        
        # Fetch data
        orderbook = await exchange_instance.fetch_orderbook(symbol, limit=100)
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
        
        # Apply clustering if enabled
        if enable_clustering and result.get('icebergs'):
            if adaptive_clustering:
                clustering_result = adaptive_clusterer.cluster_adaptive(
                    result['icebergs'],
                    orderbook
                )
            else:
                clustering_result = clusterer.cluster(result['icebergs'])
            
            # Add clustering data to result
            result['parent_orders'] = clustering_result['parent_orders']
            result['individual_icebergs'] = clustering_result['individual_icebergs']
            result['clustering_stats'] = clustering_result['clustering_stats']
            
            # Update statistics
            result['statistics']['parent_orders_found'] = len(clustering_result['parent_orders'])
            result['statistics']['clustering_rate'] = clustering_result['clustering_stats']['clustering_rate']
            
            logger.info(f"Clustering complete - Found {len(clustering_result['parent_orders'])} parent orders "
                       f"from {len(result['icebergs'])} individual detections")
        else:
            result['parent_orders'] = []
            result['individual_icebergs'] = result.get('icebergs', [])
            result['clustering_stats'] = {
                'clustering_enabled': False
            }
        
        # Log results
        logger.info(f"Detection complete - Found {result['statistics']['totalDetected']} icebergs, "
                   f"Avg confidence: {result['statistics']['averageConfidence']:.2%}")
        
        # JSON logging
        if log_results and result.get('icebergs'):
            iceberg_logger.log_detection(result, exchange, symbol)
        
        return JSONResponse(content=result)
        
    except Exception as e:
        logger.error(f"Detection error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs")
async def get_iceberg_logs(
    exchange: str = Query(None, description="Filter by exchange"),
    symbol: str = Query(None, description="Filter by symbol"),
    date: str = Query(None, description="Filter by date (YYYY-MM-DD)")
):
    """
    NEW ENDPOINT: Get list of all iceberg log files
    """
    try:
        if date and exchange and symbol:
            # Get specific day summary
            summary = iceberg_logger.get_daily_summary(exchange, symbol, date)
            return JSONResponse(content=summary)
        else:
            # Get list of all logs
            logs = iceberg_logger.get_all_logs(exchange, symbol)
            return JSONResponse(content={
                "total_logs": len(logs),
                "logs": logs
            })
    
    except Exception as e:
        logger.error(f"Failed to fetch logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs/read")
async def read_log_file(
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Trading symbol"),
    date: str = Query(None, description="Date (YYYY-MM-DD), defaults to today"),
    limit: int = Query(100, ge=1, le=1000, description="Max entries to return")
):
    """
    NEW ENDPOINT: Read entries from a specific log file
    """
    try:
        summary = iceberg_logger.get_daily_summary(exchange, symbol, date)
        
        # Limit entries if requested
        if 'entries' in summary and len(summary['entries']) > limit:
            summary['entries'] = summary['entries'][-limit:]  # Most recent entries
            summary['note'] = f"Showing last {limit} of {summary['total_entries']} entries"
        
        return JSONResponse(content=summary)
    
    except Exception as e:
        logger.error(f"Failed to read log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs/stats")
async def get_log_statistics(
    exchange: str = Query(None, description="Filter by exchange"),
    symbol: str = Query(None, description="Filter by symbol"),
    days: int = Query(7, ge=1, le=30, description="Number of days to analyze")
):
    """
    NEW ENDPOINT: Get statistics from logged detections
    """
    try:
        logs = iceberg_logger.get_all_logs(exchange, symbol)
        
        # Filter by date range
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        recent_logs = [log for log in logs if log['date'] >= cutoff_date]
        
        # Aggregate stats
        total_files = len(recent_logs)
        total_size = sum(log['size_bytes'] for log in recent_logs)
        
        # Group by exchange and symbol
        by_exchange = {}
        by_symbol = {}
        
        for log in recent_logs:
            ex = log['exchange']
            sym = log['symbol']
            
            by_exchange[ex] = by_exchange.get(ex, 0) + 1
            by_symbol[sym] = by_symbol.get(sym, 0) + 1
        
        return JSONResponse(content={
            "period_days": days,
            "total_log_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "by_exchange": by_exchange,
            "by_symbol": by_symbol,
            "recent_logs": recent_logs[:10]  # Most recent 10
        })
    
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/logs/export")
async def export_logs_to_json(
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Trading symbol"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)")
):
    """
    NEW ENDPOINT: Export logs for a date range as a single JSON file
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_entries = []
        current = start
        
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            summary = iceberg_logger.get_daily_summary(exchange, symbol, date_str)
            
            if 'entries' in summary:
                all_entries.extend(summary['entries'])
            
            current += timedelta(days=1)
        
        # Create export file
        export_filename = f"export_{exchange}_{symbol.replace('/', '_')}_{start_date}_to_{end_date}.json"
        export_path = ICEBERG_LOG_DIR / export_filename
        
        export_data = {
            "export_info": {
                "exchange": exchange,
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "total_entries": len(all_entries),
                "exported_at": datetime.now().isoformat()
            },
            "entries": all_entries
        }
        
        with open(export_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported {len(all_entries)} entries to {export_filename}")
        
        return JSONResponse(content={
            "success": True,
            "filename": export_filename,
            "path": str(export_path),
            "total_entries": len(all_entries)
        })
    
    except Exception as e:
        logger.error(f"Export failed: {e}")
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
    
    UPDATED: Now reads from JSON logs
    """
    try:
        start_date = datetime.fromisoformat(start.replace('Z', '+00:00'))
        end_date = datetime.fromisoformat(end.replace('Z', '+00:00'))
        
        logger.info(f"Historical request: {exchange}/{symbol} from {start} to {end}")
        
        # Read from logs
        all_entries = []
        current = start_date
        
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            summary = iceberg_logger.get_daily_summary(exchange, symbol, date_str)
            
            if 'entries' in summary:
                all_entries.extend(summary['entries'])
            
            current += timedelta(days=1)
        
        return JSONResponse(content={
            "history": all_entries,
            "metadata": {
                "exchange": exchange,
                "symbol": symbol,
                "start": start,
                "end": end,
                "dataPoints": len(all_entries)
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
    """Analyze order book depth for iceberg patterns"""
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
            bid_volumes = [float(vol) for _, vol in bids]
            ask_volumes = [float(vol) for _, vol in asks]
            best_bid_price = float(bids[0][0]) if bids else 0
            best_ask_price = float(asks[0][0]) if asks else 0
        
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
    
    UPDATED: Now reads from JSON logs
    """
    try:
        logger.info(f"Stats request: {exchange} period={period}")
        
        # Parse period
        if period.endswith('h'):
            hours = int(period[:-1])
            days = max(1, hours // 24)
        elif period.endswith('d'):
            days = int(period[:-1])
        else:
            days = 1
        
        # Get stats from logs
        stats_response = await get_log_statistics(exchange=exchange, days=days)
        
        return stats_response
        
    except Exception as e:
        logger.error(f"Stats error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/system-limits")
async def check_system_limits(
    exchange: str = Query(..., description="Exchange name")
):
    """
    NEW ENDPOINT: Check system limits and potential data gaps
    
    Shows:
    - Exchange API limits (orderbook/trades fetch limits)
    - Detector history limits
    - Potential data gaps that could affect parent order detection
    """
    try:
        logger.info(f"Checking system limits for {exchange}")
        
        exchange_instance = get_exchange(exchange)
        detector = get_detector(0.05)  # Get default detector
        
        # Exchange limits
        exchange_limits = {
            "orderbook": {
                "default_limit": 100,
                "max_limit": 5000 if exchange.lower() == 'binance' else 100,
                "note": "Number of price levels fetched from orderbook"
            },
            "trades": {
                "default_limit": 100,
                "max_limit": 1000 if exchange.lower() == 'binance' else 100,
                "note": "Number of recent trades fetched"
            }
        }
        
        # Detector limits
        detector_limits = {
            "orderbook_history": {
                "max_snapshots": detector.lookback_window,
                "current_size": len(detector.orderbook_history),
                "note": "Older snapshots are automatically removed"
            },
            "trade_history": {
                "max_trades": detector.lookback_window * 3,
                "current_size": len(detector.trade_history),
                "note": "Older trades are automatically removed"
            }
        }
        
        # Calculate potential gaps
        gaps_analysis = {
            "orderbook_gap": {
                "description": "If a parent order has more price levels than fetch limit",
                "risk": "medium",
                "mitigation": "Use max_limit parameter and increase fetch size"
            },
            "trades_gap": {
                "description": "If a parent order has more refills than trade history",
                "risk": "high",
                "example": "A parent order with 500 refills but only 100 trades fetched",
                "mitigation": "Fetch more trades (up to 1000) or use WebSocket for continuous data"
            },
            "time_gap": {
                "description": "If parent order spans longer than history retention",
                "risk": "medium",
                "example": "Order active for 20 minutes but only last 10 minutes in history",
                "mitigation": "Increase lookback_window or use persistent storage"
            }
        }
        
        # Recommendations
        recommendations = [
            {
                "issue": "Limited trade history",
                "solution": "Increase trades fetch limit to 1000 for better parent order detection",
                "implementation": "fetch_trades(symbol, limit=1000)"
            },
            {
                "issue": "Missing historical data",
                "solution": "Use WebSocket for continuous monitoring instead of polling",
                "implementation": "subscribe_trades() and subscribe_orderbook()"
            },
            {
                "issue": "Large parent orders split over time",
                "solution": "Increase lookback_window to 500 or use database persistence",
                "implementation": "IcebergDetector(lookback_window=500)"
            },
            {
                "issue": "Clustering misses dispersed refills",
                "solution": "Increase time_window in clustering parameters",
                "implementation": "IcebergClusterer(time_window_seconds=600)"
            }
        ]
        
        limits_info = {
            "exchange": exchange,
            "exchange_limits": exchange_limits,
            "detector_limits": detector_limits,
            "potential_gaps": gaps_analysis,
            "recommendations": recommendations,
            "current_configuration": {
                "orderbook_fetch_limit": 100,
                "trades_fetch_limit": 100,
                "detector_lookback_window": detector.lookback_window,
                "clustering_time_window": 300
            }
        }
        
        logger.info(f"System limits check complete for {exchange}")
        
        return JSONResponse(content=limits_info)
        
    except Exception as e:
        logger.error(f"System limits check error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detector-health")
async def detector_health():
    """Monitor detector performance and health"""
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
        
        # Get logging stats
        all_logs = iceberg_logger.get_all_logs()
        total_log_size = sum(log['size_bytes'] for log in all_logs)
        
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
            "logging": {
                "total_files": len(all_logs),
                "total_size_mb": round(total_log_size / (1024 * 1024), 2),
                "log_directory": str(ICEBERG_LOG_DIR)
            },
            "version": "improved_v2.1_with_logging"
        }
        
        logger.info(f"Health check - Detectors: {detector_count}, Exchanges: {exchange_count}, "
                   f"Log files: {len(all_logs)}")
        
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
    threshold: float = Query(0.05, description="Detection threshold"),
    log_results: bool = Query(False, description="Log comparison results")
):
    """Compare different detection methods"""
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
        
        # Optional logging
        if log_results:
            iceberg_logger.log_detection(result, exchange, symbol)
        
        logger.info(f"Comparison complete - {len(by_method)} methods, "
                   f"{len(result.get('icebergs', []))} total detections")
        
        return JSONResponse(content=comparison)
        
    except Exception as e:
        logger.error(f"Comparison error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cluster-analysis")
async def analyze_clustering(
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Trading symbol"),
    threshold: float = Query(0.05, description="Detection threshold"),
    time_window: int = Query(300, ge=60, le=1800, description="Clustering time window (seconds)"),
    price_tolerance: float = Query(0.1, ge=0.01, le=1.0, description="Price tolerance (%)"),
    min_refills: int = Query(3, ge=2, le=10, description="Minimum refills for parent order")
):
    """
    NEW ENDPOINT: Detailed clustering analysis
    
    Shows how individual icebergs are grouped into parent orders
    with configurable clustering parameters
    """
    try:
        logger.info(f"Clustering analysis: {exchange}/{symbol}")
        
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
        
        if not result.get('icebergs'):
            return JSONResponse(content={
                "message": "No icebergs detected",
                "parent_orders": [],
                "individual_icebergs": [],
                "clustering_stats": {}
            })
        
        # Create custom clusterer with specified parameters
        custom_clusterer = IcebergClusterer(
            time_window_seconds=time_window,
            price_tolerance_percent=price_tolerance,
            volume_tolerance_percent=50,
            min_refills=min_refills,
            min_consistency_score=0.5
        )
        
        # Cluster
        clustering_result = custom_clusterer.cluster(result['icebergs'])
        
        # Get summary
        from app.core.iceberg_orders.clustering.iceberg_clusterer import ParentIcebergOrder
        parent_order_objs = []
        for po_dict in clustering_result['parent_orders']:
            # Already in dict format, use directly
            parent_order_objs.append(po_dict)
        
        analysis = {
            "detection_summary": {
                "total_icebergs_detected": len(result['icebergs']),
                "detection_methods": result['statistics'].get('detectionMethods', {})
            },
            "clustering_parameters": {
                "time_window_seconds": time_window,
                "price_tolerance_percent": price_tolerance,
                "min_refills": min_refills
            },
            "parent_orders": clustering_result['parent_orders'],
            "individual_icebergs": clustering_result['individual_icebergs'],
            "clustering_stats": clustering_result['clustering_stats'],
            "insights": self._generate_clustering_insights(clustering_result)
        }
        
        logger.info(f"Clustering analysis complete - {len(clustering_result['parent_orders'])} parent orders")
        
        return JSONResponse(content=analysis)
        
    except Exception as e:
        logger.error(f"Clustering analysis error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def _generate_clustering_insights(clustering_result: Dict) -> Dict:
    """Generate insights from clustering results"""
    parent_orders = clustering_result['parent_orders']
    stats = clustering_result['clustering_stats']
    
    if not parent_orders:
        return {
            "message": "No parent orders found - icebergs are too dispersed or inconsistent"
        }
    
    insights = {
        "clustering_effectiveness": "high" if stats['clustering_rate'] > 70 else "medium" if stats['clustering_rate'] > 40 else "low",
        "clustering_rate_percent": stats['clustering_rate'],
        "average_refills_per_parent": stats['avg_refills_per_parent'],
        "interpretation": []
    }
    
    # Generate interpretations
    if stats['clustering_rate'] > 70:
        insights["interpretation"].append(
            "High clustering rate suggests coordinated trading activity - likely large institutional orders"
        )
    
    if stats['avg_refills_per_parent'] > 5:
        insights["interpretation"].append(
            f"Average of {stats['avg_refills_per_parent']:.1f} refills per parent order indicates systematic order splitting"
        )
    
    # Analyze largest parent order
    largest_po = max(parent_orders, key=lambda x: x['volume']['total'])
    insights["largest_parent_order"] = {
        "id": largest_po['id'],
        "side": largest_po['side'],
        "total_volume": largest_po['volume']['total'],
        "refill_count": largest_po['refills']['count'],
        "duration_minutes": largest_po['timing']['duration_minutes'],
        "interpretation": f"Largest {largest_po['side']} order with {largest_po['refills']['count']} refills over {largest_po['timing']['duration_minutes']:.1f} minutes"
    }
    
    return insights


@router.get("/compare-detections")
async def compare_detection_methods(
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Trading symbol"),
    threshold: float = Query(0.05, description="Detection threshold"),
    log_results: bool = Query(False, description="Log comparison results")
):
    """Compare different detection methods"""
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
        
        # Optional logging
        if log_results:
            iceberg_logger.log_detection(result, exchange, symbol)
        
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
    
    UPDATED: Now also logs detections via WebSocket
    """
    await manager.connect(websocket)
    
    try:
        while True:
            data = await websocket.receive_json()
            
            if data.get('action') == 'subscribe':
                exchange_name = data.get('exchange')
                symbol = data.get('symbol')
                threshold = data.get('threshold', 0.05)
                enable_logging = data.get('enable_logging', True)
                
                logger.info(f"WebSocket subscription: {exchange_name}/{symbol} (logging={enable_logging})")
                
                asyncio.create_task(
                    monitor_and_send_updates(
                        websocket,
                        exchange_name,
                        symbol,
                        threshold,
                        enable_logging
                    )
                )
            
            elif data.get('action') == 'unsubscribe':
                logger.info("WebSocket unsubscribe request")
                pass
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("WebSocket disconnected by client")


async def monitor_and_send_updates(
    websocket: WebSocket,
    exchange_name: str,
    symbol: str,
    threshold: float,
    enable_logging: bool = True
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
                
                # Log to JSON if enabled
                if enable_logging:
                    iceberg_logger.log_detection(result, exchange_name, symbol)
                
                await manager.send_personal_message(result, websocket)
            
            # Wait before next check
            await asyncio.sleep(5)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info(f"WebSocket monitoring stopped after {update_count} updates")
    except Exception as e:
        logger.error(f"Error in monitor task: {e}", exc_info=True)


@router.get("/exchanges/{exchange}/symbols")
async def get_exchange_symbols(exchange: str):
    """Get available trading symbols for an exchange"""
    try:
        logger.info(f"Fetching symbols for {exchange}")
        
        exchange_instance = get_exchange(exchange)
        symbols = await exchange_instance.get_available_symbols()
        
        logger.info(f"Found {len(symbols)} symbols on {exchange}")
        
        return JSONResponse(content={
            "exchange": exchange,
            "symbols": symbols[:100],
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
    """
    Test logging endpoint - logs detections over time
    
    UPDATED: Now uses the IcebergLogger class
    """
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
        
        # Log to JSON
        if result.get('icebergs'):
            iceberg_logger.log_detection(result, exchange_name, symbol)
        
        # Store summary
        results.append({
            'iteration': i + 1,
            'timestamp': datetime.now().isoformat(),
            'total_detected': result['statistics']['totalDetected'],
            'avg_confidence': result['statistics']['averageConfidence']
        })
        
        await asyncio.sleep(interval)
    
    return JSONResponse(content={
        'duration_seconds': duration,
        'iterations': len(results),
        'results': results,
        'summary': {
            'avg_detections': sum(r['total_detected'] for r in results) / len(results),
            'avg_confidence': sum(r['avg_confidence'] for r in results) / len(results)
        },
        'logged_to': str(iceberg_logger._get_log_filename(exchange_name, symbol))
    })
