"""
Metrics und Helper-Funktionen

Utility-Funktionen für:
- Metriken-Berechnung
- Datenvalidierung
- Performance-Tracking
"""

import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from functools import wraps
from collections import defaultdict
import statistics


logger = logging.getLogger(__name__)


# ==================== PERFORMANCE METRICS ====================

def measure_time(func):
    """
    Decorator für Zeitmessung
    
    Usage:
        @measure_time
        async def my_function():
            pass
    """
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start = time.time()
        result = await func(*args, **kwargs)
        duration = (time.time() - start) * 1000  # Millisekunden
        logger.debug(f"{func.__name__} took {duration:.2f}ms")
        return result
    
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = (time.time() - start) * 1000
        logger.debug(f"{func.__name__} took {duration:.2f}ms")
        return result
    
    # Return appropriate wrapper based on function type
    import asyncio
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


# ==================== PRICE CALCULATIONS ====================

def calculate_price_change(
    start_price: float,
    end_price: float
) -> Dict[str, float]:
    """
    Berechnet Preisänderung
    
    Args:
        start_price: Start-Preis
        end_price: End-Preis
        
    Returns:
        Dictionary mit absoluter und prozentualer Änderung
    """
    if start_price == 0:
        return {"absolute": 0.0, "percentage": 0.0}
    
    absolute_change = end_price - start_price
    percentage_change = (absolute_change / start_price) * 100
    
    return {
        "absolute": round(absolute_change, 2),
        "percentage": round(percentage_change, 4)
    }


def calculate_volatility(prices: List[float]) -> float:
    """
    Berechnet Volatilität (Standardabweichung)
    
    Args:
        prices: Liste von Preisen
        
    Returns:
        Volatilität (Standardabweichung)
    """
    if len(prices) < 2:
        return 0.0
    
    try:
        volatility = statistics.stdev(prices)
        return round(volatility, 2)
    except Exception as e:
        logger.error(f"Error calculating volatility: {e}")
        return 0.0


def calculate_vwap(trades: List[Dict[str, Any]]) -> float:
    """
    Berechnet Volume-Weighted Average Price (VWAP)
    
    Args:
        trades: Liste von Trades mit 'price' und 'amount'
        
    Returns:
        VWAP
    """
    if not trades:
        return 0.0
    
    total_value = sum(
        trade.get("price", 0.0) * trade.get("amount", 0.0)
        for trade in trades
    )
    total_volume = sum(trade.get("amount", 0.0) for trade in trades)
    
    if total_volume == 0:
        return 0.0
    
    return round(total_value / total_volume, 2)


# ==================== VOLUME CALCULATIONS ====================

def calculate_volume_metrics(
    trades: List[Dict[str, Any]]
) -> Dict[str, float]:
    """
    Berechnet Volume-Metriken
    
    Args:
        trades: Liste von Trades
        
    Returns:
        Dictionary mit Volume-Metriken
    """
    if not trades:
        return {
            "total_volume": 0.0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "buy_sell_ratio": 0.0,
            "avg_trade_size": 0.0
        }
    
    buy_volume = sum(
        trade.get("amount", 0.0)
        for trade in trades
        if trade.get("trade_type") == "buy"
    )
    sell_volume = sum(
        trade.get("amount", 0.0)
        for trade in trades
        if trade.get("trade_type") == "sell"
    )
    total_volume = buy_volume + sell_volume
    
    buy_sell_ratio = buy_volume / sell_volume if sell_volume > 0 else 0.0
    avg_trade_size = total_volume / len(trades) if len(trades) > 0 else 0.0
    
    return {
        "total_volume": round(total_volume, 2),
        "buy_volume": round(buy_volume, 2),
        "sell_volume": round(sell_volume, 2),
        "buy_sell_ratio": round(buy_sell_ratio, 2),
        "avg_trade_size": round(avg_trade_size, 2)
    }


def calculate_trade_concentration(
    trades: List[Dict[str, Any]],
    time_window_seconds: int = 60
) -> float:
    """
    Berechnet zeitliche Konzentration von Trades
    
    Args:
        trades: Liste von Trades
        time_window_seconds: Zeitfenster in Sekunden
        
    Returns:
        Konzentrations-Score (0-1)
    """
    if len(trades) < 2:
        return 1.0
    
    timestamps = [
        trade.get("timestamp") for trade in trades
    ]
    
    # Konvertiere zu datetime wenn String
    if isinstance(timestamps[0], str):
        timestamps = [
            datetime.fromisoformat(ts.replace('Z', '+00:00'))
            for ts in timestamps
        ]
    
    # Sortiere Timestamps
    timestamps.sort()
    
    # Berechne Zeit-Diffs
    time_diffs = [
        (timestamps[i+1] - timestamps[i]).total_seconds()
        for i in range(len(timestamps) - 1)
    ]
    
    # Durchschnittliche Diff
    avg_diff = statistics.mean(time_diffs)
    
    # Score: Je kleiner der Durchschnitt, desto höher die Konzentration
    concentration = 1.0 / (1.0 + avg_diff / time_window_seconds)
    
    return round(concentration, 3)


# ==================== PATTERN DETECTION ====================

def detect_bot_pattern(trades: List[Dict[str, Any]]) -> bool:
    """
    Erkennt Bot-Trading-Pattern
    
    Kriterien:
    - Viele kleine Trades
    - Regelmäßige Zeitabstände
    - Ähnliche Trade-Größen
    
    Args:
        trades: Liste von Trades
        
    Returns:
        True wenn Bot-Pattern erkannt
    """
    if len(trades) < 10:
        return False
    
    # Prüfe Trade-Größen
    amounts = [trade.get("amount", 0.0) for trade in trades]
    avg_amount = statistics.mean(amounts)
    
    # Prüfe Varianz
    try:
        stdev = statistics.stdev(amounts)
        coefficient_of_variation = stdev / avg_amount if avg_amount > 0 else 0
        
        # Geringe Varianz = Bot-Pattern
        if coefficient_of_variation < 0.2:
            return True
    except:
        pass
    
    # Prüfe Zeitabstände
    timestamps = [trade.get("timestamp") for trade in trades]
    if isinstance(timestamps[0], str):
        timestamps = [
            datetime.fromisoformat(ts.replace('Z', '+00:00'))
            for ts in timestamps
        ]
    
    timestamps.sort()
    time_diffs = [
        (timestamps[i+1] - timestamps[i]).total_seconds()
        for i in range(len(timestamps) - 1)
    ]
    
    try:
        avg_diff = statistics.mean(time_diffs)
        stdev_diff = statistics.stdev(time_diffs)
        
        # Regelmäßige Zeitabstände = Bot-Pattern
        if stdev_diff / avg_diff < 0.3:
            return True
    except:
        pass
    
    return False


def detect_whale_pattern(trades: List[Dict[str, Any]]) -> bool:
    """
    Erkennt Whale-Trading-Pattern
    
    Kriterien:
    - Große Trades
    - Hoher Gesamt-Wert
    
    Args:
        trades: Liste von Trades
        
    Returns:
        True wenn Whale-Pattern erkannt
    """
    if not trades:
        return False
    
    # Durchschnittlicher Trade-Wert
    avg_value = sum(
        trade.get("value_usd", 0.0) for trade in trades
    ) / len(trades)
    
    # Whale Threshold: $100k+
    if avg_value > 100_000:
        return True
    
    # Oder ein einzelner sehr großer Trade
    max_value = max(trade.get("value_usd", 0.0) for trade in trades)
    if max_value > 500_000:
        return True
    
    return False


def detect_smart_money_pattern(trades: List[Dict[str, Any]]) -> bool:
    """
    Erkennt Smart Money Pattern
    
    Kriterien:
    - Mittelgroße Trades
    - Gutes Timing
    - Wenige, gezielte Trades
    
    Args:
        trades: Liste von Trades
        
    Returns:
        True wenn Smart Money Pattern erkannt
    """
    if not trades:
        return False
    
    # Durchschnittlicher Trade-Wert
    avg_value = sum(
        trade.get("value_usd", 0.0) for trade in trades
    ) / len(trades)
    
    # Smart Money Threshold: $50k - $100k
    if 50_000 <= avg_value <= 100_000:
        # Wenige, gezielte Trades
        if len(trades) <= 10:
            return True
    
    return False


# ==================== DATA VALIDATION ====================

def validate_trade_data(trade: Dict[str, Any]) -> bool:
    """
    Validiert Trade-Daten
    
    Args:
        trade: Trade Dictionary
        
    Returns:
        True wenn valid
    """
    required_fields = ["timestamp", "trade_type", "amount", "price"]
    
    # Prüfe ob alle Felder vorhanden
    for field in required_fields:
        if field not in trade:
            logger.warning(f"Missing field in trade: {field}")
            return False
    
    # Prüfe Datentypen
    try:
        amount = float(trade.get("amount", 0))
        price = float(trade.get("price", 0))
        
        if amount <= 0 or price <= 0:
            logger.warning(f"Invalid trade values: amount={amount}, price={price}")
            return False
        
        trade_type = trade.get("trade_type", "").lower()
        if trade_type not in ["buy", "sell"]:
            logger.warning(f"Invalid trade_type: {trade_type}")
            return False
        
        return True
    except (ValueError, TypeError) as e:
        logger.warning(f"Trade validation error: {e}")
        return False


def validate_candle_data(candle: Dict[str, Any]) -> bool:
    """
    Validiert Candle-Daten
    
    Args:
        candle: Candle Dictionary
        
    Returns:
        True wenn valid
    """
    required_fields = ["timestamp", "open", "high", "low", "close", "volume"]
    
    for field in required_fields:
        if field not in candle:
            logger.warning(f"Missing field in candle: {field}")
            return False
    
    try:
        open_price = float(candle.get("open", 0))
        high = float(candle.get("high", 0))
        low = float(candle.get("low", 0))
        close = float(candle.get("close", 0))
        volume = float(candle.get("volume", 0))
        
        # Validiere Logik
        if high < low:
            logger.warning(f"Invalid candle: high < low")
            return False
        
        if high < max(open_price, close):
            logger.warning(f"Invalid candle: high < max(open, close)")
            return False
        
        if low > min(open_price, close):
            logger.warning(f"Invalid candle: low > min(open, close)")
            return False
        
        if volume < 0:
            logger.warning(f"Invalid candle: volume < 0")
            return False
        
        return True
    except (ValueError, TypeError) as e:
        logger.warning(f"Candle validation error: {e}")
        return False


# ==================== TIME UTILITIES ====================

def get_candle_boundaries(
    timestamp: datetime,
    timeframe_minutes: int
) -> Tuple[datetime, datetime]:
    """
    Berechnet Candle-Grenzen für gegebenen Zeitpunkt
    
    Args:
        timestamp: Zeitpunkt
        timeframe_minutes: Timeframe in Minuten
        
    Returns:
        Tuple (start_time, end_time)
    """
    # Round down to nearest timeframe
    minutes_since_epoch = int(timestamp.timestamp() / 60)
    candle_start_minutes = (minutes_since_epoch // timeframe_minutes) * timeframe_minutes
    
    start_time = datetime.fromtimestamp(candle_start_minutes * 60)
    end_time = start_time + timedelta(minutes=timeframe_minutes)
    
    return start_time, end_time


def parse_timeframe_to_minutes(timeframe: str) -> int:
    """
    Konvertiert Timeframe String zu Minuten
    
    Args:
        timeframe: z.B. "5m", "1h", "1d"
        
    Returns:
        Minuten
    """
    timeframe_map = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "4h": 240,
        "1d": 1440
    }
    
    return timeframe_map.get(timeframe.lower(), 5)


# ==================== FORMATTING ====================

def format_usd(value: float) -> str:
    """Formatiert USD-Wert"""
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.2f}K"
    else:
        return f"${value:.2f}"


def format_volume(volume: float) -> str:
    """Formatiert Volume"""
    if volume >= 1_000:
        return f"{volume/1_000:.2f}K"
    else:
        return f"{volume:.2f}"


def format_percentage(value: float) -> str:
    """Formatiert Prozent-Wert"""
    return f"{value:+.2f}%"
