# blockchain/aggregators/coingecko/get_token_historical_data.py
import requests
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from ...utils.error_handling import handle_api_error
from ...utils.time_utils import timestamp_to_datetime, get_time_range
from ...rate_limiters.rate_limiter import RateLimiter

# Initialize rate limiter for CoinGecko (50 calls/minute for free tier)
coingecko_limiter = RateLimiter(max_calls=50, time_window=60)


def get_token_historical_data(
    token_id: str,
    days: int = 30,
    vs_currency: str = "usd",
    interval: Optional[str] = None,
    api_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch historical price data for a token from CoinGecko.
    
    Args:
        token_id: CoinGecko token ID (e.g., 'bitcoin', 'ethereum')
        days: Number of days of data (1/7/14/30/90/180/365/max)
        vs_currency: Target currency for price data
        interval: Data interval (daily/hourly) - auto-selected if None
        api_key: Optional API key for higher rate limits
        
    Returns:
        List of historical price points with timestamps
        
    Example:
        >>> data = get_token_historical_data("bitcoin", days=7)
        >>> print(data[0])
        {
            'timestamp': 1634567890000,
            'datetime': datetime(2021, 10, 18, 12, 31, 30),
            'price': 62450.23,
            'currency': 'usd'
        }
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/coins/{token_id}/market_chart"
    
    params = {
        "vs_currency": vs_currency,
        "days": days
    }
    
    # Auto-select interval based on days if not specified
    if interval:
        params["interval"] = interval
    elif days <= 1:
        params["interval"] = "hourly"
    elif days <= 90:
        params["interval"] = "daily"
    
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Format the response
        formatted_data = []
        for price_point in data.get("prices", []):
            # CoinGecko returns timestamps in milliseconds
            timestamp_ms = price_point[0]
            formatted_data.append({
                "timestamp": timestamp_ms,
                "datetime": timestamp_to_datetime(timestamp_ms / 1000),
                "price": price_point[1],
                "currency": vs_currency
            })
        
        return formatted_data
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinGecko Historical")


def get_token_historical_data_range(
    token_id: str,
    start_date: datetime,
    end_date: datetime,
    vs_currency: str = "usd",
    api_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch historical price data for a specific date range.
    
    Args:
        token_id: CoinGecko token ID
        start_date: Start datetime
        end_date: End datetime
        vs_currency: Target currency
        api_key: Optional API key
        
    Returns:
        List of historical price points within the date range
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/coins/{token_id}/market_chart/range"
    
    params = {
        "vs_currency": vs_currency,
        "from": int(start_date.timestamp()),
        "to": int(end_date.timestamp())
    }
    
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        formatted_data = []
        for price_point in data.get("prices", []):
            timestamp_ms = price_point[0]
            formatted_data.append({
                "timestamp": timestamp_ms,
                "datetime": timestamp_to_datetime(timestamp_ms / 1000),
                "price": price_point[1],
                "currency": vs_currency
            })
        
        return formatted_data
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinGecko Historical Range")


def get_token_ohlc_data(
    token_id: str,
    days: int = 30,
    vs_currency: str = "usd",
    api_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch OHLC (Open, High, Low, Close) candlestick data.
    
    Args:
        token_id: CoinGecko token ID
        days: Number of days (1/7/14/30/90/180/365)
        vs_currency: Target currency
        api_key: Optional API key
        
    Returns:
        List of OHLC data points
        
    Example:
        >>> ohlc = get_token_ohlc_data("bitcoin", days=7)
        >>> print(ohlc[0])
        {
            'timestamp': 1634567890000,
            'datetime': datetime(2021, 10, 18, 0, 0, 0),
            'open': 62000.50,
            'high': 63500.00,
            'low': 61500.25,
            'close': 62450.23,
            'currency': 'usd'
        }
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/coins/{token_id}/ohlc"
    
    params = {
        "vs_currency": vs_currency,
        "days": days
    }
    
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # OHLC format: [timestamp, open, high, low, close]
        formatted_data = []
        for candle in data:
            formatted_data.append({
                "timestamp": candle[0],
                "datetime": timestamp_to_datetime(candle[0] / 1000),
                "open": candle[1],
                "high": candle[2],
                "low": candle[3],
                "close": candle[4],
                "currency": vs_currency
            })
        
        return formatted_data
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinGecko OHLC")


def get_price_at_timestamp(
    token_id: str,
    timestamp: datetime,
    vs_currency: str = "usd",
    api_key: Optional[str] = None
) -> Optional[float]:
    """
    Get token price at a specific timestamp.
    
    Args:
        token_id: CoinGecko token ID
        timestamp: Target datetime
        vs_currency: Target currency
        api_key: Optional API key
        
    Returns:
        Price at the specified timestamp, or None if not found
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/coins/{token_id}/history"
    
    # Format date as DD-MM-YYYY
    date_str = timestamp.strftime("%d-%m-%Y")
    
    params = {
        "date": date_str,
        "localization": "false"
    }
    
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        market_data = data.get("market_data", {})
        current_price = market_data.get("current_price", {})
        
        return current_price.get(vs_currency)
        
    except requests.RequestException as e:
        handle_api_error(e, "CoinGecko Historical Price")
        return None


def calculate_price_change(
    historical_data: List[Dict[str, Any]],
    period: str = "24h"
) -> Dict[str, float]:
    """
    Calculate price changes from historical data.
    
    Args:
        historical_data: List of price data points from get_token_historical_data()
        period: Time period for calculation (1h/24h/7d/30d)
        
    Returns:
        Dictionary with price change metrics
    """
    if not historical_data or len(historical_data) < 2:
        return {
            "price_change": 0.0,
            "price_change_percentage": 0.0,
            "start_price": 0.0,
            "end_price": 0.0
        }
    
    # Get most recent price
    end_price = historical_data[-1]["price"]
    
    # Determine how far back to look based on period
    now = datetime.now()
    start_time, _ = get_time_range(period, now)
    
    # Find the closest data point to the start time
    start_price = historical_data[0]["price"]
    for data_point in historical_data:
        if data_point["datetime"] >= start_time:
            start_price = data_point["price"]
            break
    
    # Calculate changes
    price_change = end_price - start_price
    price_change_percentage = (price_change / start_price * 100) if start_price > 0 else 0.0
    
    return {
        "price_change": price_change,
        "price_change_percentage": price_change_percentage,
        "start_price": start_price,
        "end_price": end_price,
        "period": period
    }


def get_price_statistics(
    historical_data: List[Dict[str, Any]]
) -> Dict[str, float]:
    """
    Calculate statistical metrics from historical price data.
    
    Args:
        historical_data: List of price data points
        
    Returns:
        Dictionary with statistical metrics (min, max, avg, volatility)
    """
    if not historical_data:
        return {
            "min_price": 0.0,
            "max_price": 0.0,
            "avg_price": 0.0,
            "volatility": 0.0
        }
    
    prices = [point["price"] for point in historical_data]
    
    min_price = min(prices)
    max_price = max(prices)
    avg_price = sum(prices) / len(prices)
    
    # Calculate simple volatility (standard deviation)
    variance = sum((p - avg_price) ** 2 for p in prices) / len(prices)
    volatility = variance ** 0.5
    
    return {
        "min_price": min_price,
        "max_price": max_price,
        "avg_price": avg_price,
        "volatility": volatility,
        "volatility_percentage": (volatility / avg_price * 100) if avg_price > 0 else 0.0
    }
