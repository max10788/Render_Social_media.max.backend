# blockchain/aggregators/coingecko/get_token_historical_data.py
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from ...utils.error_handling import handle_api_error
from ...utils.time_utils import timestamp_to_datetime
from ...rate_limiters.rate_limiter import RateLimiter

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
        token_id: CoinGecko token ID
        days: Number of days of data (1/7/14/30/90/180/365/max)
        vs_currency: Target currency
        interval: Data interval (daily/hourly)
        api_key: Optional API key
        
    Returns:
        List of historical price points with timestamps
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/coins/{token_id}/market_chart"
    
    params = {
        "vs_currency": vs_currency,
        "days": days
    }
    
    if interval:
        params["interval"] = interval
    
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
            formatted_data.append({
                "timestamp": price_point[0],
                "datetime": timestamp_to_datetime(price_point[0] / 1000),
                "price": price_point[1],
                "currency": vs_currency
            })
        
        return formatted_data
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinGecko Historical")
