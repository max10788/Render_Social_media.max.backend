# blockchain/aggregators/cryptocompare/get_historical_price.py
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

cryptocompare_limiter = RateLimiter(max_calls=100, time_window=3600)

def get_historical_price(
    fsym: str,
    tsym: str = "USD",
    timestamp: Optional[int] = None,
    exchange: str = "CCCAGG",
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get historical price at specific timestamp from CryptoCompare.
    
    Args:
        fsym: From symbol (e.g., 'BTC')
        tsym: To symbol (e.g., 'USD')
        timestamp: Unix timestamp for historical price
        exchange: Exchange to get price from
        api_key: Optional API key for higher limits
        
    Returns:
        Historical price data at specified timestamp
    """
    cryptocompare_limiter.wait_if_needed()
    
    base_url = "https://min-api.cryptocompare.com/data"
    endpoint = f"{base_url}/pricehistorical"
    
    params = {
        "fsym": fsym,
        "tsyms": tsym,
        "e": exchange
    }
    
    if timestamp:
        params["ts"] = timestamp
    
    headers = {}
    if api_key:
        headers["authorization"] = f"Apikey {api_key}"
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if "Response" in data and data["Response"] == "Error":
            raise ValueError(f"CryptoCompare Error: {data.get('Message', 'Unknown error')}")
        
        return {
            "symbol": fsym,
            "currency": tsym,
            "price": data.get(fsym, {}).get(tsym, 0),
            "timestamp": timestamp,
            "exchange": exchange
        }
        
    except requests.RequestException as e:
        return handle_api_error(e, "CryptoCompare")
