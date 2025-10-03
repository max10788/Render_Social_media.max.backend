# blockchain/aggregators/coinmarketcap/get_global_metrics.py
import requests
from typing import Dict, Any, Optional
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

cmc_limiter = RateLimiter(max_calls=333, time_window=86400)

def get_global_metrics(
    convert: str = "USD",
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get global cryptocurrency market metrics from CoinMarketCap.
    
    Args:
        convert: Target currency for conversion
        api_key: CoinMarketCap API key (required)
        
    Returns:
        Dictionary containing global market metrics
    """
    if not api_key:
        raise ValueError("CoinMarketCap API key is required")
    
    cmc_limiter.wait_if_needed()
    
    base_url = "https://pro-api.coinmarketcap.com/v1"
    endpoint = f"{base_url}/global-metrics/quotes/latest"
    
    params = {"convert": convert}
    
    headers = {
        "X-CMC_PRO_API_KEY": api_key,
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status", {}).get("error_code"):
            raise ValueError(f"CMC API Error: {data['status']['error_message']}")
        
        metrics = data["data"]
        quote = metrics["quote"][convert]
        
        return {
            "total_market_cap": quote["total_market_cap"],
            "total_volume_24h": quote["total_volume_24h"],
            "bitcoin_dominance": metrics["btc_dominance"],
            "ethereum_dominance": metrics["eth_dominance"],
            "active_cryptocurrencies": metrics["active_cryptocurrencies"],
            "active_exchanges": metrics["active_exchanges"],
            "last_updated": metrics["last_updated"]
        }
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinMarketCap Global")
