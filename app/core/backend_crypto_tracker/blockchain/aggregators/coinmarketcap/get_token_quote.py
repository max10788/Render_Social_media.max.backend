# blockchain/aggregators/coinmarketcap/get_token_quote.py
import requests
from typing import Dict, Any, Optional, List
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter
from ...data_models.market_metrics import TokenMarketData

# CoinMarketCap rate limiter (333 calls/day for free tier)
cmc_limiter = RateLimiter(max_calls=333, time_window=86400)

def get_token_quote(
    symbols: List[str],
    convert: str = "USD",
    api_key: Optional[str] = None
) -> List[TokenMarketData]:
    """
    Get latest quote for cryptocurrency symbols from CoinMarketCap.
    
    Args:
        symbols: List of cryptocurrency symbols (e.g., ['BTC', 'ETH'])
        convert: Target currency for conversion
        api_key: CoinMarketCap API key (required)
        
    Returns:
        List of TokenMarketData objects with current quotes
    """
    if not api_key:
        raise ValueError("CoinMarketCap API key is required")
    
    cmc_limiter.wait_if_needed()
    
    base_url = "https://pro-api.coinmarketcap.com/v1"
    endpoint = f"{base_url}/cryptocurrency/quotes/latest"
    
    params = {
        "symbol": ",".join(symbols),
        "convert": convert
    }
    
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
        
        results = []
        for symbol in symbols:
            if symbol in data.get("data", {}):
                token_data = data["data"][symbol]
                quote = token_data["quote"][convert]
                
                results.append(TokenMarketData(
                    token_id=str(token_data["id"]),
                    symbol=token_data["symbol"],
                    price=quote["price"],
                    market_cap=quote["market_cap"],
                    volume_24h=quote["volume_24h"],
                    price_change_24h=quote["percent_change_24h"],
                    currency=convert.lower(),
                    source="coinmarketcap",
                    rank=token_data.get("cmc_rank"),
                    circulating_supply=token_data.get("circulating_supply"),
                    total_supply=token_data.get("total_supply")
                ))
        
        return results
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinMarketCap")
