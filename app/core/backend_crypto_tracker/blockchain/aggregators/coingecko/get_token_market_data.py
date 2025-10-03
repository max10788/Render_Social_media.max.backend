# blockchain/aggregators/coingecko/get_token_market_data.py
import requests
from typing import Dict, Any, Optional
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter
from ...data_models.market_metrics import TokenMarketData

# Initialize rate limiter for CoinGecko (50 calls/minute for free tier)
coingecko_limiter = RateLimiter(max_calls=50, time_window=60)

def get_token_market_data(
    token_id: str,
    vs_currency: str = "usd",
    include_market_cap: bool = True,
    include_24hr_vol: bool = True,
    include_24hr_change: bool = True,
    api_key: Optional[str] = None
) -> TokenMarketData:
    """
    Fetch current market data for a token from CoinGecko.
    
    Args:
        token_id: CoinGecko token ID (e.g., 'bitcoin', 'ethereum')
        vs_currency: Target currency for price data
        include_market_cap: Include market cap data
        include_24hr_vol: Include 24h volume
        include_24hr_change: Include 24h price change
        api_key: Optional API key for higher rate limits
        
    Returns:
        TokenMarketData object with current market information
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/simple/price"
    
    params = {
        "ids": token_id,
        "vs_currencies": vs_currency,
        "include_market_cap": str(include_market_cap).lower(),
        "include_24hr_vol": str(include_24hr_vol).lower(),
        "include_24hr_change": str(include_24hr_change).lower()
    }
    
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if token_id not in data:
            raise ValueError(f"Token {token_id} not found")
        
        token_data = data[token_id]
        
        return TokenMarketData(
            token_id=token_id,
            symbol=token_id,  # Will be updated with detailed info if needed
            price=token_data.get(vs_currency, 0),
            market_cap=token_data.get(f"{vs_currency}_market_cap", 0),
            volume_24h=token_data.get(f"{vs_currency}_24h_vol", 0),
            price_change_24h=token_data.get(f"{vs_currency}_24h_change", 0),
            currency=vs_currency,
            source="coingecko"
        )
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinGecko")
