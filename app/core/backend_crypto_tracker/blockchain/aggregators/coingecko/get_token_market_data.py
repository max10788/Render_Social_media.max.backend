# blockchain/aggregators/coingecko/get_token_market_data.py
import requests
from typing import Dict, Any, Optional
from datetime import datetime
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
    include_detailed: bool = False,
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
        include_detailed: Fetch detailed data (uses additional API call)
        api_key: Optional API key for higher rate limits
        
    Returns:
        TokenMarketData object with current market information
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    
    # If detailed data requested, use the coins/{id} endpoint
    if include_detailed:
        return _get_detailed_market_data(token_id, vs_currency, api_key)
    
    # Otherwise use the simple price endpoint
    endpoint = f"{base_url}/simple/price"
    
    params = {
        "ids": token_id,
        "vs_currencies": vs_currency,
        "include_market_cap": str(include_market_cap).lower(),
        "include_24hr_vol": str(include_24hr_vol).lower(),
        "include_24hr_change": str(include_24hr_change).lower(),
        "include_last_updated_at": "true"
    }
    
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if token_id not in data:
            raise ValueError(f"Token {token_id} not found in CoinGecko")
        
        token_data = data[token_id]
        
        # Get timestamp
        last_updated = None
        if "last_updated_at" in token_data:
            last_updated = datetime.fromtimestamp(token_data["last_updated_at"])
        
        return TokenMarketData(
            token_id=token_id,
            symbol=token_id.upper(),  # Will be updated with detailed info if needed
            price=token_data.get(vs_currency, 0),
            market_cap=token_data.get(f"{vs_currency}_market_cap", 0),
            volume_24h=token_data.get(f"{vs_currency}_24h_vol", 0),
            price_change_24h=token_data.get(f"{vs_currency}_24h_change", 0),
            currency=vs_currency,
            source="coingecko",
            last_updated=last_updated
        )
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinGecko")


def _get_detailed_market_data(
    token_id: str,
    vs_currency: str = "usd",
    api_key: Optional[str] = None
) -> TokenMarketData:
    """
    Fetch detailed market data including ATH/ATL, supply, and more.
    
    Args:
        token_id: CoinGecko token ID
        vs_currency: Target currency
        api_key: Optional API key
        
    Returns:
        TokenMarketData with comprehensive information
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/coins/{token_id}"
    
    params = {
        "localization": "false",
        "tickers": "false",
        "community_data": "false",
        "developer_data": "false"
    }
    
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Extract market data
        market_data = data.get("market_data", {})
        
        # Get price data
        current_price = market_data.get("current_price", {}).get(vs_currency, 0)
        market_cap = market_data.get("market_cap", {}).get(vs_currency, 0)
        volume_24h = market_data.get("total_volume", {}).get(vs_currency, 0)
        
        # Get price changes
        price_change_1h = market_data.get("price_change_percentage_1h_in_currency", {}).get(vs_currency)
        price_change_24h = market_data.get("price_change_percentage_24h_in_currency", {}).get(vs_currency)
        price_change_7d = market_data.get("price_change_percentage_7d_in_currency", {}).get(vs_currency)
        price_change_30d = market_data.get("price_change_percentage_30d_in_currency", {}).get(vs_currency)
        
        # Get ATH/ATL data
        ath = market_data.get("ath", {}).get(vs_currency)
        ath_date_str = market_data.get("ath_date", {}).get(vs_currency)
        ath_date = datetime.fromisoformat(ath_date_str.replace('Z', '+00:00')) if ath_date_str else None
        
        atl = market_data.get("atl", {}).get(vs_currency)
        atl_date_str = market_data.get("atl_date", {}).get(vs_currency)
        atl_date = datetime.fromisoformat(atl_date_str.replace('Z', '+00:00')) if atl_date_str else None
        
        # Get supply data
        circulating_supply = market_data.get("circulating_supply")
        total_supply = market_data.get("total_supply")
        max_supply = market_data.get("max_supply")
        
        # Get last updated
        last_updated_str = market_data.get("last_updated")
        last_updated = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00')) if last_updated_str else None
        
        return TokenMarketData(
            token_id=token_id,
            symbol=data.get("symbol", token_id).upper(),
            price=current_price,
            market_cap=market_cap,
            volume_24h=volume_24h,
            price_change_24h=price_change_24h or 0,
            currency=vs_currency,
            source="coingecko",
            price_change_1h=price_change_1h,
            price_change_7d=price_change_7d,
            price_change_30d=price_change_30d,
            market_cap_rank=data.get("market_cap_rank"),
            circulating_supply=circulating_supply,
            total_supply=total_supply,
            max_supply=max_supply,
            ath=ath,
            ath_date=ath_date,
            atl=atl,
            atl_date=atl_date,
            last_updated=last_updated
        )
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinGecko")


# Example usage and helper functions
def get_multiple_tokens_market_data(
    token_ids: list[str],
    vs_currency: str = "usd",
    api_key: Optional[str] = None
) -> Dict[str, TokenMarketData]:
    """
    Fetch market data for multiple tokens in a single call.
    
    Args:
        token_ids: List of CoinGecko token IDs
        vs_currency: Target currency
        api_key: Optional API key
        
    Returns:
        Dictionary mapping token_id to TokenMarketData
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/simple/price"
    
    params = {
        "ids": ",".join(token_ids),
        "vs_currencies": vs_currency,
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true"
    }
    
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        results = {}
        for token_id in token_ids:
            if token_id in data:
                token_data = data[token_id]
                
                last_updated = None
                if "last_updated_at" in token_data:
                    last_updated = datetime.fromtimestamp(token_data["last_updated_at"])
                
                results[token_id] = TokenMarketData(
                    token_id=token_id,
                    symbol=token_id.upper(),
                    price=token_data.get(vs_currency, 0),
                    market_cap=token_data.get(f"{vs_currency}_market_cap", 0),
                    volume_24h=token_data.get(f"{vs_currency}_24h_vol", 0),
                    price_change_24h=token_data.get(f"{vs_currency}_24h_change", 0),
                    currency=vs_currency,
                    source="coingecko",
                    last_updated=last_updated
                )
        
        return results
        
    except requests.RequestException as e:
        handle_api_error(e, "CoinGecko")
        return {}
