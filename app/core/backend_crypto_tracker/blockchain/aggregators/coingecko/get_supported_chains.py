# blockchain/aggregators/coingecko/get_supported_chains.py
import requests
from typing import List, Dict, Any, Optional
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

coingecko_limiter = RateLimiter(max_calls=50, time_window=60)

def get_supported_chains(api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get list of blockchain networks supported by CoinGecko.
    
    Args:
        api_key: Optional API key for higher rate limits
        
    Returns:
        List of supported blockchain platforms with details
    """
    coingecko_limiter.wait_if_needed()
    
    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/asset_platforms"
    
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    
    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        platforms = response.json()
        
        # Filter and format the response
        formatted_chains = []
        for platform in platforms:
            if platform.get("chain_identifier"):
                formatted_chains.append({
                    "id": platform.get("id"),
                    "chain_id": platform.get("chain_identifier"),
                    "name": platform.get("name"),
                    "shortname": platform.get("shortname"),
                    "native_coin_id": platform.get("native_coin_id")
                })
        
        return formatted_chains
        
    except requests.RequestException as e:
        return handle_api_error(e, "CoinGecko Chains")
