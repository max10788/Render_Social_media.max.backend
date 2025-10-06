# blockchain/aggregators/coingecko/get_bitcoin_treasuries.py

import requests
from typing import List, Dict, Any, Optional
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

# CoinGecko allows querying public companies & governments’ crypto holdings per coin. :contentReference[oaicite:0]{index=0}
coingecko_limiter = RateLimiter(max_calls=50, time_window=60)

def get_bitcoin_treasuries(api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get a list of institutions holding Bitcoin in their treasury reserves via the CoinGecko API.

    This uses the endpoint:
        GET /companies/public_treasury/bitcoin
    which returns companies’ holdings of BTC. :contentReference[oaicite:1]{index=1}

    Args:
        api_key: Optional CoinGecko API key (for pro or higher-tier access)

    Returns:
        A list of dictionaries representing each institution’s BTC holdings and related metadata.
    """
    coingecko_limiter.wait_if_needed()

    base_url = "https://api.coingecko.com/api/v3"
    endpoint = f"{base_url}/companies/public_treasury/bitcoin"

    headers: Dict[str, str] = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key

    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()

        # According to docs, the JSON has fields:
        # {
        #   "total_holdings": float,
        #   "total_value_usd": float,
        #   "market_cap_dominance": float,
        #   "companies": [ { name, symbol, country, total_holdings, total_entry_value_usd, total_current_value_usd, percentage_of_total_supply }, ... ]
        # } :contentReference[oaicite:2]{index=2}
        companies = data.get("companies", [])

        formatted: List[Dict[str, Any]] = []
        for c in companies:
            formatted.append({
                "institution": c.get("name"),
                "symbol": c.get("symbol"),
                "country": c.get("country"),
                "total_bitcoin_held": c.get("total_holdings"),
                "total_entry_value_usd": c.get("total_entry_value_usd"),
                "total_current_value_usd": c.get("total_current_value_usd"),
                "percentage_of_supply": c.get("percentage_of_total_supply"),
            })

        return formatted

    except requests.RequestException as exc:
        # Let your generic error handler decide how to propagate / wrap
        return handle_api_error(exc, "CoinGecko Bitcoin Treasuries")
