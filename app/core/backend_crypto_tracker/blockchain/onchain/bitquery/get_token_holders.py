# blockchain/onchain/bitquery/get_token_holders.py
import requests
from typing import List, Dict, Any, Optional
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

bitquery_limiter = RateLimiter(max_calls=10, time_window=60)

def get_token_holders(
    token_address: str,
    network: str = "ethereum",
    limit: int = 100,
    min_balance: float = 0,
    api_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get top token holders from Bitquery.
    
    Args:
        token_address: Token contract address
        network: Blockchain network
        limit: Number of holders to return
        min_balance: Minimum balance to include
        api_key: Bitquery API key (required)
        
    Returns:
        List of token holders with balances
    """
    if not api_key:
        raise ValueError("Bitquery API key is required")
    
    bitquery_limiter.wait_if_needed()
    
    url = "https://graphql.bitquery.io"
    
    # GraphQL query for token holders
    query = """
    {
      ethereum(network: %s) {
        transfers(
          currency: {is: "%s"}
          amount: {gt: %f}
          options: {desc: "amount", limit: %d}
        ) {
          receiver {
            address
            annotation
          }
          amount
          count
        }
        
        tokenInfo: address(address: {is: "%s"}) {
          smartContract {
            currency {
              symbol
              name
              decimals
              totalSupply
            }
          }
        }
      }
    }
    """ % (
        network,
        token_address.lower(),
        min_balance,
        limit,
        token_address.lower()
    )
    
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json={"query": query}, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if "errors" in data:
            raise ValueError(f"Bitquery Error: {data['errors']}")
        
        eth_data = data.get("data", {}).get("ethereum", {})
        transfers = eth_data.get("transfers", [])
        token_info = eth_data.get("tokenInfo", {}).get("smartContract", {}).get("currency", {})
        
        # Aggregate holders
        holders_dict = {}
        for transfer in transfers:
            address = transfer["receiver"]["address"]
            if address not in holders_dict:
                holders_dict[address] = {
                    "address": address,
                    "balance": 0,
                    "transaction_count": 0,
                    "annotation": transfer["receiver"].get("annotation")
                }
            holders_dict[address]["balance"] += transfer["amount"]
            holders_dict[address]["transaction_count"] += transfer["count"]
        
        # Sort by balance and format
        holders = sorted(holders_dict.values(), key=lambda x: x["balance"], reverse=True)[:limit]
        
        # Calculate percentages if total supply available
        total_supply = token_info.get("totalSupply", 0)
        for holder in holders:
            if total_supply > 0:
                holder["percentage"] = (holder["balance"] / total_supply) * 100
            holder["token_symbol"] = token_info.get("symbol")
            holder["token_name"] = token_info.get("name")
        
        return holders
        
    except requests.RequestException as e:
        return handle_api_error(e, "Bitquery Holders")
