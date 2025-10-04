# blockchain/onchain/bitquery/get_wallet_activity.py
import requests
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter
from ...data_models.wallet_activity import WalletActivity

bitquery_limiter = RateLimiter(max_calls=10, time_window=60)

def get_wallet_activity(
    wallet_address: str,
    network: str = "ethereum",
    time_range: int = 30,  # days
    api_key: Optional[str] = None
) -> WalletActivity:
    """
    Get comprehensive wallet activity analysis from Bitquery.
    
    Args:
        wallet_address: Wallet address to analyze
        network: Blockchain network
        time_range: Number of days to analyze
        api_key: Bitquery API key (required)
        
    Returns:
        WalletActivity object with comprehensive metrics
    """
    if not api_key:
        raise ValueError("Bitquery API key is required")
    
    bitquery_limiter.wait_if_needed()
    
    url = "https://graphql.bitquery.io"
    
    # Calculate date range
    to_date = datetime.now()
    from_date = to_date - timedelta(days=time_range)
    
    # GraphQL query for wallet activity
    query = """
    {
      ethereum(network: %s) {
        address(address: {is: "%s"}) {
          balance
          balances {
            currency {
              symbol
              address
              name
            }
            value
          }
          annotation
          smartContract {
            contractType
            currency {
              symbol
            }
          }
        }
        
        sent: transfers(
          sender: {is: "%s"}
          date: {since: "%s", till: "%s"}
        ) {
          count
          amount
          currency {
            symbol
          }
        }
        
        received: transfers(
          receiver: {is: "%s"}
          date: {since: "%s", till: "%s"}
        ) {
          count
          amount
          currency {
            symbol
          }
        }
        
        transactions(
          txSender: {is: "%s"}
          date: {since: "%s", till: "%s"}
        ) {
          count
          gasValue
        }
      }
    }
    """ % (
        network,
        wallet_address.lower(),
        wallet_address.lower(),
        from_date.isoformat(),
        to_date.isoformat(),
        wallet_address.lower(),
        from_date.isoformat(),
        to_date.isoformat(),
        wallet_address.lower(),
        from_date.isoformat(),
        to_date.isoformat()
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
        address_data = eth_data.get("address", {})
        
        # Calculate totals
        total_sent = sum(t.get("amount", 0) for t in eth_data.get("sent", []))
        total_received = sum(t.get("amount", 0) for t in eth_data.get("received", []))
        total_transactions = eth_data.get("transactions", [{}])[0].get("count", 0) if eth_data.get("transactions") else 0
        total_gas_spent = eth_data.get("transactions", [{}])[0].get("gasValue", 0) if eth_data.get("transactions") else 0
        
        # Token balances
        token_balances = {}
        for balance in address_data.get("balances", []):
            if balance["currency"]["symbol"]:
                token_balances[balance["currency"]["symbol"]] = {
                    "value": balance["value"],
                    "address": balance["currency"].get("address"),
                    "name": balance["currency"].get("name")
                }
        
        return WalletActivity(
            address=wallet_address,
            network=network,
            eth_balance=address_data.get("balance", 0),
            token_balances=token_balances,
            total_transactions=total_transactions,
            total_sent=total_sent,
            total_received=total_received,
            total_gas_spent=total_gas_spent,
            is_contract=bool(address_data.get("smartContract")),
            contract_type=address_data.get("smartContract", {}).get("contractType") if address_data.get("smartContract") else None,
            time_range_days=time_range,
            last_updated=datetime.now().isoformat()
        )
        
    except requests.RequestException as e:
        return handle_api_error(e, "Bitquery Activity")
