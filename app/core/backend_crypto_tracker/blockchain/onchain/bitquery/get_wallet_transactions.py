# blockchain/onchain/bitquery/get_wallet_transactions.py
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter
from app.core.backend_crypto_tracker.api.controllers.transaction_controller import WalletTransactionsRequest

# Bitquery rate limiter (10 requests per minute for free tier)
bitquery_limiter = RateLimiter(max_calls=10, time_window=60)

def get_wallet_transactions(
    wallet_address: str,
    network: str = "ethereum",
    limit: int = 100,
    offset: int = 0,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    api_key: Optional[str] = None
) -> List[WalletTransactionsRequest]:
    """
    Get wallet transactions from Bitquery.
    
    Args:
        wallet_address: Wallet address to query
        network: Blockchain network
        limit: Number of transactions to return
        offset: Pagination offset
        from_date: Start date for transactions
        to_date: End date for transactions
        api_key: Bitquery API key (required)
        
    Returns:
        List of wallet transactions
    """
    if not api_key:
        raise ValueError("Bitquery API key is required")
    
    bitquery_limiter.wait_if_needed()
    
    # GraphQL endpoint
    url = "https://graphql.bitquery.io"
    
    # Prepare date filters
    date_filter = ""
    if from_date:
        date_filter += f'since: "{from_date.isoformat()}"'
    if to_date:
        if date_filter:
            date_filter += ", "
        date_filter += f'till: "{to_date.isoformat()}"'
    
    # GraphQL query for transactions
    query = """
    {
      ethereum(network: %s) {
        transactions(
          any: [{txFrom: {is: "%s"}}, {txTo: {is: "%s"}}]
          options: {limit: %d, offset: %d, desc: "block.timestamp.iso8601"}
          %s
        ) {
          transaction {
            hash
            from {
              address
            }
            to {
              address
            }
            gas
            gasPrice
            gasValue
            value
          }
          block {
            timestamp {
              iso8601
            }
            height
          }
          success
        }
      }
    }
    """ % (
        network,
        wallet_address.lower(),
        wallet_address.lower(),
        limit,
        offset,
        f"date: {{{date_filter}}}" if date_filter else ""
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
        
        transactions = []
        for tx_data in data.get("data", {}).get("ethereum", {}).get("transactions", []):
            tx = tx_data["transaction"]
            block = tx_data["block"]
            
            transactions.append(WalletTransactionRequest(
                hash=tx["hash"],
                from_address=tx["from"]["address"],
                to_address=tx["to"]["address"] if tx["to"] else None,
                value=float(tx["value"]) if tx["value"] else 0,
                gas=tx["gas"],
                gas_price=tx["gasPrice"],
                timestamp=block["timestamp"]["iso8601"],
                block_number=block["height"],
                success=tx_data["success"],
                network=network
            ))
        
        return transactions
        
    except requests.RequestException as e:
        return handle_api_error(e, "Bitquery Transactions")
