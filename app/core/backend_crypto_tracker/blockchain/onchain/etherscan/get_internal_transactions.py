# blockchain/onchain/etherscan/get_internal_transactions.py
import requests
from typing import List, Dict, Any, Optional
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

etherscan_limiter = RateLimiter(max_calls=5, time_window=1)

def get_internal_transactions(
    address: str,
    tx_hash: Optional[str] = None,
    start_block: int = 0,
    end_block: int = 99999999,
    network: str = "mainnet",
    api_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get internal transactions from Etherscan.
    
    Args:
        address: Address to query (contract or wallet)
        tx_hash: Specific transaction hash (optional)
        start_block: Starting block number
        end_block: Ending block number
        network: Ethereum network
        api_key: Etherscan API key (required)
        
    Returns:
        List of internal transactions
    """
    if not api_key:
        raise ValueError("Etherscan API key is required")
    
    etherscan_limiter.wait_if_needed()
    
    base_urls = {
        "mainnet": "https://api.etherscan.io/api",
        "goerli": "https://api-goerli.etherscan.io/api",
        "sepolia": "https://api-sepolia.etherscan.io/api",
        "bsc": "https://api.bscscan.com/api",
        "polygon": "https://api.polygonscan.com/api"
    }
    
    base_url = base_urls.get(network, base_urls["mainnet"])
    
    params = {
        "module": "account",
        "apikey": api_key
    }
    
    if tx_hash:
        params["action"] = "txlistinternal"
        params["txhash"] = tx_hash
    else:
        params["action"] = "txlistinternal"
        params["address"] = address
        params["startblock"] = start_block
        params["endblock"] = end_block
        params["sort"] = "desc"
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data["status"] != "1":
            if "No transactions found" in str(data.get("message", "")):
                return []
            return []
        
        transactions = []
        for tx in data.get("result", []):
            transactions.append({
                "hash": tx.get("hash"),
                "from": tx.get("from"),
                "to": tx.get("to"),
                "value": int(tx.get("value", 0)) / 10**18,  # Convert to ETH
                "value_wei": tx.get("value"),
                "block_number": int(tx.get("blockNumber", 0)),
                "timestamp": tx.get("timeStamp"),
                "is_error": tx.get("isError") == "1",
                "error_code": tx.get("errCode") if tx.get("isError") == "1" else None,
                "gas": tx.get("gas"),
                "gas_used": tx.get("gasUsed"),
                "trace_id": tx.get("traceId"),
                "type": tx.get("type", "call")
            })
        
        return transactions
        
    except requests.RequestException as e:
        return handle_api_error(e, "Etherscan Internal Txs")
