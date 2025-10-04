# blockchain/onchain/etherscan/get_token_balance.py
import requests
from typing import Dict, Any, Optional, List
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

etherscan_limiter = RateLimiter(max_calls=5, time_window=1)

def get_token_balance(
    wallet_address: str,
    token_address: Optional[str] = None,
    network: str = "mainnet",
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get token balance for a wallet from Etherscan.
    
    Args:
        wallet_address: Wallet address to check
        token_address: Specific token contract (None for ETH)
        network: Ethereum network
        api_key: Etherscan API key (required)
        
    Returns:
        Token balance information
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
    
    if token_address:
        # Get ERC20 token balance
        params = {
            "module": "account",
            "action": "tokenbalance",
            "contractaddress": token_address,
            "address": wallet_address,
            "tag": "latest",
            "apikey": api_key
        }
    else:
        # Get ETH balance
        params = {
            "module": "account",
            "action": "balance",
            "address": wallet_address,
            "tag": "latest",
            "apikey": api_key
        }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data["status"] != "1":
            return {
                "wallet": wallet_address,
                "balance": 0,
                "error": data.get("message", "Failed to get balance")
            }
        
        balance_wei = int(data["result"])
        balance_eth = balance_wei / 10**18  # Convert to ETH (or token units)
        
        result = {
            "wallet": wallet_address,
            "balance_wei": balance_wei,
            "balance": balance_eth,
            "token_address": token_address
        }
        
        # Get multiple token balances if no specific token
        if not token_address:
            result["type"] = "ETH"
            # Also get top token balances
            params_tokens = {
                "module": "account",
                "action": "tokentx",
                "address": wallet_address,
                "startblock": 0,
                "endblock": 99999999,
                "sort": "desc",
                "apikey": api_key
            }
            
            response_tokens = requests.get(base_url, params=params_tokens)
            if response_tokens.status_code == 200:
                tokens_data = response_tokens.json()
                if tokens_data["status"] == "1" and tokens_data.get("result"):
                    # Get unique tokens
                    unique_tokens = {}
                    for tx in tokens_data["result"][:100]:  # Check last 100 transactions
                        token_addr = tx.get("contractAddress")
                        if token_addr and token_addr not in unique_tokens:
                            unique_tokens[token_addr] = {
                                "symbol": tx.get("tokenSymbol"),
                                "name": tx.get("tokenName"),
                                "decimals": int(tx.get("tokenDecimal", 18))
                            }
                    result["tokens"] = list(unique_tokens.values())[:10]  # Top 10 tokens
        
        return result
        
    except requests.RequestException as e:
        return handle_api_error(e, "Etherscan Balance")
