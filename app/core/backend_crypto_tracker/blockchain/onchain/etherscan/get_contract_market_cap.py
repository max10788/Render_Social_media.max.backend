# blockchain/onchain/etherscan/get_contract_market_cap.py
import requests
from typing import Dict, Any, Optional
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

etherscan_limiter = RateLimiter(max_calls=5, time_window=1)

def get_contract_market_cap(
    contract_address: str,
    network: str = "mainnet",
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Calculate market cap for a token contract from Etherscan data.
    
    Args:
        contract_address: Token contract address
        network: Ethereum network
        api_key: Etherscan API key (required)
        
    Returns:
        Market cap and supply information
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
    
    # Get token supply
    params_supply = {
        "module": "stats",
        "action": "tokensupply",
        "contractaddress": contract_address,
        "apikey": api_key
    }
    
    try:
        response = requests.get(base_url, params=params_supply)
        response.raise_for_status()
        supply_data = response.json()
        
        if supply_data["status"] != "1":
            return {
                "contract": contract_address,
                "error": "Failed to get token supply",
                "message": supply_data.get("message")
            }
        
        total_supply_wei = int(supply_data["result"])
        
        # Get token info (decimals, symbol)
        params_info = {
            "module": "token",
            "action": "tokeninfo",
            "contractaddress": contract_address,
            "apikey": api_key
        }
        
        token_info = {}
        response_info = requests.get(base_url, params=params_info)
        if response_info.status_code == 200:
            info_data = response_info.json()
            if info_data.get("status") == "1" and info_data.get("result"):
                result_info = info_data["result"][0] if isinstance(info_data["result"], list) else info_data["result"]
                token_info = {
                    "symbol": result_info.get("symbol"),
                    "name": result_info.get("name"),
                    "decimals": int(result_info.get("decimals", 18)),
                    "price_usd": float(result_info.get("price", 0)) if result_info.get("price") else None
                }
        
        # Calculate circulating supply (you might want to exclude certain addresses)
        # For now, we'll use total supply as circulating supply
        decimals = token_info.get("decimals", 18)
        total_supply = total_supply_wei / (10 ** decimals)
        
        result = {
            "contract": contract_address,
            "total_supply": total_supply,
            "total_supply_wei": total_supply_wei,
            "decimals": decimals,
            "symbol": token_info.get("symbol"),
            "name": token_info.get("name")
        }
        
        # If we have price data, calculate market cap
        if token_info.get("price_usd"):
            result["price_usd"] = token_info["price_usd"]
            result["market_cap_usd"] = total_supply * token_info["price_usd"]
        
        return result
        
    except requests.RequestException as e:
        return handle_api_error(e, "Etherscan Market Cap")
