# blockchain/onchain/etherscan/get_contract_abi.py
import requests
import json
from typing import Dict, Any, Optional, List
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

# Etherscan rate limiter (5 calls/second for free tier)
etherscan_limiter = RateLimiter(max_calls=5, time_window=1)

def get_contract_abi(
    contract_address: str,
    network: str = "mainnet",
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get contract ABI from Etherscan.
    
    Args:
        contract_address: Contract address
        network: Ethereum network (mainnet/goerli/sepolia)
        api_key: Etherscan API key (required)
        
    Returns:
        Contract ABI and verification status
    """
    if not api_key:
        raise ValueError("Etherscan API key is required")
    
    etherscan_limiter.wait_if_needed()
    
    # Select base URL based on network
    base_urls = {
        "mainnet": "https://api.etherscan.io/api",
        "goerli": "https://api-goerli.etherscan.io/api",
        "sepolia": "https://api-sepolia.etherscan.io/api",
        "bsc": "https://api.bscscan.com/api",
        "polygon": "https://api.polygonscan.com/api"
    }
    
    base_url = base_urls.get(network, base_urls["mainnet"])
    
    params = {
        "module": "contract",
        "action": "getabi",
        "address": contract_address,
        "apikey": api_key
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data["status"] != "1":
            return {
                "address": contract_address,
                "verified": False,
                "abi": None,
                "message": data.get("result", "Contract source code not verified")
            }
        
        # Parse ABI
        abi = json.loads(data["result"])
        
        # Extract useful information from ABI
        functions = []
        events = []
        
        for item in abi:
            if item.get("type") == "function":
                functions.append({
                    "name": item.get("name"),
                    "inputs": item.get("inputs", []),
                    "outputs": item.get("outputs", []),
                    "stateMutability": item.get("stateMutability")
                })
            elif item.get("type") == "event":
                events.append({
                    "name": item.get("name"),
                    "inputs": item.get("inputs", [])
                })
        
        return {
            "address": contract_address,
            "verified": True,
            "abi": abi,
            "functions": functions,
            "events": events,
            "function_count": len(functions),
            "event_count": len(events)
        }
        
    except requests.RequestException as e:
        return handle_api_error(e, "Etherscan ABI")
