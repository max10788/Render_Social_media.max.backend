# blockchain/onchain/etherscan/get_contract_creation.py
import requests
from typing import Dict, Any, Optional
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

etherscan_limiter = RateLimiter(max_calls=5, time_window=1)

def get_contract_creation(
    contract_address: str,
    network: str = "mainnet",
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get contract creation transaction and creator from Etherscan.
    
    Args:
        contract_address: Contract address
        network: Ethereum network
        api_key: Etherscan API key (required)
        
    Returns:
        Contract creation details
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
        "module": "contract",
        "action": "getcontractcreation",
        "contractaddresses": contract_address,
        "apikey": api_key
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data["status"] != "1" or not data.get("result"):
            return {
                "address": contract_address,
                "found": False,
                "message": "Contract creation details not found"
            }
        
        creation_data = data["result"][0]
        
        return {
            "address": contract_address,
            "found": True,
            "creator_address": creation_data.get("contractCreator"),
            "creation_tx_hash": creation_data.get("txHash"),
            "deployed_at_block": creation_data.get("blockNumber")
        }
        
    except requests.RequestException as e:
        return handle_api_error(e, "Etherscan Creation")
