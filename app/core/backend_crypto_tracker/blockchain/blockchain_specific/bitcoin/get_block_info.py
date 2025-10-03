# blockchain/blockchain_specific/bitcoin/get_block_info.py
import requests
from typing import Dict, Any, Optional
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

bitcoin_limiter = RateLimiter(max_calls=10, time_window=60)

def get_block_info(
    block_hash: Optional[str] = None,
    block_height: Optional[int] = None,
    network: str = "main",
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get Bitcoin block information by hash or height.
    
    Args:
        block_hash: Block hash to query
        block_height: Block height to query
        network: Bitcoin network (main/test)
        api_key: Optional API key
        
    Returns:
        Block information including transactions
    """
    if not block_hash and block_height is None:
        raise ValueError("Either block_hash or block_height must be provided")
    
    bitcoin_limiter.wait_if_needed()
    
    # Using BlockCypher API as example
    base_url = f"https://api.blockcypher.com/v1/btc/{network}"
    
    if block_hash:
        endpoint = f"{base_url}/blocks/{block_hash}"
    else:
        endpoint = f"{base_url}/blocks/{block_height}"
    
    params = {}
    if api_key:
        params["token"] = api_key
    
    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        
        return {
            "hash": data.get("hash"),
            "height": data.get("height"),
            "time": data.get("time"),
            "n_tx": data.get("n_tx"),
            "size": data.get("size"),
            "fee": data.get("fees"),
            "merkle_root": data.get("mrkl_root"),
            "prev_block": data.get("prev_block"),
            "next_blocks": data.get("next_blocks", [])
        }
        
    except requests.RequestException as e:
        return handle_api_error(e, "Bitcoin Block")
