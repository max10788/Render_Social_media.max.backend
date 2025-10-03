# blockchain/blockchain_specific/ethereum/get_block_by_number.py
import requests
from typing import Dict, Any, Optional, Union
from web3 import Web3
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

eth_limiter = RateLimiter(max_calls=5, time_window=1)

def get_block_by_number(
    block_number: Union[int, str],
    full_transactions: bool = False,
    network: str = "mainnet",
    rpc_url: Optional[str] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get Ethereum block by number.
    
    Args:
        block_number: Block number or 'latest', 'pending', 'earliest'
        full_transactions: Include full transaction details
        network: Ethereum network
        rpc_url: Custom RPC URL
        api_key: API key for RPC provider
        
    Returns:
        Block information
    """
    eth_limiter.wait_if_needed()
    
    try:
        if not rpc_url:
            if network == "mainnet":
                rpc_url = f"https://mainnet.infura.io/v3/{api_key}" if api_key else "https://eth.public-rpc.com"
        
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        
        if not w3.is_connected():
            raise ConnectionError("Failed to connect to Ethereum node")
        
        # Get block
        if isinstance(block_number, str):
            block = w3.eth.get_block(block_number, full_transactions)
        else:
            block = w3.eth.get_block(block_number, full_transactions)
        
        # Convert to dict and handle non-serializable types
        block_dict = dict(block)
        
        return {
            "number": block_dict.get('number'),
            "hash": block_dict.get('hash').hex() if block_dict.get('hash') else None,
            "parentHash": block_dict.get('parentHash').hex() if block_dict.get('parentHash') else None,
            "timestamp": block_dict.get('timestamp'),
            "gasUsed": block_dict.get('gasUsed'),
            "gasLimit": block_dict.get('gasLimit'),
            "baseFeePerGas": block_dict.get('baseFeePerGas'),
            "difficulty": block_dict.get('difficulty'),
            "totalDifficulty": block_dict.get('totalDifficulty'),
            "size": block_dict.get('size'),
            "transactionsCount": len(block_dict.get('transactions', [])),
            "transactions": block_dict.get('transactions') if full_transactions else None
        }
        
    except Exception as e:
        return handle_api_error(e, "Ethereum Block")
