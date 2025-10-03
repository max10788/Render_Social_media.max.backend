# blockchain/blockchain_specific/ethereum/get_gas_price.py
import requests
from typing import Dict, Any, Optional
from web3 import Web3
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

eth_limiter = RateLimiter(max_calls=5, time_window=1)

def get_gas_price(
    network: str = "mainnet",
    rpc_url: Optional[str] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get current Ethereum gas prices.
    
    Args:
        network: Ethereum network (mainnet/goerli/sepolia)
        rpc_url: Custom RPC URL
        api_key: API key for RPC provider
        
    Returns:
        Current gas prices in Gwei
    """
    eth_limiter.wait_if_needed()
    
    try:
        # Default RPC URLs
        if not rpc_url:
            if network == "mainnet":
                rpc_url = f"https://mainnet.infura.io/v3/{api_key}" if api_key else "https://eth.public-rpc.com"
            elif network == "goerli":
                rpc_url = f"https://goerli.infura.io/v3/{api_key}" if api_key else "https://goerli.public-rpc.com"
        
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        
        if not w3.is_connected():
            raise ConnectionError("Failed to connect to Ethereum node")
        
        # Get gas price
        gas_price = w3.eth.gas_price
        
        # Get base fee and priority fee for EIP-1559
        latest_block = w3.eth.get_block('latest')
        base_fee = latest_block.get('baseFeePerGas', 0)
        
        # Estimate priority fees
        max_priority_fee = w3.eth.max_priority_fee
        
        return {
            "standard": {
                "gasPrice": Web3.from_wei(gas_price, 'gwei'),
                "baseFee": Web3.from_wei(base_fee, 'gwei'),
                "priorityFee": Web3.from_wei(max_priority_fee, 'gwei')
            },
            "fast": {
                "gasPrice": Web3.from_wei(int(gas_price * 1.2), 'gwei'),
                "baseFee": Web3.from_wei(base_fee, 'gwei'),
                "priorityFee": Web3.from_wei(int(max_priority_fee * 1.5), 'gwei')
            },
            "slow": {
                "gasPrice": Web3.from_wei(int(gas_price * 0.8), 'gwei'),
                "baseFee": Web3.from_wei(base_fee, 'gwei'),
                "priorityFee": Web3.from_wei(int(max_priority_fee * 0.8), 'gwei')
            },
            "blockNumber": latest_block['number'],
            "timestamp": latest_block['timestamp']
        }
        
    except Exception as e:
        return handle_api_error(e, "Ethereum Gas")
