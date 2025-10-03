# blockchain/blockchain_specific/ethereum/get_transaction_by_hash.py
import requests
from typing import Dict, Any, Optional
from web3 import Web3
from ...utils.error_handling import handle_api_error
from ...rate_limiters.rate_limiter import RateLimiter

eth_limiter = RateLimiter(max_calls=5, time_window=1)

def get_transaction_by_hash(
    tx_hash: str,
    network: str = "mainnet",
    rpc_url: Optional[str] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get Ethereum transaction by hash.
    
    Args:
        tx_hash: Transaction hash
        network: Ethereum network
        rpc_url: Custom RPC URL
        api_key: API key for RPC provider
        
    Returns:
        Transaction details including receipt
    """
    eth_limiter.wait_if_needed()
    
    try:
        if not rpc_url:
            if network == "mainnet":
                rpc_url = f"https://mainnet.infura.io/v3/{api_key}" if api_key else "https://eth.public-rpc.com"
        
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        
        if not w3.is_connected():
            raise ConnectionError("Failed to connect to Ethereum node")
        
        # Get transaction
        tx = w3.eth.get_transaction(tx_hash)
        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
        
        return {
            "hash": tx_hash,
            "from": tx['from'],
            "to": tx['to'],
            "value": str(Web3.from_wei(tx['value'], 'ether')),
            "gas": tx['gas'],
            "gasPrice": str(Web3.from_wei(tx['gasPrice'], 'gwei')) if 'gasPrice' in tx else None,
            "nonce": tx['nonce'],
            "blockNumber": tx['blockNumber'],
            "blockHash": tx['blockHash'].hex() if tx['blockHash'] else None,
            "input": tx['input'],
            "status": tx_receipt['status'] if tx_receipt else None,
            "gasUsed": tx_receipt['gasUsed'] if tx_receipt else None,
            "effectiveGasPrice": str(Web3.from_wei(tx_receipt['effectiveGasPrice'], 'gwei')) if tx_receipt and 'effectiveGasPrice' in tx_receipt else None,
            "contractAddress": tx_receipt['contractAddress'] if tx_receipt else None
        }
        
    except Exception as e:
        return handle_api_error(e, "Ethereum Transaction")
