# app/core/backend_crypto_tracker/blockchain/blockchain_specific/ethereum/get_wallet_token_balances_and_prices.py
from typing import Dict, Any, List, Set

async def execute_get_wallet_token_balances_and_prices(
    wallet_address: str,
    chain: str,
    include_transactions: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    OPTIMIZED: Fetches both token balances and prices in a single call
    
    Args:
        wallet_address: The wallet address
        chain: Blockchain (ethereum, bsc)
        include_transactions: Transactions to identify relevant tokens
        
    Returns:
        Dict with 'balances', 'prices', and 'total_portfolio_value_usd'
    """
    # Import existing modules
    from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_wallet_token_balances import execute_get_wallet_token_balances
    from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_token_prices_bulk import execute_get_token_prices_bulk
    
    # 1. Get token balances
    token_balances = await execute_get_wallet_token_balances(wallet_address, chain=chain)
    
    # 2. Collect all unique token addresses
    token_addresses_set: Set[str] = set()
    
    # From balances
    for balance in token_balances:
        token_addr = balance.get('token_address', '').lower()
        if token_addr:
            token_addresses_set.add(token_addr)
    
    # From transaction token transfers (if provided)
    if include_transactions:
        for tx in include_transactions:
            for transfer in tx.get('token_transfers', []):
                token_addr = transfer.get('token_address', '').lower()
                if token_addr:
                    token_addresses_set.add(token_addr)
    
    # 3. Get prices for all unique tokens
    token_addresses_list = list(token_addresses_set)
    prices = {}
    total_portfolio_value_usd = 0.0
    
    if token_addresses_list:
        prices = await execute_get_token_prices_bulk(token_addresses_list, chain=chain)
        
        # Calculate total portfolio value
        for balance in token_balances:
            token_addr = balance.get('token_address', '').lower()
            token_amount = balance.get('balance', 0)
            token_price = prices.get(token_addr, 0)
            
            balance['value_usd'] = token_amount * token_price
            total_portfolio_value_usd += balance['value_usd']
    
    return {
        'balances': token_balances,
        'prices': prices,
        'total_portfolio_value_usd': total_portfolio_value_usd
    }
