"""
Token Registry - Known ERC20 tokens with decimals and info
"""

# Most traded tokens on Ethereum
TOKEN_REGISTRY = {
    # Stablecoins
    '0xdac17f958d2ee523a2206206994597c13d831ec7': {
        'symbol': 'USDT',
        'name': 'Tether USD',
        'decimals': 6,
        'type': 'stablecoin',
        'usd_value': 1.0  # Fixed
    },
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': {
        'symbol': 'USDC',
        'name': 'USD Coin',
        'decimals': 6,
        'type': 'stablecoin',
        'usd_value': 1.0
    },
    '0x6b175474e89094c44da98b954eedeac495271d0f': {
        'symbol': 'DAI',
        'name': 'Dai Stablecoin',
        'decimals': 18,
        'type': 'stablecoin',
        'usd_value': 1.0
    },
    
    # Wrapped assets
    '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599': {
        'symbol': 'WBTC',
        'name': 'Wrapped Bitcoin',
        'decimals': 8,
        'type': 'wrapped',
        'price_feed': 'bitcoin'  # Link to CoinGecko
    },
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': {
        'symbol': 'WETH',
        'name': 'Wrapped Ether',
        'decimals': 18,
        'type': 'wrapped',
        'price_feed': 'ethereum'
    },
    
    # Add top 20 tokens here...
}

def get_token_info(address: str) -> dict:
    """Get token info by address."""
    return TOKEN_REGISTRY.get(address.lower(), {
        'symbol': 'UNKNOWN',
        'decimals': 18,  # Default
        'type': 'unknown'
    })
