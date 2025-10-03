# blockchain/aggregators/__init__.py
"""
Data aggregators module for fetching market data from various sources.
"""
from .coingecko import (
    get_token_market_data,
    get_token_historical_data,
    get_supported_chains
)
from .coinmarketcap import (
    get_token_quote,
    get_global_metrics
)
from .cryptocompare import get_historical_price

__all__ = [
    'get_token_market_data',
    'get_token_historical_data', 
    'get_supported_chains',
    'get_token_quote',
    'get_global_metrics',
    'get_historical_price'
]
