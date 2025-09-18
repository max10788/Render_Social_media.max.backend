"""
Blockchain module for handling all blockchain-related operations.
"""

# Import all providers
from base_provider import BaseAPIProvider
from coingecko_provider import CoinGeckoProvider
from binance_provider import BinanceProvider
from cryptocompare_provider import CryptoCompareProvider
from kraken_provider import KrakenProvider
from bitquery_provider import BitqueryProvider
from coinmarketcap_provider import CoinMarketCapProvider
from bitget_provider import BitgetProvider
from coinbase_provider import CoinbaseProvider
from bitcoin_provider import BitcoinProvider
from ethereum_provider import EthereumProvider
from solana_provider import SolanaProvider
from sui_provider import SuiProvider

# Import rate limiters
from .rate_limiters.rate_limiter import RateLimiter

# Import data models
from .data_models.token_price_data import TokenPriceData

# Make all important classes available at package level
__all__ = [
    # Base classes
    'BaseAPIProvider',
    'RateLimiter',
    'TokenPriceData',
    
    # Exchange providers
    'BinanceProvider',
    'KrakenProvider',
    'BitgetProvider',
    'CoinbaseProvider',
    
    # Data aggregators
    'CoinGeckoProvider',
    'CryptoCompareProvider',
    'CoinMarketCapProvider',
    
    # On-chain data providers
    'BitqueryProvider',
    
    # Blockchain-specific providers
    'BitcoinProvider',
    'EthereumProvider',
    'SolanaProvider',
    'SuiProvider',
]
