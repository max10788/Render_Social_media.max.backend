"""
Blockchain module for handling all blockchain-related operations.
"""

# Import all providers
from .providers.base_provider import BaseAPIProvider
from .providers.coingecko_provider import CoinGeckoProvider
from .providers.binance_provider import BinanceProvider
from .providers.cryptocompare_provider import CryptoCompareProvider
from .providers.kraken_provider import KrakenProvider
from .providers.bitquery_provider import BitqueryProvider
from .providers.coinmarketcap_provider import CoinMarketCapProvider
from .providers.bitget_provider import BitgetProvider
from .providers.coinbase_provider import CoinbaseProvider
from .providers.bitcoin_provider import BitcoinProvider
from .providers.ethereum_provider import EthereumProvider
from .providers.solana_provider import SolanaProvider
from .providers.sui_provider import SuiProvider

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
