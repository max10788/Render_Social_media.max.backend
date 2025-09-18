"""
Blockchain module for handling all blockchain-related operations.
"""

# Import all providers from subdirectories
from .aggregators.coingecko_provider import CoinGeckoProvider
from .aggregators.coinmarketcap_provider import CoinMarketCapProvider
from .aggregators.cryptocompare_provider import CryptoCompareProvider

from .blockchain_specific.bitcoin_provider import BitcoinProvider
from .blockchain_specific.ethereum_provider import EthereumProvider
from .blockchain_specific.solana_provider import SolanaProvider
from .blockchain_specific.sui_provider import SuiProvider

from .exchanges.base_provider import BaseAPIProvider
from .exchanges.binance_provider import BinanceProvider
from .exchanges.bitget_provider import BitgetProvider
from .exchanges.coinbase_provider import CoinbaseProvider
from .exchanges.kraken_provider import KrakenProvider

from .onchain.bitquery_provider import BitqueryProvider
from .onchain.etherscan_provider import EtherscanProvider

from .data_models.token_price_data import TokenPriceData
from .rate_limiters.rate_limiter import RateLimiter

# Make all important classes available at package level
__all__ = [
    # Base classes
    'BaseAPIProvider',
    'RateLimiter',
    'TokenPriceData',
    
    # Aggregator providers
    'CoinGeckoProvider',
    'CoinMarketCapProvider',
    'CryptoCompareProvider',
    
    # Blockchain-specific providers
    'BitcoinProvider',
    'EthereumProvider',
    'SolanaProvider',
    'SuiProvider',
    
    # Exchange providers
    'BinanceProvider',
    'BitgetProvider',
    'CoinbaseProvider',
    'KrakenProvider',
    
    # On-chain data providers
    'BitqueryProvider',
    'EtherscanProvider',
]
