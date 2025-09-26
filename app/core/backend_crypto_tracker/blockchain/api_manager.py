# blockchain/api_manager.py
import asyncio
import aiohttp
import os
from typing import Dict, List, Optional, Any, Union
from collections import defaultdict
from datetime import datetime, timedelta
import time
import random

from .aggregators.coingecko_provider import CoinGeckoProvider
from .aggregators.coinmarketcap_provider import CoinMarketCapProvider
from .aggregators.cryptocompare_provider import CryptoCompareProvider
from .blockchain_specific.ethereum_provider import EthereumProvider
from .blockchain_specific.solana_provider import SolanaProvider
from .blockchain_specific.sui_provider import SuiProvider
from .exchanges.binance_provider import BinanceProvider
from .exchanges.bitget_provider import BitgetProvider
from .exchanges.coinbase_provider import CoinbaseProvider
from .exchanges.kraken_provider import KrakenProvider
from .onchain.bitquery_provider import BitqueryProvider
from .onchain.etherscan_provider import EtherscanProvider
from ..utils.cache import AnalysisCache
from ..utils.logger import get_logger

logger = get_logger(__name__)

class APIManager:
    def __init__(self):
        # Provider-Dictionary - wird erst in initialize() gefüllt
        self.providers = {}
        
        # Definiere Provider-Prioritäten für verschiedene Datentypen
        self.provider_priorities = {
            'token_price': ['coingecko', 'coinmarketcap', 'cryptocompare', 'binance', 'coinbase'],
            'token_metadata': ['coingecko', 'coinmarketcap', 'cryptocompare', 'etherscan'],
            'token_holders': ['etherscan', 'bitquery', 'ethereum', 'solana', 'sui'],
            'token_liquidity': ['binance', 'coinbase', 'bitget', 'kraken'],
            'contract_verification': ['etherscan', 'ethereum'],
            'transaction_history': ['etherscan', 'bitquery', 'ethereum', 'solana', 'sui']
        }
        
        # Rate-Limiting pro Provider
        self.rate_limits = {
            'coingecko': {'requests_per_minute': 50, 'requests_per_hour': 1000},
            'coinmarketcap': {'requests_per_minute': 333, 'requests_per_day': 10000},
            'cryptocompare': {'requests_per_minute': 30, 'requests_per_hour': 1000},
            'binance': {'requests_per_minute': 1200, 'requests_per_day': 100000},
            'bitget': {'requests_per_minute': 100, 'requests_per_day': 10000},
            'coinbase': {'requests_per_minute': 10, 'requests_per_hour': 600},
            'kraken': {'requests_per_minute': 15, 'requests_per_day': 10000},
            'etherscan': {'requests_per_second': 5, 'requests_per_day': 100000},
            'bitquery': {'requests_per_minute': 20, 'requests_per_day': 10000},
            'ethereum': {'requests_per_second': 10, 'requests_per_day': 100000},
            'solana': {'requests_per_second': 10, 'requests_per_day': 100000},
            'sui': {'requests_per_second': 10, 'requests_per_day': 100000}
        }
        
        # Rate-Limiting-Tracker
        self.request_timestamps = defaultdict(list)
        
        # Cache - verwende AnalysisCache statt TokenCache
        self.cache = AnalysisCache(max_size=1000, default_ttl=300)
        
        # Session für HTTP-Anfragen
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        
        # Initialisiere nur Provider mit gültigen API-Schlüsseln
        await self._initialize_providers()
        
        return self
    
    async def _initialize_providers(self):
        """Initialisiert Provider nur, wenn die entsprechenden API-Schlüssel vorhanden sind"""
        # Aggregatoren
        if os.getenv('COINGECKO_API_KEY'):
            self.providers['coingecko'] = CoinGeckoProvider()
            logger.info("CoinGecko provider initialized")
        else:
            logger.warning("CoinGecko API key not provided, using limited functionality")
            # CoinGecko funktioniert auch ohne API-Key, aber mit Limits
            self.providers['coingecko'] = CoinGeckoProvider()
        
        if os.getenv('COINMARKETCAP_API_KEY'):
            self.providers['coinmarketcap'] = CoinMarketCapProvider()
            logger.info("CoinMarketCap provider initialized")
        else:
            logger.warning("CoinMarketCap API key not provided, skipping this provider")
        
        if os.getenv('CRYPTOCOMPARE_API_KEY'):
            self.providers['cryptocompare'] = CryptoCompareProvider()
            logger.info("CryptoCompare provider initialized")
        else:
            logger.warning("CryptoCompare API key not provided, skipping this provider")
        
        # Blockchain-spezifisch
        if os.getenv('ETHERSCAN_API_KEY'):
            self.providers['ethereum'] = EthereumProvider()
            self.providers['etherscan'] = EtherscanProvider()
            logger.info("Ethereum and Etherscan providers initialized")
        else:
            logger.warning("Etherscan API key not provided, using limited functionality")
            # EthereumProvider kann auch ohne API-Key funktionieren, aber mit Limits
            self.providers['ethereum'] = EthereumProvider()
        
        if os.getenv('SOLANA_RPC_URL'):
            self.providers['solana'] = SolanaProvider()
            logger.info("Solana provider initialized")
        else:
            logger.warning("Solana RPC URL not provided, skipping this provider")
        
        if os.getenv('SUI_RPC_URL'):
            self.providers['sui'] = SuiProvider()
            logger.info("Sui provider initialized")
        else:
            logger.warning("Sui RPC URL not provided, skipping this provider")
        
        # Exchanges
        if os.getenv('BINANCE_API_KEY') and os.getenv('BINANCE_SECRET_KEY'):
            self.providers['binance'] = BinanceProvider()
            logger.info("Binance provider initialized")
        else:
            logger.warning("Binance API keys not provided, skipping this provider")
        
        if os.getenv('BITGET_API_KEY') and os.getenv('BITGET_SECRET_KEY'):
            self.providers['bitget'] = BitgetProvider()
            logger.info("Bitget provider initialized")
        else:
            logger.warning("Bitget API keys not provided, skipping this provider")
        
        if os.getenv('COINBASE_API_KEY') and os.getenv('COINBASE_SECRET_KEY'):
            self.providers['coinbase'] = CoinbaseProvider()
            logger.info("Coinbase provider initialized")
        else:
            logger.warning("Coinbase API keys not provided, skipping this provider")
        
        if os.getenv('KRAKEN_API_KEY') and os.getenv('KRAKEN_SECRET_KEY'):
            self.providers['kraken'] = KrakenProvider()
            logger.info("Kraken provider initialized")
        else:
            logger.warning("Kraken API keys not provided, skipping this provider")
        
        # On-Chain
        if os.getenv('BITQUERY_API_KEY'):
            self.providers['bitquery'] = BitqueryProvider()
            logger.info("Bitquery provider initialized")
        else:
            logger.warning("Bitquery API key not provided, skipping this provider")
        
        # Initialisiere alle Provider
        for provider_name, provider in self.providers.items():
            if hasattr(provider, '__aenter__'):
                try:
                    await provider.__aenter__()
                except Exception as e:
                    logger.error(f"Failed to initialize {provider_name}: {e}")
                    self.providers.pop(provider_name, None)
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Schließe alle Provider
        close_tasks = []
        for provider_name, provider in self.providers.items():
            if hasattr(provider, '__aexit__'):
                close_tasks.append(self._safe_close_provider(provider, provider_name))
        
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # Schließe Session
        if self.session:
            await self.session.close()
    
    async def _safe_close_provider(self, provider, provider_name):
        """Sicheres Schließen eines Providers"""
        try:
            await provider.__aexit__(None, None, None)
            if hasattr(provider, 'close'):
                await provider.close()
        except Exception as e:
            logger.error(f"Fehler beim Schließen von {provider_name}: {e}")
    
    async def get_token_data(self, token_address: str, chain: str) -> Dict[str, Any]:
        """Holt alle Token-Daten von verschiedenen Providern mit Fallback-Strategie"""
        # Cache-Schlüssel für alle Token-Daten
        cache_key = f"token_data:{token_address}:{chain}"
        
        # Prüfe Cache
        cached_data = await self.cache.get(cache_key)
        if cached_data:
            logger.debug(f"Token-Daten aus Cache: {cache_key}")
            return cached_data
        
        # Sammle alle Daten parallel
        tasks = [
            self.get_token_price(token_address, chain),
            self.get_token_metadata(token_address, chain),
            self.get_token_holders(token_address, chain),
            self.get_token_liquidity(token_address, chain),
            self.get_contract_verification(token_address, chain)
        ]
        
        # Führe alle Aufgaben parallel aus
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verarbeite Ergebnisse
        token_data = {
            'address': token_address,
            'chain': chain,
            'name': '',
            'symbol': '',
            'market_cap': 0,
            'volume_24h': 0,
            'liquidity': 0,
            'holders_count': 0,
            'contract_verified': False,
            'creation_date': None,
            'holders': [],
            'price_history': []
        }
        
        # Verarbeite jedes Ergebnis
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"Fehler bei der Datenerfassung: {result}")
                continue
            
            if i == 0:  # Preisdaten
                if result:
                    token_data.update(result)
            elif i == 1:  # Metadaten
                if result:
                    token_data['name'] = result.get('name', '')
                    token_data['symbol'] = result.get('symbol', '')
            elif i == 2:  # Holder
                if result:
                    token_data['holders'] = result
                    token_data['holders_count'] = len(result)
            elif i == 3:  # Liquidität
                if result:
                    token_data['liquidity'] = result
            elif i == 4:  # Contract-Verifizierung
                if result:
                    token_data['contract_verified'] = result.get('verified', False)
                    token_data['creation_date'] = result.get('creation_date')
        
        # Speichere im Cache
        await self.cache.set(token_data, ttl=300, cache_key)  # 5 Minuten
        
        return token_data
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Token-Preisdaten mit Fallback-Strategie"""
        cache_key = f"price:{token_address}:{chain}"
        
        # Prüfe Cache
        cached_data = await self.cache.get(cache_key)
        if cached_data:
            return cached_data
        
        # Versuche alle Provider in der Reihenfolge der Prioritäten
        for provider_name in self.provider_priorities['token_price']:
            provider = self.providers.get(provider_name)
            if not provider:
                continue
            
            # Prüfe Rate-Limit
            if not await self._check_rate_limit(provider_name):
                continue
            
            try:
                # Hole Daten vom Provider
                if hasattr(provider, 'get_token_price'):
                    data = await provider.get_token_price(token_address, chain)
                    if data:
                        # Speichere im Cache
                        await self.cache.set(data, ttl=60, cache_key)  # 1 Minute
                        return data
            except Exception as e:
                logger.warning(f"Fehler bei {provider_name}: {e}")
                continue
        
        return None
    
    async def get_token_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Token-Metadaten mit Fallback-Strategie"""
        cache_key = f"metadata:{token_address}:{chain}"
        
        # Prüfe Cache
        cached_data = await self.cache.get(cache_key)
        if cached_data:
            return cached_data
        
        # Versuche alle Provider in der Reihenfolge der Prioritäten
        for provider_name in self.provider_priorities['token_metadata']:
            provider = self.providers.get(provider_name)
            if not provider:
                continue
            
            # Prüfe Rate-Limit
            if not await self._check_rate_limit(provider_name):
                continue
            
            try:
                # Hole Daten vom Provider
                if hasattr(provider, 'get_token_metadata'):
                    data = await provider.get_token_metadata(token_address, chain)
                    if data:
                        # Speichere im Cache
                        await self.cache.set(data, ttl=3600, cache_key)  # 1 Stunde
                        return data
            except Exception as e:
                logger.warning(f"Fehler bei {provider_name}: {e}")
                continue
        
        return None
    
    async def get_token_holders(self, token_address: str, chain: str) -> Optional[List[Dict[str, Any]]]:
        """Holt Token-Holder mit Fallback-Strategie"""
        cache_key = f"holders:{token_address}:{chain}"
        
        # Prüfe Cache
        cached_data = await self.cache.get(cache_key)
        if cached_data:
            return cached_data
        
        # Versuche alle Provider in der Reihenfolge der Prioritäten
        for provider_name in self.provider_priorities['token_holders']:
            provider = self.providers.get(provider_name)
            if not provider:
                continue
            
            # Prüfe Rate-Limit
            if not await self._check_rate_limit(provider_name):
                continue
            
            try:
                # Hole Daten vom Provider
                if hasattr(provider, 'get_token_holders'):
                    data = await provider.get_token_holders(token_address, chain)
                    if data:
                        # Speichere im Cache
                        await self.cache.set(data, ttl=600, cache_key)  # 10 Minuten
                        return data
            except Exception as e:
                logger.warning(f"Fehler bei {provider_name}: {e}")
                continue
        
        return None
    
    async def get_token_liquidity(self, token_address: str, chain: str) -> Optional[float]:
        """Holt Token-Liquidität mit Fallback-Strategie"""
        cache_key = f"liquidity:{token_address}:{chain}"
        
        # Prüfe Cache
        cached_data = await self.cache.get(cache_key)
        if cached_data:
            return cached_data
        
        # Versuche alle Provider in der Reihenfolge der Prioritäten
        for provider_name in self.provider_priorities['token_liquidity']:
            provider = self.providers.get(provider_name)
            if not provider:
                continue
            
            # Prüfe Rate-Limit
            if not await self._check_rate_limit(provider_name):
                continue
            
            try:
                # Hole Daten vom Provider
                if hasattr(provider, 'get_token_liquidity'):
                    data = await provider.get_token_liquidity(token_address, chain)
                    if data is not None:
                        # Speichere im Cache
                        await self.cache.set(data, ttl=300, cache_key)  # 5 Minuten
                        return data
            except Exception as e:
                logger.warning(f"Fehler bei {provider_name}: {e}")
                continue
        
        return None
    
    async def get_contract_verification(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Contract-Verifizierung mit Fallback-Strategie"""
        cache_key = f"verification:{token_address}:{chain}"
        
        # Prüfe Cache
        cached_data = await self.cache.get(cache_key)
        if cached_data:
            return cached_data
        
        # Versuche alle Provider in der Reihenfolge der Prioritäten
        for provider_name in self.provider_priorities['contract_verification']:
            provider = self.providers.get(provider_name)
            if not provider:
                continue
            
            # Prüfe Rate-Limit
            if not await self._check_rate_limit(provider_name):
                continue
            
            try:
                # Hole Daten vom Provider
                if hasattr(provider, 'get_contract_verification'):
                    data = await provider.get_contract_verification(token_address, chain)
                    if data:
                        # Speichere im Cache
                        await self.cache.set(data, ttl=3600, cache_key)  # 1 Stunde
                        return data
            except Exception as e:
                logger.warning(f"Fehler bei {provider_name}: {e}")
                continue
        
        return None
    
    async def _check_rate_limit(self, provider_name: str) -> bool:
        """Prüft, ob das Rate-Limit für einen Provider erreicht wurde"""
        now = time.time()
        limits = self.rate_limits.get(provider_name, {})
        
        # Prüfe pro Minute
        if 'requests_per_minute' in limits:
            minute_ago = now - 60
            recent_requests = [ts for ts in self.request_timestamps[provider_name] if ts > minute_ago]
            if len(recent_requests) >= limits['requests_per_minute']:
                return False
        
        # Prüfe pro Stunde
        if 'requests_per_hour' in limits:
            hour_ago = now - 3600
            recent_requests = [ts for ts in self.request_timestamps[provider_name] if ts > hour_ago]
            if len(recent_requests) >= limits['requests_per_hour']:
                return False
        
        # Prüfe pro Tag
        if 'requests_per_day' in limits:
            day_ago = now - 86400
            recent_requests = [ts for ts in self.request_timestamps[provider_name] if ts > day_ago]
            if len(recent_requests) >= limits['requests_per_day']:
                return False
        
        # Prüfe pro Sekunde
        if 'requests_per_second' in limits:
            second_ago = now - 1
            recent_requests = [ts for ts in self.request_timestamps[provider_name] if ts > second_ago]
            if len(recent_requests) >= limits['requests_per_second']:
                return False
        
        # Füge aktuellen Request hinzu
        self.request_timestamps[provider_name].append(now)
        
        # Bereinige alte Timestamps
        self._cleanup_old_timestamps(provider_name, now)
        
        return True
    
    def _cleanup_old_timestamps(self, provider_name: str, now: float):
        """Bereinigt alte Timestamps für einen Provider"""
        if provider_name not in self.request_timestamps:
            return
        
        # Behalte nur Timestamps der letzten 24 Stunden
        day_ago = now - 86400
        self.request_timestamps[provider_name] = [
            ts for ts in self.request_timestamps[provider_name] if ts > day_ago
        ]
    # Füge diese Methoden zur APIManager-Klasse hinzu (token_analyzer.py, Zeile ca. 200)
    
    async def get_token_holders_etherscan(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """Holt Token-Holder über Etherscan API"""
        try:
            if chain.lower() == 'ethereum' and self.ethereum_provider:
                return await self.ethereum_provider.get_token_holders(token_address, chain)
            elif chain.lower() == 'bsc' and self.bsc_provider:
                return await self.bsc_provider.get_token_holders(token_address, chain)
            else:
                logger.warning(f"No Etherscan provider available for chain: {chain}")
                return []
        except Exception as e:
            logger.error(f"Error getting token holders from Etherscan: {e}")
            return []
    
    async def get_token_holders_moralis(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """Holt Token-Holder über Moralis API"""
        try:
            # Moralis API implementierung
            if not os.getenv('MORALIS_API_KEY'):
                logger.warning("Moralis API key not provided")
                return []
            
            # Chain-Mapping für Moralis
            chain_mapping = {
                'ethereum': 'eth',
                'bsc': 'bsc',
                'polygon': 'polygon',
                'avalanche': 'avalanche'
            }
            
            moralis_chain = chain_mapping.get(chain.lower())
            if not moralis_chain:
                logger.warning(f"Chain {chain} not supported by Moralis")
                return []
            
            url = f"https://deep-index.moralis.io/api/v2/erc20/{token_address}/owners"
            headers = {
                'X-API-Key': os.getenv('MORALIS_API_KEY'),
                'Content-Type': 'application/json'
            }
            params = {
                'chain': moralis_chain,
                'limit': 100,
                'cursor': ''
            }
            
            if not self.session:
                self.session = aiohttp.ClientSession()
                
            async with self.session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    holders = []
                    
                    for owner in data.get('result', []):
                        holders.append({
                            'TokenHolderAddress': owner.get('owner_address'),
                            'TokenHolderQuantity': owner.get('balance'),
                            'percentage': 0  # Wird später berechnet
                        })
                    
                    return holders
                else:
                    error_text = await response.text()
                    logger.error(f"Moralis API error {response.status}: {error_text}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error getting token holders from Moralis: {e}")
            return []
    
    async def get_token_holders_generic(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """Generische Methode für Token-Holder mit Fallback-Logic"""
        holders = []
        
        # Strategie 1: Etherscan/BSCscan
        try:
            holders = await self.get_token_holders_etherscan(token_address, chain)
            if holders:
                logger.info(f"Successfully retrieved {len(holders)} holders from Etherscan")
                return holders
        except Exception as e:
            logger.warning(f"Etherscan failed: {e}")
        
        # Strategie 2: Moralis
        try:
            holders = await self.get_token_holders_moralis(token_address, chain)
            if holders:
                logger.info(f"Successfully retrieved {len(holders)} holders from Moralis")
                return holders
        except Exception as e:
            logger.warning(f"Moralis failed: {e}")
        
        # Strategie 3: Bitquery (wenn verfügbar)
        try:
            if 'bitquery' in self.providers:
                holders = await self._get_holders_from_bitquery(token_address, chain)
                if holders:
                    logger.info(f"Successfully retrieved {len(holders)} holders from Bitquery")
                    return holders
        except Exception as e:
            logger.warning(f"Bitquery failed: {e}")
        
        logger.warning(f"All token holder providers failed for {token_address} on {chain}")
        return []
    
    async def _get_holders_from_bitquery(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """Holt Token-Holder über Bitquery GraphQL API"""
        try:
            bitquery_provider = self.providers.get('bitquery')
            if not bitquery_provider:
                return []
            
            # GraphQL Query für Token Holders
            query = f"""
            {{
              ethereum(network: {chain}) {{
                tokenHolders(
                  currency: {{is: "{token_address}"}}
                  limit: 100
                  orderBy: {{descending: balance}}
                ) {{
                  address {{
                    address
                  }}
                  balance
                }}
              }}
            }}
            """
            
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': bitquery_provider.api_key if bitquery_provider.api_key else ''
            }
            
            data = await bitquery_provider._make_post_request(
                bitquery_provider.base_url, 
                {'query': query}, 
                headers
            )
            
            if data.get('data') and data['data'].get('ethereum'):
                token_holders = data['data']['ethereum']['tokenHolders']
                holders = []
                
                for holder in token_holders:
                    holders.append({
                        'TokenHolderAddress': holder['address']['address'],
                        'TokenHolderQuantity': holder['balance'],
                        'percentage': 0  # Wird später berechnet
                    })
                
                return holders
                
        except Exception as e:
            logger.error(f"Error getting holders from Bitquery: {e}")
        
        return []
