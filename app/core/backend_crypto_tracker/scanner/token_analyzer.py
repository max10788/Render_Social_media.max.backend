import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
import time
import random
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
from web3 import Web3
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
import os

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, InvalidAddressException
from app.core.backend_crypto_tracker.config.scanner_config import scanner_config
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum_provider import EthereumProvider
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana_provider import SolanaProvider
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui_provider import SuiProvider
from app.core.backend_crypto_tracker.utils.cache import AnalysisCache  # Angepasst: Verwende AnalysisCache statt TokenCache

# Import all providers
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider
from app.core.backend_crypto_tracker.blockchain.aggregators.coingecko_provider import CoinGeckoProvider
from app.core.backend_crypto_tracker.blockchain.aggregators.coinmarketcap_provider import CoinMarketCapProvider
from app.core.backend_crypto_tracker.blockchain.aggregators.cryptocompare_provider import CryptoCompareProvider
from app.core.backend_crypto_tracker.blockchain.exchanges.bitget_provider import BitgetProvider
from app.core.backend_crypto_tracker.blockchain.exchanges.kraken_provider import KrakenProvider
from app.core.backend_crypto_tracker.blockchain.exchanges.binance_provider import BinanceProvider
from app.core.backend_crypto_tracker.blockchain.exchanges.coinbase_provider import CoinbaseProvider
from app.core.backend_crypto_tracker.blockchain.onchain.bitquery_provider import BitqueryProvider
from app.core.backend_crypto_tracker.processor.database.models.token import Token
from app.core.backend_crypto_tracker.processor.database.models.wallet import WalletAnalysis, WalletTypeEnum

logger = get_logger(__name__)

@dataclass
class TokenAnalysisConfig:
    max_tokens_per_scan: int = 100
    max_market_cap: float = 5_000_000
    min_liquidity_threshold: float = 50_000
    whale_threshold_percentage: float = 5.0
    dev_threshold_percentage: float = 2.0
    sniper_time_threshold_seconds: int = 300
    rugpull_sell_threshold_percentage: float = 50.0
    max_holders_to_analyze: int = 100
    request_delay_seconds: float = 1.0
    enable_cache: bool = True
    cache_ttl_seconds: int = 300  # 5 Minuten

def retry_with_backoff(max_retries=3, base_delay=1, max_delay=60):
    """Decorator für Retry mit exponentiellem Backoff"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            last_exception = None
            
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    retries += 1
                    
                    # Bei Rate-Limit-Fehlern länger warten
                    if "Rate limit exceeded" in str(e) or "429" in str(e):
                        delay = min(base_delay * (2 ** (retries - 1)) + random.uniform(0, 0.5), max_delay)
                    else:
                        delay = min(base_delay * (1.5 ** (retries - 1)) + random.uniform(0, 0.1), max_delay)
                    
                    logger.warning(f"Retry {retries}/{max_retries} after error: {str(e)}. Waiting {delay:.2f}s...")
                    await asyncio.sleep(delay)
            
            logger.error(f"Max retries ({max_retries}) exceeded. Last error: {str(last_exception)}")
            raise last_exception
        return wrapper
    return decorator

class APIManager:
    """Zentralisiert den Zugriff auf verschiedene API-Provider mit Lastverteilung und Fehlerbehandlung"""
    
    def __init__(self):
        self.providers = {}
        self.active_provider = None
        self.provider_failures = {}
        self.session = None
        
    async def initialize(self):
        """Initialisiert alle Provider und die HTTP-Session"""
        self.session = aiohttp.ClientSession()
        
        # Provider nur initialisieren, wenn API-Schlüssel vorhanden sind
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
        
        if os.getenv('BITGET_API_KEY') and os.getenv('BITGET_SECRET_KEY'):
            self.providers['bitget'] = BitgetProvider()
            logger.info("Bitget provider initialized")
        else:
            logger.warning("Bitget API keys not provided, skipping this provider")
        
        if os.getenv('KRAKEN_API_KEY') and os.getenv('KRAKEN_SECRET_KEY'):
            self.providers['kraken'] = KrakenProvider()
            logger.info("Kraken provider initialized")
        else:
            logger.warning("Kraken API keys not provided, skipping this provider")
        
        if os.getenv('BINANCE_API_KEY') and os.getenv('BINANCE_SECRET_KEY'):
            self.providers['binance'] = BinanceProvider()
            logger.info("Binance provider initialized")
        else:
            logger.warning("Binance API keys not provided, skipping this provider")
        
        if os.getenv('COINBASE_API_KEY') and os.getenv('COINBASE_SECRET_KEY'):
            self.providers['coinbase'] = CoinbaseProvider()
            logger.info("Coinbase provider initialized")
        else:
            logger.warning("Coinbase API keys not provided, skipping this provider")
        
        if os.getenv('BITQUERY_API_KEY'):
            self.providers['bitquery'] = BitqueryProvider()
            logger.info("Bitquery provider initialized")
        else:
            logger.warning("Bitquery API key not provided, skipping this provider")
        
        # Provider-Sessions initialisieren
        for provider_name, provider in self.providers.items():
            if hasattr(provider, '__aenter__'):
                try:
                    await provider.__aenter__()
                except Exception as e:
                    logger.error(f"Failed to initialize {provider_name}: {e}")
                    self.providers.pop(provider_name, None)
        
        # Wähle einen aktiven Provider aus den verfügbaren
        available_providers = [p for p in ['coingecko', 'coinmarketcap', 'cryptocompare'] if p in self.providers]
        if available_providers:
            self.active_provider = random.choice(available_providers)
            logger.info(f"Selected {self.active_provider} as active provider")
        else:
            logger.error("No price providers available")
        
    async def get_token_price(self, token_address: str, chain: str):
        """Ruft Token-Preisdaten vom aktiven Provider oder Fallback-Provider ab"""
        providers_to_try = [self.active_provider] if self.active_provider else []
        
        # Fallback-Provider hinzufügen
        fallback_providers = [p for p in ['coingecko', 'coinmarketcap', 'cryptocompare'] 
                            if p != self.active_provider and p in self.providers]
        providers_to_try.extend(fallback_providers)
        
        last_exception = None
        for provider_name in providers_to_try:
            provider = self.providers.get(provider_name)
            if not provider:
                continue
                
            try:
                # Prüfe, ob der Provider in den letzten 5 Minuten mehr als 3 Fehler hatte
                if self.provider_failures.get(provider_name, 0) > 3:
                    failure_time = self.provider_failures.get(f"{provider_name}_time", 0)
                    if time.time() - failure_time < 300:  # 5 Minuten
                        continue
                
                price_data = await provider.get_token_price(token_address, chain)
                
                # Erfolgreiche Anfrage, Fehlerzähler zurücksetzen
                if provider_name in self.provider_failures:
                    self.provider_failures[provider_name] = 0
                
                # Aktualisiere den aktiven Provider bei Erfolg
                self.active_provider = provider_name
                return price_data
                
            except Exception as e:
                last_exception = e
                # Fehlerzähler erhöhen
                self.provider_failures[provider_name] = self.provider_failures.get(provider_name, 0) + 1
                self.provider_failures[f"{provider_name}_time"] = time.time()
                logger.warning(f"Error with provider {provider_name}: {str(e)}")
        
        # Wenn alle Provider fehlschlagen, werfe die letzte Exception
        raise last_exception or APIException("All price providers failed")
    
    async def get_low_cap_tokens(self, max_market_cap: float, limit: int):
        """Ruft Low-Cap-Token-Daten vom aktiven Provider oder Fallback-Provider ab"""
        # Nur CoinGecko und CoinMarketCap unterstützen diese Funktion
        providers_to_try = []
        if self.active_provider in ['coingecko', 'coinmarketcap']:
            providers_to_try.append(self.active_provider)
        
        # Fallback-Provider hinzufügen
        for provider_name in ['coingecko', 'coinmarketcap']:
            if provider_name != self.active_provider and provider_name in self.providers:
                providers_to_try.append(provider_name)
        
        last_exception = None
        for provider_name in providers_to_try:
            provider = self.providers.get(provider_name)
            if not provider:
                continue
                
            try:
                # Prüfe, ob der Provider in den letzten 5 Minuten mehr als 3 Fehler hatte
                if self.provider_failures.get(provider_name, 0) > 3:
                    failure_time = self.provider_failures.get(f"{provider_name}_time", 0)
                    if time.time() - failure_time < 300:  # 5 Minuten
                        continue
                
                tokens = await provider.get_low_cap_tokens(max_market_cap, limit)
                
                # Erfolgreiche Anfrage, Fehlerzähler zurücksetzen
                if provider_name in self.provider_failures:
                    self.provider_failures[provider_name] = 0
                
                # Aktualisiere den aktiven Provider bei Erfolg
                self.active_provider = provider_name
                return tokens
                
            except Exception as e:
                last_exception = e
                # Fehlerzähler erhöhen
                self.provider_failures[provider_name] = self.provider_failures.get(provider_name, 0) + 1
                self.provider_failures[f"{provider_name}_time"] = time.time()
                logger.warning(f"Error with provider {provider_name}: {str(e)}")
        
        # Wenn alle Provider fehlschlagen, werfe die letzte Exception
        raise last_exception or APIException("All token providers failed")
    
    async def close(self):
        """Schließt alle Provider und die Session"""
        for provider in self.providers.values():
            if provider and hasattr(provider, '__aexit__'):
                await provider.__aexit__(None, None, None)
            if provider and hasattr(provider, 'close'):
                await provider.close()
        
        if self.session:
            await self.session.close()

class TokenAnalyzer:
    def __init__(self, config: TokenAnalysisConfig = None):
        self.config = config or TokenAnalysisConfig()
        
        # Provider-Initialisierung
        self.api_manager = APIManager()
        self.ethereum_provider = None
        self.bsc_provider = None
        self.solana_provider = None
        self.sui_provider = None
        
        self.w3_eth = None
        self.w3_bsc = None
        
        # Cache initialisieren, falls aktiviert - angepasst für AnalysisCache
        self.cache = AnalysisCache(max_size=1000, default_ttl=self.config.cache_ttl_seconds) if self.config.enable_cache else None
        
        # Konfiguration laden
        self.ethereum_rpc = scanner_config.rpc_config.ethereum_rpc
        self.bsc_rpc = scanner_config.rpc_config.bsc_rpc
        self.etherscan_key = scanner_config.rpc_config.etherscan_api_key
        self.bscscan_key = scanner_config.rpc_config.bscscan_api_key
        
        # Bekannte Contract-Adressen
        self.known_contracts = scanner_config.rpc_config.known_contracts
        self.cex_wallets = scanner_config.rpc_config.cex_wallets
    
    async def __aenter__(self):
        # API-Manager initialisieren
        await self.api_manager.initialize()
        
        # Blockchain-Provider initialisieren
        if os.getenv('ETHERSCAN_API_KEY'):
            self.ethereum_provider = EthereumProvider(self.etherscan_key)
            logger.info("Ethereum provider initialized")
        else:
            logger.warning("Etherscan API key not provided, using limited functionality")
            self.ethereum_provider = EthereumProvider()  # Funktioniert auch ohne API-Key
        
        if os.getenv('BSCSCAN_API_KEY'):
            self.bsc_provider = EthereumProvider(self.bscscan_key)  # BSC verwendet auch EthereumProvider
            logger.info("BSC provider initialized")
        else:
            logger.warning("BSCscan API key not provided, using limited functionality")
            self.bsc_provider = EthereumProvider()  # Funktioniert auch ohne API-Key
        
        if os.getenv('SOLANA_RPC_URL'):
            self.solana_provider = SolanaProvider()
            logger.info("Solana provider initialized")
        else:
            logger.warning("Solana RPC URL not provided, skipping this provider")
        
        if os.getenv('SUI_RPC_URL'):
            self.sui_provider = SuiProvider()
            logger.info("Sui provider initialized")
        else:
            logger.warning("Sui RPC URL not provided, skipping this provider")
        
        # Web3-Verbindungen initialisieren
        if self.ethereum_rpc:
            self.w3_eth = Web3(Web3.HTTPProvider(self.ethereum_rpc))
        
        if self.bsc_rpc:
            self.w3_bsc = Web3(Web3.HTTPProvider(self.bsc_rpc))
        
        # Provider-Sessions initialisieren
        if self.ethereum_provider:
            await self.ethereum_provider.__aenter__()
        
        if self.bsc_provider:
            await self.bsc_provider.__aenter__()
        
        if self.solana_provider:
            await self.solana_provider.__aenter__()
        
        if self.sui_provider:
            await self.sui_provider.__aenter__()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Sicheres Schließen aller Ressourcen
        close_tasks = []
        
        if self.api_manager:
            close_tasks.append(self._safe_close_api_manager())
        
        if self.ethereum_provider:
            close_tasks.append(self._safe_close(self.ethereum_provider, exc_type, exc_val, exc_tb, "ethereum_provider"))
        
        if self.bsc_provider:
            close_tasks.append(self._safe_close(self.bsc_provider, exc_type, exc_val, exc_tb, "bsc_provider"))
        
        if self.solana_provider:
            close_tasks.append(self._safe_close(self.solana_provider, exc_type, exc_val, exc_tb, "solana_provider"))
        
        if self.sui_provider:
            close_tasks.append(self._safe_close(self.sui_provider, exc_type, exc_val, exc_tb, "sui_provider"))
        
        # Alle Schließvorgänge parallel ausführen
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # Web3-Verbindungen trennen
        if hasattr(self, 'w3_eth') and self.w3_eth:
            self.w3_eth = None
        
        if hasattr(self, 'w3_bsc') and self.w3_bsc:
            self.w3_bsc = None
    
    async def _safe_close(self, service, exc_type, exc_val, exc_tb, service_name):
        """Sicheres Schließen einer Service-Verbindung"""
        try:
            await service.__aexit__(exc_type, exc_val, exc_tb)
            # Zusätzlich: Explizite close-Methode aufrufen, falls vorhanden
            if hasattr(service, 'close'):
                await service.close()
        except Exception as e:
            logger.warning(f"Error closing {service_name}: {str(e)}")
    
    async def _safe_close_api_manager(self):
        """Sicheres Schließen des API-Managers"""
        try:
            await self.api_manager.close()
        except Exception as e:
            logger.warning(f"Error closing API manager: {str(e)}")
    
    async def scan_low_cap_tokens(self, max_tokens: int = None) -> List[Dict[str, Any]]:
        """Hauptfunktion zum Scannen von Low-Cap Tokens"""
        max_tokens = max_tokens or self.config.max_tokens_per_scan
        
        logger.info(f"Starting low-cap token scan (max {max_tokens} tokens)...")
        
        # Cache-Schlüssel für diese Anfrage
        cache_key = f"low_cap_tokens_{max_tokens}_{self.config.max_market_cap}"
        
        # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
        if self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                logger.info(f"Returning cached low-cap tokens data")
                return cached_result
        
        # Hole Low-Cap Tokens über den API-Manager
        tokens = await self.api_manager.get_low_cap_tokens(
            max_market_cap=self.config.max_market_cap,
            limit=max_tokens
        )
        
        if not tokens:
            logger.error("No tokens found")
            return []
        
        # Analysiere Tokens
        results = []
        for i, token in enumerate(tokens):
            try:
                logger.info(f"Processing token {i+1}/{len(tokens)}: {token.symbol}")
                
                # Rate limiting
                if i > 0:
                    await asyncio.sleep(self.config.request_delay_seconds)
                
                analysis = await self.analyze_token(token)
                if analysis:
                    results.append(analysis)
            except Exception as e:
                logger.error(f"Error analyzing {token.symbol}: {e}")
                continue
        
        # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
        if self.cache:
            await self.cache.set(results, self.config.cache_ttl_seconds, cache_key)
        
        logger.info(f"Analysis completed. {len(results)} tokens successfully analyzed.")
        return results
    
    async def analyze_token(self, token_data: Token) -> Optional[Dict[str, Any]]:
        """Vollständige Analyse eines Tokens"""
        logger.info(f"Analyzing token: {token_data.symbol} ({token_data.address})")
        
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"token_analysis_{token_data.address}_{token_data.chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached token analysis for {token_data.symbol}")
                    return cached_result
            
            # Hole Token-Holder
            holders = await self._fetch_token_holders(token_data.address, token_data.chain)
            
            if not holders:
                logger.warning(f"No holder data for {token_data.symbol}")
                return None
            
            # Analysiere Wallets
            wallet_analyses = await self._analyze_wallets(token_data, holders)
            
            # Berechne Token-Score
            score_data = self._calculate_token_score(token_data, wallet_analyses)
            
            result = {
                'token_data': token_data,
                'wallet_analyses': wallet_analyses,
                'token_score': score_data['total_score'],
                'analysis_date': datetime.utcnow(),
                'metrics': score_data['metrics'],
                'risk_flags': score_data['risk_flags']
            }
            
            # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
            if self.cache:
                await self.cache.set(result, self.config.cache_ttl_seconds, cache_key)
            
            return result
        except Exception as e:
            logger.error(f"Error analyzing token {token_data.symbol}: {e}")
            return None
    
    @retry_with_backoff(max_retries=3, base_delay=2, max_delay=30)
    async def analyze_custom_token(self, token_address: str, chain: str) -> Dict[str, Any]:
        """Analysiert einen einzelnen, benutzerdefinierten Token"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"custom_token_analysis_{token_address}_{chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached custom token analysis for {token_address}")
                    return cached_result
            
            # 1. Token-Metadaten abrufen
            token_data = await self._fetch_custom_token_data(token_address, chain)
            
            # Wenn keine Token-Daten abgerufen werden konnten, erstelle ein minimales Token-Objekt
            if not token_data:
                logger.warning(f"Could not retrieve token data for {token_address} on {chain}, creating minimal token object")
                token_data = Token(
                    address=token_address,
                    name="Unknown",
                    symbol="UNKNOWN",
                    chain=chain,
                    market_cap=0,
                    volume_24h=0,
                    liquidity=0,
                    holders_count=0,
                    contract_verified=False,
                    creation_date=None,
                    token_score=0
                )
            
            # 2. Holder-Analyse durchführen
            holders = await self._fetch_token_holders(token_address, chain)
            
            if not holders:
                logger.warning(f"No holder data for {token_address} on {chain}")
                holders = []
            
            # 3. Wallet-Klassifizierung
            wallet_analyses = await self._analyze_wallets(token_data, holders)
            
            # 4. Score-Berechnung
            score_data = self._calculate_token_score(token_data, wallet_analyses)
            
            # 5. Ergebnis zusammenstellen
            analysis_result = {
                'token_info': {
                    'address': token_data.address,
                    'name': token_data.name,
                    'symbol': token_data.symbol,
                    'chain': chain,
                    'market_cap': token_data.market_cap,
                    'volume_24h': token_data.volume_24h,
                    'holders_count': len(holders),
                    'liquidity': token_data.liquidity
                },
                'score': score_data['total_score'],
                'metrics': score_data['metrics'],
                'risk_flags': score_data['risk_flags'],
                'wallet_analysis': {
                    'total_wallets': len(wallet_analyses),
                    'dev_wallets': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.DEV_WALLET]),
                    'whale_wallets': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.WHALE_WALLET]),
                    'rugpull_suspects': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.RUGPULL_SUSPECT]),
                    'top_holders': [
                        {
                            'address': w.wallet_address,
                            'balance': w.balance,
                            'percentage': w.percentage_of_supply,
                            'type': w.wallet_type.value
                        }
                        for w in sorted(wallet_analyses, key=lambda x: x.balance, reverse=True)[:10]
                    ]
                }
            }
            
            # Speichere das Ergebnis im Cache
            if self.cache:
                await self.cache.set(analysis_result, self.config.cache_ttl_seconds, cache_key)
            
            return analysis_result
        except Exception as e:
            logger.error(f"Error analyzing custom token {token_address} on {chain}: {e}")
            raise
    
    @retry_with_backoff(max_retries=3, base_delay=2, max_delay=30)
    async def _fetch_custom_token_data(self, token_address: str, chain: str) -> Optional[Token]:
        """Holt Token-Daten für verschiedene Chains mit Rate-Limit-Handling"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"token_data_{token_address}_{chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached token data for {token_address}")
                    return cached_result
            
            # Hole Token-Daten vom API-Manager
            price_data = await self.api_manager.get_token_price(token_address, chain)
            
            # PRÜFEN, OB price_data None IST
            if price_data is None:
                logger.warning(f"Keine Preisdaten für Token {token_address} auf {chain} verfügbar")
                return None
            
            # Erstelle Token-Objekt mit getattr für sicheren Attributzugriff
            token = Token(
                address=token_address,
                name=getattr(price_data, 'name', ''),  # Verwende getattr mit Standardwert
                symbol=getattr(price_data, 'symbol', ''),  # Verwende getattr mit Standardwert
                chain=chain,
                market_cap=getattr(price_data, 'market_cap', 0),
                volume_24h=getattr(price_data, 'volume_24h', 0),
                liquidity=0,  # Wird später berechnet
                holders_count=0,  # Wird später geholt
                contract_verified=False,  # Wird später geprüft
                creation_date=None,  # Wird später geholt
                token_score=0  # Wird später berechnet
            )
            
            # Zusätzliche Token-Informationen abrufen
            if chain in ['ethereum', 'bsc']:
                token = await self._fetch_evm_token_data(token)
            elif chain == 'solana':
                token = await self._fetch_solana_token_data(token)
            elif chain == 'sui':
                token = await self._fetch_sui_token_data(token)
            
            # Speichere das Ergebnis im Cache
            if self.cache:
                await self.cache.set(token, self.config.cache_ttl_seconds, cache_key)
            
            return token
        except Exception as e:
            logger.error(f"Error fetching token data for {token_address} on {chain}: {e}")
        return None
    
    async def _fetch_evm_token_data(self, token: Token) -> Token:
        """Holt zusätzliche Token-Daten für EVM-Chains"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"evm_token_data_{token.address}_{token.chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached EVM token data for {token.address}")
                    return cached_result
            
            # Wähle den richtigen Web3-Provider
            w3 = self.w3_eth if token.chain == 'ethereum' else self.w3_bsc
            
            # ERC20-ABI für grundlegende Funktionen
            erc20_abi = [
                {
                    "constant": True,
                    "inputs": [],
                    "name": "name",
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "symbol",
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "totalSupply",
                    "outputs": [{"name": "", "type": "uint256"}],
                    "type": "function"
                }
            ]
            
            # Erstelle Contract-Instanz
            contract = w3.eth.contract(address=token.address, abi=erc20_abi)
            
            # Hole Token-Name und Symbol
            try:
                token.name = contract.functions.name().call()
            except Exception:
                pass
            
            try:
                token.symbol = contract.functions.symbol().call()
            except Exception:
                pass
            
            # Prüfe, ob der Contract verifiziert ist
            try:
                if token.chain == 'ethereum' and self.ethereum_provider:
                    token.contract_verified = await self.ethereum_provider.is_contract_verified(token.address, 'ethereum')
                elif token.chain == 'bsc' and self.bsc_provider:
                    token.contract_verified = await self.bsc_provider.is_contract_verified(token.address, 'bsc')
            except Exception:
                pass
            
            # Hole Erstellungsdatum des Contracts
            try:
                if token.chain == 'ethereum' and self.ethereum_provider:
                    creation_tx = await self.ethereum_provider.get_contract_creation_tx(token.address, token.chain)
                elif token.chain == 'bsc' and self.bsc_provider:
                    creation_tx = await self.bsc_provider.get_contract_creation_tx(token.address, token.chain)
                
                if creation_tx and w3:
                    tx_receipt = w3.eth.get_transaction_receipt(creation_tx)
                    if tx_receipt:
                        block = w3.eth.get_block(tx_receipt.blockNumber)
                        token.creation_date = datetime.fromtimestamp(block.timestamp)
            except Exception:
                pass
            
            # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
            if self.cache:
                await self.cache.set(token, self.config.cache_ttl_seconds, cache_key)
            
            return token
        except Exception as e:
            logger.error(f"Error fetching EVM token data for {token.address}: {e}")
            return token
    
    async def _fetch_solana_token_data(self, token: Token) -> Token:
        """Holt zusätzliche Token-Daten für Solana"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"solana_token_data_{token.address}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached Solana token data for {token.address}")
                    return cached_result
            
            if self.solana_provider:
                token_info = await self.solana_provider.get_token_info(token.address)
                
                if token_info:
                    token.name = token_info.get('name', '')
                    token.symbol = token_info.get('symbol', '')
                    token.creation_date = token_info.get('creation_date')
            
            # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
            if self.cache:
                await self.cache.set(token, self.config.cache_ttl_seconds, cache_key)
            
            return token
        except Exception as e:
            logger.error(f"Error fetching Solana token data for {token.address}: {e}")
            return token
    
    async def _fetch_sui_token_data(self, token: Token) -> Token:
        """Holt zusätzliche Token-Daten für Sui"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"sui_token_data_{token.address}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached Sui token data for {token.address}")
                    return cached_result
            
            if self.sui_provider:
                token_info = await self.sui_provider.get_token_info(token.address)
                
                if token_info:
                    token.name = token_info.get('name', '')
                    token.symbol = token_info.get('symbol', '')
                    token.creation_date = token_info.get('creation_date')
            
            # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
            if self.cache:
                await self.cache.set(token, self.config.cache_ttl_seconds, cache_key)
            
            return token
        except Exception as e:
            logger.error(f"Error fetching Sui token data for {token.address}: {e}")
            return token
    
    async def _fetch_token_holders(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """Holt Token-Holder für verschiedene Chains mit Etherscan und Moralis"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"token_holders_{token_address}_{chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached token holders for {token_address}")
                    # Logge auch die gecachten Wallet-Adressen
                    logger.info(f"Found {len(cached_result)} cached holders for {token_address} on {chain}")
                    for i, holder in enumerate(cached_result[:10]):  # Zeige max. 10 Wallets
                        address = holder.get('address', 'N/A')
                        balance = holder.get('balance', 0)
                        percentage = holder.get('percentage', 0)
                        logger.info(f"  Cached Wallet {i+1}: {address} (Balance: {balance}, Percentage: {percentage:.2f}%)")
                    if len(cached_result) > 10:
                        logger.info(f"  ... and {len(cached_result) - 10} more cached wallets")
                    return cached_result
            
            holders = []
            
            # Versuche zuerst Etherscan für Ethereum und BSC
            if chain.lower() in ['ethereum', 'bsc']:
                try:
                    holders = await self.api_manager.get_token_holders_etherscan(token_address, chain)
                    if holders:
                        logger.info(f"Found {len(holders)} holders from Etherscan for {token_address} on {chain}")
                        # Logge die Wallet-Adressen
                        for i, holder in enumerate(holders[:10]):  # Zeige max. 10 Wallets
                            address = holder.get('address', 'N/A')
                            balance = holder.get('balance', 0)
                            percentage = holder.get('percentage', 0)
                            logger.info(f"  Etherscan Wallet {i+1}: {address} (Balance: {balance}, Percentage: {percentage:.2f}%)")
                        if len(holders) > 10:
                            logger.info(f"  ... and {len(holders) - 10} more wallets from Etherscan")
                except Exception as e:
                    logger.warning(f"Error fetching holders from Etherscan: {e}")
            
            # Wenn keine Holder gefunden wurden, versuche Moralis
            if not holders:
                try:
                    holders = await self.api_manager.get_token_holders_moralis(token_address, chain)
                    if holders:
                        logger.info(f"Found {len(holders)} holders from Moralis for {token_address} on {chain}")
                        # Logge die Wallet-Adressen
                        for i, holder in enumerate(holders[:10]):  # Zeige max. 10 Wallets
                            address = holder.get('address', 'N/A')
                            balance = holder.get('balance', 0)
                            percentage = holder.get('percentage', 0)
                            logger.info(f"  Moralis Wallet {i+1}: {address} (Balance: {balance}, Percentage: {percentage:.2f}%)")
                        if len(holders) > 10:
                            logger.info(f"  ... and {len(holders) - 10} more wallets from Moralis")
                except Exception as e:
                    logger.warning(f"Error fetching holders from Moralis: {e}")
            
            # Immer noch die bestehenden Methoden als Fallback versuchen
            if not holders:
                try:
                    source = None
                    if chain.lower() in ['ethereum', 'bsc']:
                        if chain.lower() == 'ethereum' and self.ethereum_provider:
                            holders = await self.ethereum_provider.get_token_holders(token_address, chain)
                            source = "Ethereum provider"
                        elif chain.lower() == 'bsc' and self.bsc_provider:
                            holders = await self.bsc_provider.get_token_holders(token_address, chain)
                            source = "BSC provider"
                    elif chain.lower() == 'solana' and self.solana_provider:
                        holders = await self.solana_provider.get_token_holders(token_address)
                        source = "Solana provider"
                    elif chain.lower() == 'sui' and self.sui_provider:
                        holders = await self.sui_provider.get_token_holders(token_address)
                        source = "Sui provider"
                    
                    if holders:
                        logger.info(f"Found {len(holders)} holders from {source} for {token_address} on {chain}")
                        # Logge die Wallet-Adressen
                        for i, holder in enumerate(holders[:10]):  # Zeige max. 10 Wallets
                            address = holder.get('address', 'N/A')
                            balance = holder.get('balance', 0)
                            percentage = holder.get('percentage', 0)
                            logger.info(f"  {source} Wallet {i+1}: {address} (Balance: {balance}, Percentage: {percentage:.2f}%)")
                        if len(holders) > 10:
                            logger.info(f"  ... and {len(holders) - 10} more wallets from {source}")
                except Exception as e:
                    logger.warning(f"Error fetching holders from fallback provider: {e}")
            
            # Speichere das Ergebnis im Cache
            if self.cache and holders:
                await self.cache.set(holders, self.config.cache_ttl_seconds, cache_key)
            
            return holders
        except Exception as e:
            logger.error(f"Error fetching token holders for {token_address} on {chain}: {e}")
            return []
    
    async def _analyze_wallets(self, token_data: Token, holders: List[Dict[str, Any]]) -> List[WalletAnalysis]:
        """Analysiert die Wallets der Token-Holder"""
        wallet_analyses = []
        
        # Berechne die Gesamtmenge der Token
        total_supply = sum(float(h.get('TokenHolderQuantity', 0)) for h in holders)
        
        # Begrenze die Anzahl der zu analysierenden Wallets
        holders_to_analyze = holders[:self.config.max_holders_to_analyze]
        
        for holder in holders_to_analyze:
            try:
                balance = float(holder.get('TokenHolderQuantity', 0))
                percentage = (balance / total_supply) * 100 if total_supply > 0 else 0
                wallet_address = holder.get('TokenHolderAddress', '')
                
                # Cache-Schlüssel für diese Anfrage
                cache_key = f"wallet_analysis_{wallet_address}_{token_data.address}"
                
                # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
                if self.cache:
                    cached_result = await self.cache.get(cache_key)
                    if cached_result:
                        logger.info(f"Returning cached wallet analysis for {wallet_address}")
                        wallet_analyses.append(cached_result)
                        continue
                
                # Hole Transaktionsdaten für die Wallet
                transaction_data = await self._fetch_wallet_transaction_data(wallet_address, token_data.chain)
                
                # Klassifiziere die Wallet
                wallet_type = self._classify_wallet(wallet_address, balance, percentage, transaction_data, token_data)
                
                # Erstelle Wallet-Analyse
                wallet_analysis = WalletAnalysis(
                    wallet_address=wallet_address,
                    wallet_type=wallet_type,
                    balance=balance,
                    percentage_of_supply=percentage,
                    transaction_count=transaction_data.get('tx_count', 0),
                    first_transaction=transaction_data.get('first_tx_time'),
                    last_transaction=transaction_data.get('last_tx_time'),
                    risk_score=self._calculate_wallet_risk_score(wallet_type, percentage, transaction_data)
                )
                
                wallet_analyses.append(wallet_analysis)
                
                # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
                if self.cache:
                    await self.cache.set(wallet_analysis, self.config.cache_ttl_seconds, cache_key)
                    
            except Exception as e:
                logger.error(f"Error analyzing wallet {holder.get('TokenHolderAddress', 'Unknown')}: {e}")
                continue
        
        return wallet_analyses
    
    async def _fetch_wallet_transaction_data(self, wallet_address: str, chain: str) -> Dict[str, Any]:
        """Holt Transaktionsdaten für eine Wallet"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"wallet_transactions_{wallet_address}_{chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached wallet transactions for {wallet_address}")
                    return cached_result
            
            transaction_data = {}
            
            if chain in ['ethereum', 'bsc']:
                if chain == 'ethereum' and self.ethereum_provider:
                    transaction_data = await self.ethereum_provider.get_wallet_transactions(wallet_address, chain)
                elif chain == 'bsc' and self.bsc_provider:
                    transaction_data = await self.bsc_provider.get_wallet_transactions(wallet_address, chain)
            elif chain == 'solana' and self.solana_provider:
                transaction_data = await self.solana_provider.get_wallet_transactions(wallet_address)
            elif chain == 'sui' and self.sui_provider:
                transaction_data = await self.sui_provider.get_wallet_transactions(wallet_address)
            
            # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
            if self.cache:
                await self.cache.set(transaction_data, self.config.cache_ttl_seconds, cache_key)
            
            return transaction_data
        except Exception as e:
            logger.error(f"Error fetching transaction data for {wallet_address} on {chain}: {e}")
            return {}
    
    def _classify_wallet(self, wallet_address: str, balance: float, percentage: float,
                        transaction_data: Dict[str, Any], token_data: Token) -> WalletTypeEnum:
        """Klassifiziert Wallets basierend auf verschiedenen Kriterien"""
        # Burn Wallet Check
        if wallet_address.lower() in [addr.lower() for addr in self.known_contracts.get('burn_addresses', [])]:
            return WalletTypeEnum.BURN_WALLET
        
        # DEX Contract Check
        if wallet_address.lower() in [addr.lower() for addr in self.known_contracts.values() if isinstance(addr, str)]:
            return WalletTypeEnum.DEX_CONTRACT
        
        # CEX Wallet Check
        for exchange, wallets in self.cex_wallets.items():
            if wallet_address.lower() in [w.lower() for w in wallets]:
                return WalletTypeEnum.CEX_WALLET
        
        # Whale Wallet (>5% der Supply)
        if percentage > self.config.whale_threshold_percentage:
            return WalletTypeEnum.WHALE_WALLET
        
        # Dev Wallet Heuristik (frühe Transaktionen + hoher Anteil)
        tx_count = transaction_data.get('tx_count', 0)
        if percentage > self.config.dev_threshold_percentage and tx_count < 10:
            return WalletTypeEnum.DEV_WALLET
        
        # Sniper Wallet (sehr frühe Käufe)
        first_tx_time = transaction_data.get('first_tx_time')
        if first_tx_time and token_data.creation_date:
            time_diff = first_tx_time - token_data.creation_date
            if time_diff.total_seconds() < self.config.sniper_time_threshold_seconds:  # Erste 5 Minuten
                return WalletTypeEnum.SNIPER_WALLET
        
        # Rugpull Verdacht (plötzliche große Verkäufe)
        recent_sells = transaction_data.get('recent_large_sells', 0)
        if recent_sells > percentage * (self.config.rugpull_sell_threshold_percentage / 100):  # Verkauft mehr als 50% des Holdings
            return WalletTypeEnum.RUGPULL_SUSPECT
        
        return WalletTypeEnum.UNKNOWN
    
    def _calculate_wallet_risk_score(self, wallet_type: WalletTypeEnum, percentage: float, 
                                    transaction_data: Dict[str, Any]) -> float:
        """Berechnet einen Risiko-Score für eine Wallet"""
        risk_score = 0.0
        
        # Basis-Risiko basierend auf Wallet-Typ
        type_risk_scores = {
            WalletTypeEnum.BURN_WALLET: 0.0,
            WalletTypeEnum.DEX_CONTRACT: 0.0,
            WalletTypeEnum.CEX_WALLET: 0.0,
            WalletTypeEnum.WHALE_WALLET: 30.0,
            WalletTypeEnum.DEV_WALLET: 50.0,
            WalletTypeEnum.SNIPER_WALLET: 40.0,
            WalletTypeEnum.RUGPULL_SUSPECT: 90.0,
            WalletTypeEnum.UNKNOWN: 10.0
        }
        
        risk_score += type_risk_scores.get(wallet_type, 10.0)
        
        # Zusätzliches Risiko basierend auf dem prozentualen Anteil
        if percentage > 20:
            risk_score += 30.0
        elif percentage > 10:
            risk_score += 20.0
        elif percentage > 5:
            risk_score += 10.0
        
        # Zusätzliches Risiko für verdächtige Transaktionen
        recent_sells = transaction_data.get('recent_large_sells', 0)
        if recent_sells > 0:
            risk_score += min(recent_sells * 5, 30.0)
        
        return min(risk_score, 100.0)
    
    def _calculate_gini_coefficient(self, balances: List[float]) -> float:
        """Berechnet den Gini-Koeffizienten für Token-Verteilung"""
        if not balances or len(balances) < 2:
            return 0.0
        
        sorted_balances = sorted(balances)
        n = len(sorted_balances)
        cumsum = np.cumsum(sorted_balances)
        
        # Gini-Koeffizient berechnen
        return (2.0 * sum((i + 1) * balance for i, balance in enumerate(sorted_balances))) / (n * cumsum[-1]) - (n + 1) / n
    
    def _calculate_token_score(self, token_data: Token, wallet_analyses: List[WalletAnalysis]) -> Dict[str, Any]:
        """Berechnet einen Risiko-Score für den Token"""
        score = 100.0  # Start mit perfektem Score
        risk_flags = []
        
        # Marktkapitalisierung Score (niedrigere MC = höheres Risiko)
        if token_data.market_cap < 100000:  # < $100k
            score -= 30
            risk_flags.append("very_low_market_cap")
        elif token_data.market_cap < 500000:  # < $500k
            score -= 20
            risk_flags.append("low_market_cap")
        elif token_data.market_cap < 1000000:  # < $1M
            score -= 10
            risk_flags.append("moderate_market_cap")
        
        # Liquiditäts-Score
        if token_data.liquidity < self.config.min_liquidity_threshold:  # < $50k Liquidität
            score -= 25
            risk_flags.append("low_liquidity")
        elif token_data.liquidity < 100000:  # < $100k
            score -= 15
            risk_flags.append("moderate_liquidity")
        
        # Contract Verification
        if not token_data.contract_verified:
            score -= 15
            risk_flags.append("unverified_contract")
        
        # Wallet-Verteilungsanalyse - nur wenn Wallet-Analysen vorhanden sind
        if wallet_analyses:
            total_supply_analyzed = sum(w.percentage_of_supply for w in wallet_analyses)
            whale_percentage = sum(w.percentage_of_supply for w in wallet_analyses if w.wallet_type == WalletTypeEnum.WHALE_WALLET)
            dev_percentage = sum(w.percentage_of_supply for w in wallet_analyses if w.wallet_type == WalletTypeEnum.DEV_WALLET)
            rugpull_suspects = sum(1 for w in wallet_analyses if w.wallet_type == WalletTypeEnum.RUGPULL_SUSPECT)
            
            # Whale-Konzentration
            if whale_percentage > 50:
                score -= 40
                risk_flags.append("high_whale_concentration")
            elif whale_percentage > 30:
                score -= 25
                risk_flags.append("moderate_whale_concentration")
            elif whale_percentage > 15:
                score -= 10
                risk_flags.append("low_whale_concentration")
            
            # Dev Wallet Konzentration
            if dev_percentage > 20:
                score -= 30
                risk_flags.append("high_dev_concentration")
            elif dev_percentage > 10:
                score -= 15
                risk_flags.append("moderate_dev_concentration")
            
            # Rugpull Verdächtige
            if rugpull_suspects > 0:
                score -= rugpull_suspects * 20
                risk_flags.append("rugpull_suspects")
            
            # Gini-Koeffizient
            balances = [w.balance for w in wallet_analyses]
            gini = self._calculate_gini_coefficient(balances)
            if gini > 0.8:  # Sehr ungleiche Verteilung
                score -= 20
                risk_flags.append("very_uneven_distribution")
            elif gini > 0.6:
                score -= 10
                risk_flags.append("uneven_distribution")
        else:
            # Wenn keine Wallet-Analysen vorhanden sind, füge ein Risikoflag hinzu
            score -= 25  # Deutlicher Abzug für fehlende Wallet-Daten
            risk_flags.append("no_wallet_data")
            
            # Setze Standardwerte für die Metriken
            whale_percentage = 0
            dev_percentage = 0
            rugpull_suspects = 0
            gini = 0
        
        # Metriken sammeln
        metrics = {
            'total_holders_analyzed': len(wallet_analyses),
            'whale_wallets': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.WHALE_WALLET]),
            'dev_wallets': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.DEV_WALLET]),
            'rugpull_suspects': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.RUGPULL_SUSPECT]),
            'gini_coefficient': gini,
            'whale_percentage': whale_percentage,
            'dev_percentage': dev_percentage
        }
        
        return {
            'total_score': max(0, min(100, score)),
            'metrics': metrics,
            'risk_flags': risk_flags
        }
    
    async def close(self):
        """Schließt alle offenen Ressourcen wie Provider-Sessions."""
        close_tasks = []
        
        # Schließe API-Manager
        if self.api_manager:
            close_tasks.append(self._safe_close_api_manager())
        
        # Schließe Blockchain-Provider
        providers = [
            self.ethereum_provider,
            self.bsc_provider,
            self.solana_provider,
            self.sui_provider
        ]
        
        for provider in providers:
            if provider:
                close_tasks.append(self._safe_close_provider(provider, provider.__class__.__name__))
        
        # Führe alle Schließvorgänge parallel aus
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        logger.info("TokenAnalyzer resources closed successfully")

    async def _safe_close_provider(self, provider, provider_name):
        """Sicheres Schließen eines Providers"""
        try:
            if hasattr(provider, '__aexit__'):
                await provider.__aexit__(None, None, None)
            if hasattr(provider, 'close'):
                await provider.close()
        except Exception as e:
            logger.error(f"Error closing {provider_name}: {str(e)}")
