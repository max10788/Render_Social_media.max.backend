"""
Base provider class for all API providers.
"""

import asyncio
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import aiohttp

from app.core.backend_crypto_tracker.utils.exceptions import APIException, RateLimitExceededException
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# Globale Singleton-Instanz für den Provider
_unified_api_provider_instance = None
# Globales Flag, um anzuzeigen, ob die Provider bereits initialisiert wurden
_providers_initialized = False


class BaseAPIProvider(ABC):
    """Basisklasse für alle API-Anbieter mit zentralisierter Anfrageverwaltung"""
    
    def __init__(self, name: str, base_url: str, api_key: Optional[str] = None, api_key_env: Optional[str] = None):
        self.name = name
        self.base_url = base_url
        # Wenn kein API-Schlüssel übergeben wurde, versuchen, ihn aus der Umgebungsvariable zu lesen
        if api_key is None and api_key_env is not None:
            self.api_key = os.getenv(api_key_env)
        else:
            self.api_key = api_key
        self.session = None
        self.last_request_time = 0
        self.min_request_interval = 1.0  # Sekunden zwischen Anfragen
        self.is_available = True
        self.retry_count = 0
        self.max_retries = 3
        # Rate-Limiting mit Token-Bucket-Algorithmus
        self.request_tokens = 10  # Start mit 10 Tokens
        self.max_tokens = 10  # Maximale Anzahl von Tokens
        self.refill_rate = 0.5  # Tokens pro Sekunde nachfüllen (30 pro Minute)
        self.last_refill = time.time()
        
        # Blockchain-spezifische Provider
        self.blockchain_providers = {}
        
        # Flag, um zu überprüfen, ob die Provider bereits initialisiert wurden
        self._providers_initialized = False
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        await self._initialize_blockchain_providers()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
        
        # Schließe alle blockchain-spezifischen Provider
        for provider_name, provider in self.blockchain_providers.items():
            if hasattr(provider, '__aexit__'):
                await provider.__aexit__(None, None, None)
            if hasattr(provider, 'close'):
                await provider.close()
    
    async def _initialize_blockchain_providers(self):
        """Initialisiert die blockchain-spezifischen Provider nur einmal"""
        global _providers_initialized
        
        # Prüfe, ob die Provider bereits global initialisiert wurden
        if _providers_initialized:
            logger.debug("Blockchain providers already initialized globally, skipping...")
            return
            
        # Prüfe, ob die Provider bereits instanzspezifisch initialisiert wurden
        if self._providers_initialized:
            logger.debug("Blockchain providers already initialized for this instance, skipping...")
            return
            
        logger.debug("Initializing blockchain providers...")
        
        # Importiere hier die blockchain-spezifischen Provider, um zirkuläre Importe zu vermeiden
        try:
            from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum_provider import EthereumProvider
            from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana_provider import SolanaProvider
            from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui_provider import SuiProvider
            from app.core.backend_crypto_tracker.blockchain.blockchain_specific.bitcoin_provider import BitcoinProvider
            
            # Initialisiere Ethereum-Provider
            if 'ethereum' not in self.blockchain_providers and os.getenv('ETHERSCAN_API_KEY'):
                try:
                    self.blockchain_providers['ethereum'] = EthereumProvider(os.getenv('ETHERSCAN_API_KEY'))
                    logger.debug("Ethereum provider initialized in BaseAPIProvider")
                except Exception as e:
                    logger.error(f"Failed to initialize Ethereum provider: {e}")
            
            # Initialisiere Solana-Provider
            if 'solana' not in self.blockchain_providers and os.getenv('SOLANA_RPC_URL'):
                try:
                    self.blockchain_providers['solana'] = SolanaProvider()
                    logger.debug("Solana provider initialized in BaseAPIProvider")
                except Exception as e:
                    logger.error(f"Failed to initialize Solana provider: {e}")
            
            # Initialisiere Sui-Provider
            if 'sui' not in self.blockchain_providers and os.getenv('SUI_RPC_URL'):
                try:
                    self.blockchain_providers['sui'] = SuiProvider()
                    logger.debug("Sui provider initialized in BaseAPIProvider")
                except Exception as e:
                    logger.error(f"Failed to initialize Sui provider: {e}")
            
            # Initialisiere Bitcoin-Provider
            if 'bitcoin' not in self.blockchain_providers and os.getenv('BITCOIN_RPC_URL'):
                try:
                    self.blockchain_providers['bitcoin'] = BitcoinProvider()
                    logger.debug("Bitcoin provider initialized in BaseAPIProvider")
                except Exception as e:
                    logger.error(f"Failed to initialize Bitcoin provider: {e}")
            
            # Initialisiere die Sessions der blockchain-spezifischen Provider
            for provider_name, provider in list(self.blockchain_providers.items()):
                try:
                    if hasattr(provider, '__aenter__'):
                        await provider.__aenter__()
                except Exception as e:
                    logger.error(f"Failed to initialize {provider_name}: {e}")
                    self.blockchain_providers.pop(provider_name, None)
            
            # Markiere die Provider als initialisiert
            self._providers_initialized = True
            _providers_initialized = True
                        
        except ImportError as e:
            logger.error(f"Failed to import blockchain providers: {e}")
    
    @abstractmethod
    async def get_token_price(self, token_address: str, chain: str):
        """Abstrakte Methode zum Abrufen von Token-Preisen"""
        pass
    
    @abstractmethod
    def get_rate_limits(self) -> Dict[str, int]:
        """Gibt die Rate-Limits zurück (requests_per_minute, requests_per_hour, etc.)"""
        pass
    
    async def get_token_holders(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """
        Ruft Token-Holder-Adressen von einem Smart Contract ab.
        Leitet die Anfrage an den entsprechenden blockchain-spezifischen Provider weiter.
        """
        try:
            # Wähle den richtigen Provider basierend auf der Blockchain
            chain_lower = chain.lower()
            
            # Prüfe, ob ein blockchain-spezifischer Provider für diese Chain verfügbar ist
            if chain_lower in self.blockchain_providers:
                provider = self.blockchain_providers[chain_lower]
                
                # Prüfe, ob der Provider die get_token_holders-Methode unterstützt
                if hasattr(provider, 'get_token_holders'):
                    logger.debug(f"Using blockchain-specific provider for {chain} to get token holders")
                    return await provider.get_token_holders(token_address, chain)
                else:
                    logger.debug(f"Blockchain-specific provider for {chain} does not support get_token_holders")
            
            # Fallback: Verwende die Basisimplementierung
            logger.debug(f"No blockchain-specific provider available for {chain}, using fallback method")
            return await self._get_token_holders_fallback(token_address, chain)
                
        except Exception as e:
            logger.error(f"Error fetching token holders: {e}")
            return []
    
    async def _get_token_holders_fallback(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """
        Fallback-Methode für das Abrufen von Token-Holdern,
        falls kein blockchain-spezifischer Provider verfügbar ist.
        """
        logger.debug(f"Using fallback method for token holders on {chain}")
        return []
    
    async def get_wallet_transactions(self, wallet_address: str, chain: str, token_address: Optional[str] = None) -> Dict[str, Any]:
        """
        Ruft Transaktionsdaten für eine Wallet ab.
        Leitet die Anfrage an den entsprechenden blockchain-spezifischen Provider weiter.
        """
        try:
            # Wähle den richtigen Provider basierend auf der Blockchain
            chain_lower = chain.lower()
            
            # Prüfe, ob ein blockchain-spezifischer Provider für diese Chain verfügbar ist
            if chain_lower in self.blockchain_providers:
                provider = self.blockchain_providers[chain_lower]
                
                # Prüfe, ob der Provider die get_wallet_transactions-Methode unterstützt
                if hasattr(provider, 'get_wallet_transactions'):
                    logger.debug(f"Using blockchain-specific provider for {chain} to get wallet transactions")
                    return await provider.get_wallet_transactions(wallet_address, chain, token_address)
                else:
                    logger.debug(f"Blockchain-specific provider for {chain} does not support get_wallet_transactions")
            
            # Fallback: Verwende die Basisimplementierung
            logger.debug(f"No blockchain-specific provider available for {chain}, using fallback method")
            return await self._get_wallet_transactions_fallback(wallet_address, chain, token_address)
                
        except Exception as e:
            logger.error(f"Error fetching wallet transactions: {e}")
            return {
                'tx_count': 0,
                'first_tx_time': None,
                'last_tx_time': None,
                'recent_large_sells': 0
            }
    
    async def _get_wallet_transactions_fallback(self, wallet_address: str, chain: str, token_address: Optional[str] = None) -> Dict[str, Any]:
        """
        Fallback-Methode für das Abrufen von Wallet-Transaktionen,
        falls kein blockchain-spezifischer Provider verfügbar ist.
        """
        logger.debug(f"Using fallback method for wallet transactions on {chain}")
        return {
            'tx_count': 0,
            'first_tx_time': None,
            'last_tx_time': None,
            'recent_large_sells': 0
        }
    
    async def get_contract_info(self, contract_address: str, chain: str) -> Dict[str, Any]:
        """
        Ruft Informationen über einen Smart Contract ab.
        Leitet die Anfrage an den entsprechenden blockchain-spezifischen Provider weiter.
        """
        try:
            # Wähle den richtigen Provider basierend auf der Blockchain
            chain_lower = chain.lower()
            
            # Prüfe, ob ein blockchain-spezifischer Provider für diese Chain verfügbar ist
            if chain_lower in self.blockchain_providers:
                provider = self.blockchain_providers[chain_lower]
                
                # Prüfe, ob der Provider die get_contract_info-Methode unterstützt
                if hasattr(provider, 'get_contract_info'):
                    logger.debug(f"Using blockchain-specific provider for {chain} to get contract info")
                    return await provider.get_contract_info(contract_address, chain)
                else:
                    logger.debug(f"Blockchain-specific provider for {chain} does not support get_contract_info")
            
            # Fallback: Verwende die Basisimplementierung
            logger.debug(f"No blockchain-specific provider available for {chain}, using fallback method")
            return await self._get_contract_info_fallback(contract_address, chain)
                
        except Exception as e:
            logger.error(f"Error fetching contract info: {e}")
            return {}
    
    async def _get_contract_info_fallback(self, contract_address: str, chain: str) -> Dict[str, Any]:
        """
        Fallback-Methode für das Abrufen von Contract-Informationen,
        falls kein blockchain-spezifischer Provider verfügbar ist.
        """
        logger.debug(f"Using fallback method for contract info on {chain}")
        return {}
    
    async def get_low_cap_tokens(self, max_market_cap: float = 5_000_000, limit: int = 100) -> List[Any]:
        """
        Ruft Low-Cap Tokens ab.
        Diese Methode muss von konkreten Implementierungen überschrieben werden.
        """
        logger.debug("Using fallback method for low_cap_tokens")
        return []
    
    def _refill_tokens(self):
        """Füllt Tokens gemäß dem Refill-Rate auf (Token-Bucket-Algorithmus)"""
        now = time.time()
        elapsed = now - self.last_refill
        # Füge Tokens basierend auf der verstrichenen Zeit hinzu
        tokens_to_add = elapsed * self.refill_rate
        self.request_tokens = min(self.max_tokens, self.request_tokens + tokens_to_add)
        self.last_refill = now
        
    async def _wait_for_token(self):
        """Wartet, bis ein Token verfügbar ist"""
        self._refill_tokens()
        
        if self.request_tokens >= 1:
            self.request_tokens -= 1
            return
        
        # Berechne, wie lange wir warten müssen, bis ein Token verfügbar ist
        wait_time = (1 - self.request_tokens) / self.refill_rate
        logger.debug(f"Rate limit reached for {self.name}. Waiting {wait_time:.2f} seconds...")
        await asyncio.sleep(wait_time)
        
        # Nach dem Warten, verwende ein Token
        self.request_tokens -= 1
    
    async def _make_request(self, url: str, params: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        """Interne Methode für HTTP-Anfragen mit verbessertem Rate-Limiting und Retry-Logik"""
        # Warte auf ein Token (Rate-Limiting)
        await self._wait_for_token()
        
        # Mindestabstand zwischen Anfragen
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
                
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 429:
                    error_text = await response.text()
                    logger.warning(f"Rate limit exceeded for {self.name}: {error_text}")
                    
                    # Retry-Logik für Rate-Limit-Fehler
                    self.retry_count += 1
                    if self.retry_count <= self.max_retries:
                        # Versuche, Retry-After aus den Headern zu lesen
                        retry_after = int(response.headers.get('Retry-After', 0))
                        
                        # Wenn kein Retry-After angegeben ist, verwende exponentielles Backoff
                        if retry_after <= 0:
                            retry_after = min(60, 5 * (2 ** (self.retry_count - 1)))
                        
                        logger.warning(f"Rate limit exceeded. Retry {self.retry_count}/{self.max_retries} after {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        
                        # Setze die Tokens zurück, um weitere Rate-Limits zu vermeiden
                        self.request_tokens = 0
                        
                        return await self._make_request(url, params, headers)
                    else:
                        # Max retries erreicht, setze Zähler zurück
                        self.retry_count = 0
                        raise RateLimitExceededException(self.name, self.max_tokens, "minute")
                
                # Reset retry count bei erfolgreicher Anfrage
                self.retry_count = 0
                
                if response.status >= 400:
                    error_text = await response.text()
                    logger.error(f"HTTP error {response.status} for {self.name}: {error_text}")
                    raise APIException(f"HTTP error {response.status}: {error_text}")
                
                try:
                    return await response.json()
                except Exception as e:
                    logger.error(f"Error parsing JSON response from {self.name}: {e}")
                    raise APIException(f"Error parsing response: {str(e)}")
                    
        except aiohttp.ClientError as e:
            logger.error(f"Network error for {self.name}: {e}")
            raise APIException(f"Network error: {str(e)}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout error for {self.name}")
            raise APIException(f"Timeout error for {self.name}")
    
    async def _make_post_request(self, url: str, json_data: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        """Interne Methode für POST-Anfragen mit verbessertem Rate-Limiting und Retry-Logik"""
        # Warte auf ein Token (Rate-Limiting)
        await self._wait_for_token()
        
        # Mindestabstand zwischen Anfragen
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
                
            async with self.session.post(url, json=json_data, headers=headers) as response:
                if response.status == 429:
                    error_text = await response.text()
                    logger.warning(f"Rate limit exceeded for {self.name}: {error_text}")
                    
                    # Retry-Logik für Rate-Limit-Fehler
                    self.retry_count += 1
                    if self.retry_count <= self.max_retries:
                        # Versuche, Retry-After aus den Headern zu lesen
                        retry_after = int(response.headers.get('Retry-After', 0))
                        
                        # Wenn kein Retry-After angegeben ist, verwende exponentielles Backoff
                        if retry_after <= 0:
                            retry_after = min(60, 5 * (2 ** (self.retry_count - 1)))
                        
                        logger.warning(f"Rate limit exceeded. Retry {self.retry_count}/{self.max_retries} after {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        
                        # Setze die Tokens zurück, um weitere Rate-Limits zu vermeiden
                        self.request_tokens = 0
                        
                        return await self._make_post_request(url, json_data, headers)
                    else:
                        # Max retries erreicht, setze Zähler zurück
                        self.retry_count = 0
                        raise RateLimitExceededException(self.name, self.max_tokens, "minute")
                
                # Reset retry count bei erfolgreicher Anfrage
                self.retry_count = 0
                
                if response.status >= 400:
                    error_text = await response.text()
                    logger.error(f"HTTP error {response.status} for {self.name}: {error_text}")
                    raise APIException(f"HTTP error {response.status}: {error_text}")
                
                try:
                    return await response.json()
                except Exception as e:
                    logger.error(f"Error parsing JSON response from {self.name}: {e}")
                    raise APIException(f"Error parsing response: {str(e)}")
                    
        except aiohttp.ClientError as e:
            logger.error(f"Network error for {self.name}: {e}")
            raise APIException(f"Network error: {str(e)}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout error for {self.name}")
            raise APIException(f"Timeout error for {self.name}")
    
    def check_availability(self) -> bool:
        """Prüft, ob der Anbieter verfügbar ist"""
        return self.is_available
    
    async def close(self):
        """Schließt alle offenen Ressourcen wie Client-Sessions."""
        if hasattr(self, 'session') and self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
            logger.debug(f"{self.name} provider client session closed successfully")
        
        # Schließe alle blockchain-spezifischen Provider
        for provider_name, provider in self.blockchain_providers.items():
            try:
                if hasattr(provider, '__aexit__'):
                    await provider.__aexit__(None, None, None)
                if hasattr(provider, 'close'):
                    await provider.close()
                logger.debug(f"{provider_name} provider closed successfully")
            except Exception as e:
                logger.error(f"Error closing {provider_name}: {str(e)}")


class UnifiedAPIProvider(BaseAPIProvider):
    """
    Konkrete Implementierung des BaseAPIProvider, die als zentraler API-Manager dient.
    Diese Klasse implementiert die abstrakten Methoden und leitet Anfragen an die entsprechenden
    blockchain-spezifischen Provider weiter.
    """
    
    def __init__(self):
        super().__init__(
            name="UnifiedAPIProvider",
            base_url="https://api.example.com",  # Platzhalter-URL
            api_key_env="UNIFIED_API_KEY"  # Platzhalter für API-Key
        )
        
        # Zusätzliche Provider für Preisdaten
        self.price_providers = {}
        
        # Flag, um zu überprüfen, ob die Preis-Provider bereits initialisiert wurden
        self._price_providers_initialized = False
    
    async def __aenter__(self):
        await super().__aenter__()
        await self._initialize_price_providers()
        return self
    
    async def _initialize_price_providers(self):
        """Initialisiert die Preisdaten-Provider nur einmal"""
        # Prüfe, ob die Preis-Provider bereits instanzspezifisch initialisiert wurden
        if self._price_providers_initialized:
            logger.debug("Price providers already initialized for this instance, skipping...")
            return
            
        logger.debug("Initializing price providers...")
        
        try:
            from app.core.backend_crypto_tracker.blockchain.aggregators.coingecko_provider import CoinGeckoProvider
            from app.core.backend_crypto_tracker.blockchain.aggregators.coinmarketcap_provider import CoinMarketCapProvider
            from app.core.backend_crypto_tracker.blockchain.aggregators.cryptocompare_provider import CryptoCompareProvider
            
            # Initialisiere CoinGecko-Provider
            if 'coingecko' not in self.price_providers:
                try:
                    if os.getenv('COINGECKO_API_KEY'):
                        self.price_providers['coingecko'] = CoinGeckoProvider()
                        logger.debug("CoinGecko provider initialized in UnifiedAPIProvider")
                    else:
                        logger.debug("CoinGecko API key not provided, using limited functionality")
                        # CoinGecko funktioniert auch ohne API-Key, aber mit Limits
                        self.price_providers['coingecko'] = CoinGeckoProvider()
                except Exception as e:
                    logger.error(f"Failed to initialize CoinGecko provider: {e}")
            
            # Initialisiere CoinMarketCap-Provider
            if 'coinmarketcap' not in self.price_providers and os.getenv('COINMARKETCAP_API_KEY'):
                try:
                    self.price_providers['coinmarketcap'] = CoinMarketCapProvider()
                    logger.debug("CoinMarketCap provider initialized in UnifiedAPIProvider")
                except Exception as e:
                    logger.error(f"Failed to initialize CoinMarketCap provider: {e}")
            
            # Initialisiere CryptoCompare-Provider
            if 'cryptocompare' not in self.price_providers and os.getenv('CRYPTOCOMPARE_API_KEY'):
                try:
                    self.price_providers['cryptocompare'] = CryptoCompareProvider()
                    logger.debug("CryptoCompare provider initialized in UnifiedAPIProvider")
                except Exception as e:
                    logger.error(f"Failed to initialize CryptoCompare provider: {e}")
            
            # Initialisiere die Sessions der Preisdaten-Provider
            for provider_name, provider in list(self.price_providers.items()):
                try:
                    if hasattr(provider, '__aenter__'):
                        await provider.__aenter__()
                except Exception as e:
                    logger.error(f"Failed to initialize {provider_name}: {e}")
                    self.price_providers.pop(provider_name, None)
            
            # Markiere die Preis-Provider als initialisiert
            self._price_providers_initialized = True
                        
        except ImportError as e:
            logger.error(f"Failed to import price providers: {e}")
    
    async def get_token_price(self, token_address: str, chain: str):
        """Ruft Token-Preisdaten von einem der Preisdaten-Provider ab"""
        providers_to_try = ['coingecko', 'coinmarketcap', 'cryptocompare']
        
        last_exception = None
        for provider_name in providers_to_try:
            provider = self.price_providers.get(provider_name)
            if not provider:
                continue
                
            try:
                logger.debug(f"Trying to get token price from {provider_name}")
                price_data = await provider.get_token_price(token_address, chain)
                
                if price_data:
                    logger.debug(f"Successfully got token price from {provider_name}")
                    return price_data
                    
            except Exception as e:
                last_exception = e
                logger.warning(f"Error with provider {provider_name}: {str(e)}")
        
        # Wenn alle Provider fehlschlagen, werfe die letzte Exception
        if last_exception:
            raise last_exception
        else:
            raise APIException("All price providers failed")
    
    def get_rate_limits(self) -> Dict[str, int]:
        """Gibt die Rate-Limits zurück (requests_per_minute, requests_per_hour, etc.)"""
        return {
            "requests_per_minute": 30,
            "requests_per_hour": 1000,
            "requests_per_day": 10000
        }
    
    async def get_low_cap_tokens(self, max_market_cap: float = 5_000_000, limit: int = 100) -> List[Any]:
        """
        Ruft Low-Cap Tokens von einem der Preisdaten-Provider ab
        """
        providers_to_try = ['coingecko', 'coinmarketcap', 'cryptocompare']
        
        last_exception = None
        for provider_name in providers_to_try:
            provider = self.price_providers.get(provider_name)
            if not provider:
                continue
                
            try:
                logger.debug(f"Trying to get low-cap tokens from {provider_name}")
                
                # Prüfe, ob der Provider eine get_low_cap_tokens-Methode hat
                if hasattr(provider, 'get_low_cap_tokens'):
                    tokens = await provider.get_low_cap_tokens(max_market_cap, limit)
                    
                    if tokens:
                        logger.debug(f"Successfully got {len(tokens)} low-cap tokens from {provider_name}")
                        return tokens
                else:
                    logger.debug(f"Provider {provider_name} does not support get_low_cap_tokens")
                    
            except Exception as e:
                last_exception = e
                logger.warning(f"Error with provider {provider_name}: {str(e)}")
        
        # Wenn alle Provider fehlschlagen, werfe die letzte Exception
        if last_exception:
            raise last_exception
        else:
            logger.warning("No providers available for low-cap tokens")
            return []
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await super().__aexit__(exc_type, exc_val, exc_tb)
        
        # Schließe alle Preisdaten-Provider
        for provider_name, provider in self.price_providers.items():
            try:
                if hasattr(provider, '__aexit__'):
                    await provider.__aexit__(None, None, None)
                if hasattr(provider, 'close'):
                    await provider.close()
                logger.debug(f"{provider_name} price provider closed successfully")
            except Exception as e:
                logger.error(f"Error closing {provider_name}: {str(e)}")


def get_unified_api_provider() -> UnifiedAPIProvider:
    """
    Gibt eine Singleton-Instanz des UnifiedAPIProvider zurück.
    Dies stellt sicher, dass die Provider nur einmal initialisiert werden.
    """
    global _unified_api_provider_instance
    
    if _unified_api_provider_instance is None:
        _unified_api_provider_instance = UnifiedAPIProvider()
        logger.debug("Created new UnifiedAPIProvider instance")
    else:
        logger.debug("Reusing existing UnifiedAPIProvider instance")
    
    return _unified_api_provider_instance
