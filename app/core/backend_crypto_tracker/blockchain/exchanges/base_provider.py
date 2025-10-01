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
# Globale Flags, um anzuzeigen, ob die Provider bereits initialisiert wurden
_blockchain_providers_initialized = False
_price_providers_initialized = False
# Globale Instanzen der Provider, um mehrfache Initialisierungen zu vermeiden
_global_blockchain_providers = {}
_global_price_providers = {}


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
        
        # Zusätzliche Provider für Preisdaten
        self.price_providers = {}
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        # WICHTIG: Keine Provider-Initialisierung hier!
        # Provider werden nur in UnifiedAPIProvider initialisiert
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
    
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
    
    async def close(self):
        """Schließt alle offenen Ressourcen wie Client-Sessions."""
        if hasattr(self, 'session') and self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
            logger.debug(f"{self.name} provider client session closed successfully")


class UnifiedAPIProvider(BaseAPIProvider):
    """
    Konkrete Implementierung des BaseAPIProvider, die als zentraler API-Manager dient.
    Diese Klasse implementiert die abstrakten Methoden und leitet Anfragen an die entsprechenden
    blockchain-spezifischen Provider weiter.
    """
    
    def __init__(self):
        super().__init__(
            name="UnifiedAPIProvider",
            base_url="https://api.example.com".rstrip(),  # Entferne Leerzeichen am Ende
            api_key_env="UNIFIED_API_KEY"
        )
        self._providers_initialized = False
        
        # Prüfe, ob die notwendigen API-Keys vorhanden sind
        if not os.getenv('ETHERSCAN_API_KEY'):
            logger.warning("ETHERSCAN_API_KEY not set. Ethereum token holder data may not be available.")
        
        if not os.getenv('BSCSCAN_API_KEY'):
            logger.warning("BSCSCAN_API_KEY not set. BSC token holder data may not be available.")
        
        # WICHTIG: Füge das providers-Attribut hinzu
        self.providers = {}
    
    async def __aenter__(self):
        await super().__aenter__()
        
        # Initialisiere Provider nur einmal
        if not self._providers_initialized:
            await self._initialize_all_providers()
            self._providers_initialized = True
        
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Schließe alle Provider
        for provider_name, provider in self.blockchain_providers.items():
            if hasattr(provider, '__aexit__'):
                await provider.__aexit__(None, None, None)
            if hasattr(provider, 'close'):
                await provider.close()
        
        for provider_name, provider in self.price_providers.items():
            if hasattr(provider, '__aexit__'):
                await provider.__aexit__(None, None, None)
            if hasattr(provider, 'close'):
                await provider.close()
        
        # Schließe die Basis-Session
        await super().__aexit__(exc_type, exc_val, exc_tb)
    
    async def _initialize_all_providers(self):
        """Initialisiert alle Provider einmalig"""
        global _blockchain_providers_initialized, _price_providers_initialized
        global _global_blockchain_providers, _global_price_providers
        
        # Verwende globale Provider, wenn bereits initialisiert
        if _blockchain_providers_initialized:
            self.blockchain_providers = _global_blockchain_providers
            logger.debug("Using globally initialized blockchain providers")
        else:
            await self._initialize_blockchain_providers()
            _global_blockchain_providers = self.blockchain_providers
            _blockchain_providers_initialized = True
        
        if _price_providers_initialized:
            self.price_providers = _global_price_providers
            logger.debug("Using globally initialized price providers")
        else:
            await self._initialize_price_providers()
            _global_price_providers = self.price_providers
            _price_providers_initialized = True
        
        # WICHTIG: Aktualisiere das providers-Attribut nach der Initialisierung
        self._update_providers_attribute()
    
    def _update_providers_attribute(self):
        """Aktualisiert das providers-Attribut mit allen verfügbaren Providern"""
        self.providers = {}
        
        # Füge Preis-Provider hinzu
        for name, provider in self.price_providers.items():
            self.providers[name] = provider
        
        # Füge Blockchain-Provider hinzu
        for name, provider in self.blockchain_providers.items():
            self.providers[name] = provider
        
        logger.debug(f"Updated providers attribute with {len(self.providers)} providers")
    
async def _initialize_blockchain_providers(self):
    """Initialisiert die blockchain-spezifischen Provider"""
    logger.debug("Initializing blockchain providers...")
    
    try:
        # Ethereum Provider
        if os.getenv('ETHERSCAN_API_KEY'):
            from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum_provider import EthereumProvider
            self.blockchain_providers['ethereum'] = EthereumProvider(os.getenv('ETHERSCAN_API_KEY'))
            await self.blockchain_providers['ethereum'].__aenter__()
            logger.debug("Ethereum provider initialized")
        
        # BSC Provider
        if os.getenv('BSCSCAN_API_KEY'):
            from app.core.backend_crypto_tracker.blockchain.blockchain_specific.bsc_provider import BSCProvider
            self.blockchain_providers['bsc'] = BSCProvider(os.getenv('BSCSCAN_API_KEY'))
            await self.blockchain_providers['bsc'].__aenter__()
            logger.debug("BSC provider initialized")
        
        # Weitere Provider können hier hinzugefügt werden...
        
    except ImportError as e:
        logger.error(f"Failed to import blockchain providers: {e}")
    except Exception as e:
        logger.error(f"Failed to initialize blockchain providers: {e}")
    
    async def _initialize_price_providers(self):
        """Initialisiert die Preisdaten-Provider"""
        logger.debug("Initializing price providers...")
        
        try:
            # CoinGecko Provider (funktioniert auch ohne API-Key)
            from app.core.backend_crypto_tracker.blockchain.aggregators.coingecko_provider import CoinGeckoProvider
            self.price_providers['coingecko'] = CoinGeckoProvider()
            await self.price_providers['coingecko'].__aenter__()
            logger.debug("CoinGecko provider initialized")
            
            # CoinMarketCap Provider (mit API-Key)
            if os.getenv('COINMARKETCAP_API_KEY'):
                from app.core.backend_crypto_tracker.blockchain.aggregators.coinmarketcap_provider import CoinMarketCapProvider
                self.price_providers['coinmarketcap'] = CoinMarketCapProvider()
                await self.price_providers['coinmarketcap'].__aenter__()
                logger.debug("CoinMarketCap provider initialized")
            
            # Weitere Provider können hier hinzugefügt werden...
            
        except ImportError as e:
            logger.error(f"Failed to import price providers: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize price providers: {e}")
    
    async def get_token_price(self, token_address: str, chain: str):
        """Ruft Token-Preisdaten von einem der Preisdaten-Provider ab - CoinMarketCap zuerst"""
        # Reihenfolge: CoinMarketCap (wenn verfügbar), dann CoinGecko
        providers_to_try = []
        
        # CoinMarketCap zuerst, wenn verfügbar (höheres Rate-Limit)
        if 'coinmarketcap' in self.price_providers:
            providers_to_try.append('coinmarketcap')
        
        # Dann CoinGecko als Fallback
        if 'coingecko' in self.price_providers:
            providers_to_try.append('coingecko')
        
        last_exception = None
        for provider_name in providers_to_try:
            provider = self.price_providers.get(provider_name)
            if not provider:
                continue
                
            try:
                logger.debug(f"Trying to get token price from {provider_name}")
                
                # Versuche zuerst die direkte Adressabfrage für CoinMarketCap
                if provider_name == 'coinmarketcap' and hasattr(provider, 'get_token_price_by_address'):
                    price_data = await provider.get_token_price_by_address(token_address, chain)
                else:
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
    
    # Füge diese Methoden hinzu, die der TokenDataResolver erwartet
    async def get_coinmarketcap_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Token-Metadaten von CoinMarketCap"""
        try:
            if 'coinmarketcap' in self.price_providers:
                provider = self.price_providers['coinmarketcap']
                if hasattr(provider, 'get_token_metadata'):
                    return await provider.get_token_metadata(token_address, chain)
        except Exception as e:
            logger.warning(f"Error getting metadata from CoinMarketCap: {e}")
        return None
    
    async def get_coingecko_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Token-Metadaten von CoinGecko"""
        try:
            if 'coingecko' in self.price_providers:
                provider = self.price_providers['coingecko']
                if hasattr(provider, 'get_token_metadata'):
                    return await provider.get_token_metadata(token_address, chain)
        except Exception as e:
            logger.warning(f"Error getting metadata from CoinGecko: {e}")
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        """Gibt die Rate-Limits zurück"""
        return {
            "requests_per_minute": 30,
            "requests_per_hour": 1000,
            "requests_per_day": 10000
        }
    
    async def get_low_cap_tokens(self, max_market_cap: float = 5_000_000, limit: int = 100) -> List[Any]:
        """Ruft Low-Cap Tokens von einem der Preisdaten-Provider ab"""
        providers_to_try = ['coingecko']
        
        for provider_name in providers_to_try:
            provider = self.price_providers.get(provider_name)
            if not provider:
                continue
                
            try:
                logger.debug(f"Trying to get low-cap tokens from {provider_name}")
                
                if hasattr(provider, 'get_low_cap_tokens'):
                    tokens = await provider.get_low_cap_tokens(max_market_cap, limit)
                    
                    if tokens:
                        logger.debug(f"Successfully got {len(tokens)} low-cap tokens from {provider_name}")
                        return tokens
                else:
                    logger.debug(f"Provider {provider_name} does not support get_low_cap_tokens")
                    
            except Exception as e:
                logger.warning(f"Error with provider {provider_name}: {str(e)}")
        
        logger.warning("No providers available for low-cap tokens")
        return []


def get_unified_api_provider() -> UnifiedAPIProvider:
    """
    Gibt eine Singleton-Instanz des UnifiedAPIProvider zurück.
    """
    global _unified_api_provider_instance
    
    if _unified_api_provider_instance is None:
        _unified_api_provider_instance = UnifiedAPIProvider()
        logger.debug("Created new UnifiedAPIProvider instance")
    else:
        logger.debug("Reusing existing UnifiedAPIProvider instance")
    
    return _unified_api_provider_instance


def reset_providers():
    """
    Setzt die globalen Provider-Instanzen zurück.
    """
    global _unified_api_provider_instance, _blockchain_providers_initialized, _price_providers_initialized
    global _global_blockchain_providers, _global_price_providers
    
    if _unified_api_provider_instance is not None:
        try:
            if hasattr(_unified_api_provider_instance, 'close'):
                import asyncio
                loop = asyncio.get_event_loop()
                loop.run_until_complete(_unified_api_provider_instance.close())
        except Exception as e:
            logger.error(f"Error closing existing provider instance: {e}")
    
    _unified_api_provider_instance = None
    _blockchain_providers_initialized = False
    _price_providers_initialized = False
    _global_blockchain_providers = {}
    _global_price_providers = {}
    
    logger.debug("Providers have been reset")
