import aiohttp
import logging
import os
import time
import json
import asyncio
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, RateLimitExceededException
from app.core.backend_crypto_tracker.config.blockchain_api_keys import APIKeys
from app.core.backend_crypto_tracker.processor.database.models.token import Token
# Importe aktualisiert
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData
# Import all providers
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseProvider

from app.core.backend_crypto_tracker.blockchain.onchain.bitquery_provider import BitqueryProvider

logger = get_logger(__name__)

class APIRateLimiter:
    """Einfacher Rate-Limiter für API-Anfragen"""

    def __init__(self):
        self.request_timestamps = {}
        self.limits = {}

    async def acquire(self, service_name: str, max_requests: int, time_window: int) -> bool:
        """Prüft, ob eine Anfrage gemacht werden kann"""
        current_time = time.time()

        if service_name not in self.request_timestamps:
            self.request_timestamps[service_name] = []

        # Alte Zeitstempel entfernen
        window_start = current_time - time_window
        self.request_timestamps[service_name] = [
            ts for ts in self.request_timestamps[service_name] if ts > window_start
        ]

        # Prüfen, ob das Limit erreicht ist
        if len(self.request_timestamps[service_name]) >= max_requests:
            return False

        # Neue Anfrage hinzufügen
        self.request_timestamps[service_name].append(current_time)
        return True

    def get_wait_time(self, service_name: str, max_requests: int, time_window: int) -> float:
        """Berechnet die Wartezeit bis zur nächsten Anfrage"""
        current_time = time.time()

        if service_name not in self.request_timestamps:
            return 0

        # Alte Zeitstempel entfernen
        window_start = current_time - time_window
        self.request_timestamps[service_name] = [
            ts for ts in self.request_timestamps[service_name] if ts > window_start
        ]

        # Wenn das Limit erreicht ist, berechne Wartezeit
        if len(self.request_timestamps[service_name]) >= max_requests:
            oldest_request = min(self.request_timestamps[service_name])
            return max(0, (oldest_request + time_window) - current_time)

        return 0

class PriceService:
    # Klasseninitialisierung angepasst
    def __init__(self, coingecko_api_key: Optional[str] = None):
        self.coingecko_api_key = coingecko_api_key or os.getenv('COINGECKO_API_KEY')
        self.session = None
        self.api_key_valid = False
        self.cache = {}  # Einfacher Cache für Token-Preise
        self.cache_expiry = 300  # 5 Minuten Cache
        
        # Provider für verschiedene Blockchains
        self.coingecko_provider = CoinGeckoProvider(self.coingecko_api_key)
        self.ethereum_provider = EthereumProvider()
        self.bsc_provider = EthereumProvider()  # BSC verwendet auch EthereumProvider
        self.solana_provider = SolanaProvider()
        self.sui_provider = SuiProvider()
        
        # Rate-Limiter für direkte API-Aufrufe
        self.rate_limiter = APIRateLimiter()

        # API-Limits für verschiedene Dienste
        self.API_LIMITS = {
            "CoinGecko": (10, 60),  # 10 req/min
            "Uniswap": (30, 60),
            "Etherscan": (5, 60),
            "BscScan": (5, 60),
            "PancakeSwap": (30, 60),
            "Jupiter": (60, 60),
        }

        # Für Demo-API-Schlüssel immer die öffentliche API verwenden
        self.base_url = "https://api.coingecko.com/api/v3"

        # Protokolliere den Status des API-Schlüssels (maskiert für Sicherheit)
        if self.coingecko_api_key:
            if len(self.coingecko_api_key) > 8:
                masked_key = self.coingecko_api_key[:4] + "..." + self.coingecko_api_key[-4:]
            else:
                masked_key = "***"
            logger.info(f"CoinGecko API key configured: {masked_key}")
        else:
            logger.warning("No CoinGecko API key configured - using public API with rate limits")

    # aenter und aexit Methoden angepasst
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        # Validiere den API-Schlüssel beim Start
        await self._validate_api_key()
        await self.coingecko_provider.__aenter__()
        await self.ethereum_provider.__aenter__()
        await self.bsc_provider.__aenter__()
        await self.solana_provider.__aenter__()
        await self.sui_provider.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.coingecko_provider.__aexit__(exc_type, exc_val, exc_tb)
        await self.ethereum_provider.__aexit__(exc_type, exc_val, exc_tb)
        await self.bsc_provider.__aexit__(exc_type, exc_val, exc_tb)
        await self.solana_provider.__aexit__(exc_type, exc_val, exc_tb)
        await self.sui_provider.__aexit__(exc_type, exc_val, exc_tb)
        if self.session:
            await self.session.close()

    async def _validate_api_key(self):
        """Validiert den CoinGecko API-Schlüssel"""
        if not self.coingecko_api_key:
            self.api_key_valid = False
            return

        try:
            # Zuerst mit der öffentlichen API testen
            url = "https://api.coingecko.com/api/v3/ping"
            headers = {"x-cg-pro-api-key": self.coingecko_api_key}

            async with self.session.get(url, headers=headers) as response:
                if response.status == 200:
                    self.api_key_valid = True
                    logger.info("CoinGecko API key is valid with public API")
                else:
                    error_text = await response.text()
                    logger.error(f"CoinGecko API key validation failed with public API: {response.status} - {error_text}")
                    self.api_key_valid = False
        except Exception as e:
            logger.error(f"Error validating CoinGecko API key: {e}")
            self.api_key_valid = False

    def _get_from_cache(self, key: str):
        """Holt Daten aus dem Cache"""
        if key in self.cache:
            data, timestamp = self.cache[key]
            if time.time() - timestamp < self.cache_expiry:
                return data
            # Cache-Eintrag ist abgelaufen, entferne ihn
            del self.cache[key]
        return None

    def _save_to_cache(self, key: str, data):
        """Speichert Daten im Cache"""
        self.cache[key] = (data, time.time())

    # Methode get_token_price angepasst
    async def get_token_price(self, token_address: str, chain: str) -> TokenPriceData:
        """Holt Preisinformationen für ein Token basierend auf der Blockchain mit Multi-API-Fallback"""
        if not chain_config.is_supported(chain):
            raise ValueError(f"Unsupported chain: {chain}")

        # Prüfe Cache
        cache_key = f"{chain}:{token_address}"
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            logger.debug(f"Using cached data for {token_address}")
            return cached_data

        try:
            # Wähle den richtigen Provider basierend auf der Blockchain
            if chain == 'ethereum':
                provider = self.ethereum_provider
            elif chain == 'bsc':
                provider = self.bsc_provider
            elif chain == 'solana':
                provider = self.solana_provider
            elif chain == 'sui':
                provider = self.sui_provider
            else:
                raise ValueError(f"Unsupported chain: {chain}")
            
            # API-Anfrage mit dem ausgewählten Provider
            async with provider:
                api_price_data = await provider.get_token_price(token_address, chain)
            
            if api_price_data:
                # Konvertiere API-TokenPriceData zu lokalem TokenPriceData
                token_data = TokenPriceData(
                    price=api_price_data.price,
                    market_cap=api_price_data.market_cap,
                    volume_24h=api_price_data.volume_24h,
                    price_change_percentage_24h=api_price_data.price_change_percentage_24h,
                    source=api_price_data.source,
                    confidence_score=self._calculate_confidence_score(api_price_data)
                )
                
                # Speichere im Cache
                self._save_to_cache(cache_key, token_data)
                return token_data
            else:
                logger.warning(f"No price data found for {token_address}")
                raise ValueError(f"Token data could not be retrieved for {token_address} on {chain}")
                
        except Exception as e:
            logger.error(f"Error fetching token price: {e}")
            raise APIException(f"Failed to fetch token price: {str(e)}")

    def _calculate_confidence_score(self, price_data: TokenPriceData) -> float:
        """Berechnet einen Confidence-Score basierend auf der Datenqualität"""
        score = 1.0

        # Reduziere Score, wenn wichtige Daten fehlen
        if price_data.market_cap == 0:
            score -= 0.3

        if price_data.volume_24h == 0:
            score -= 0.2

        if price_data.price_change_percentage_24h is None:
            score -= 0.1

        # Reduziere Score für bestimmte Quellen
        if price_data.source == "Binance":
            score -= 0.05  # Binance hat oft weniger vollständige Daten

        return max(0.1, score)  # Mindestens 0.1

    # Restliche Methoden bleiben unverändert...
    async def _get_evm_token_price(self, token_address: str, chain: str) -> TokenPriceData:
        """Preis für EVM-basierte Token (Ethereum, BSC) mit Rate-Limiting"""
        platform_id = 'ethereum' if chain == 'ethereum' else 'binance-smart-chain'

        # Zuerst mit CoinGecko versuchen
        if self.api_key_valid:
            try:
                # Rate-Limiting mit korrektem acquire-Aufruf
                max_req, window = self.API_LIMITS["CoinGecko"]
                while not await self.rate_limiter.acquire("CoinGecko", max_req, window):
                    wait_time = self.rate_limiter.get_wait_time("CoinGecko", max_req, window)
                    logger.warning(f"Rate limit reached for CoinGecko, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)

                token_data = await self._get_from_coingecko(token_address, platform_id)
                if token_data:
                    # Speichere im Cache
                    self._save_to_cache(f"{chain}:{token_address}", token_data)
                    return token_data
            except Exception as e:
                logger.error(f"Error fetching from CoinGecko: {e}")

        # Fallback: Alternative Datenquellen
        logger.warning(f"Falling back to alternative sources for {token_address}")
        return await self._get_evm_token_price_fallback(token_address, chain)

    async def _get_from_coingecko(self, token_address: str, platform_id: str) -> Optional[TokenPriceData]:
        """Holt Tokendaten von CoinGecko mit Rate-Limiting"""
        # Verwende die öffentliche API für Demo-Schlüssel
        url = f"{self.base_url}/simple/token_price/{platform_id}"
        params = {
            'contract_addresses': token_address,
            'vs_currencies': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true'
        }

        headers = {}
        if self.api_key_valid:
            headers['x-cg-pro-api-key'] = self.coingecko_api_key

        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                logger.debug(f"CoinGecko response status: {response.status}")
                logger.debug(f"Request URL: {response.url}")

                if response.status == 429:
                    error_text = await response.text()
                    logger.error(f"Rate limit exceeded. Response: {error_text}")
                    raise RateLimitExceededException("CoinGecko", 10, "minute")

                response.raise_for_status()
                data = await response.json()

                logger.debug(f"CoinGecko response (truncated): {str(data)[:200]}...")

                token_data = data.get(token_address.lower(), {})
                if not token_data:
                    logger.error(f"No token data found in response for {token_address}")
                    return None

                return TokenPriceData(
                    price=token_data.get('usd', 0),
                    market_cap=token_data.get('usd_market_cap', 0),
                    volume_24h=token_data.get('usd_24h_vol', 0),
                    price_change_percentage_24h=token_data.get('usd_24h_change')
                )

        except aiohttp.ClientError as e:
            logger.error(f"Error fetching from CoinGecko: {e}")
            return None

    async def _get_evm_token_price_fallback(self, token_address: str, chain: str) -> TokenPriceData:
        """Holt Tokendaten von alternativen Quellen für EVM-Chains mit Rate-Limiting"""
        # Für Ethereum können wir DEX APIs wie Uniswap oder 1inch verwenden
        # Für BSC können wir PancakeSwap verwenden

        if chain == 'ethereum':
            # Versuche, den Preis von Uniswap zu holen
            try:
                token_data = await self._get_uniswap_price(token_address)
                if token_data:
                    # Speichere im Cache
                    self._save_to_cache(f"ethereum:{token_address}", token_data)
                    return token_data
            except Exception as e:
                logger.error(f"Error fetching from Uniswap: {e}")

            # Fallback: Etherscan
            return await self._get_etherscan_price(token_address)
        elif chain == 'bsc':
            # Versuche, den Preis von PancakeSwap zu holen
            try:
                token_data = await self._get_pancakeswap_price(token_address)
                if token_data:
                    # Speichere im Cache
                    self._save_to_cache(f"bsc:{token_address}", token_data)
                    return token_data
            except Exception as e:
                logger.error(f"Error fetching from PancakeSwap: {e}")

            # Fallback: BscScan
            return await self._get_bscscan_price(token_address)

        return TokenPriceData(price=0, market_cap=0, volume_24h=0)

    async def _get_uniswap_price(self, token_address: str) -> TokenPriceData:
        """Holt Token-Preis von Uniswap mit korrektem Rate-Limiting"""
        try:
            # Rate-Limiting mit korrektem acquire-Aufruf
            max_req, window = self.API_LIMITS["Uniswap"]
            while not await self.rate_limiter.acquire("Uniswap", max_req, window):
                wait_time = self.rate_limiter.get_wait_time("Uniswap", max_req, window)
                logger.warning(f"Rate limit reached for Uniswap, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

            # Uniswap V3 API
            url = "https://api.uniswap.org/v1/quote"
            params = {
                'tokenIn': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',  # WETH
                'tokenOut': token_address,
                'amount': '1000000000000000000',  # 1 WETH
                'type': 'exactIn'
            }

            async with self.session.get(url, params=params) as response:
                if response.status == 409:
                    logger.warning(f"Uniswap returned 409 Conflict for {token_address}")
                    raise APIException(f"Uniswap conflict for token {token_address}")

                response.raise_for_status()
                data = await response.json()

                if data.get('quote'):
                    # Berechne den Preis basierend auf dem Quote
                    price = float(data['quote']) / 1000000000000000000  # WETH to token
                    return TokenPriceData(
                        price=price,
                        market_cap=0,  # Nicht verfügbar
                        volume_24h=0,  # Nicht verfügbar
                        price_change_percentage_24h=0,  # Nicht verfügbar
                        source="Uniswap"
                    )
        except Exception as e:
            logger.error(f"Error fetching from Uniswap: {e}")
            raise

        # Fallback: Etherscan
        return await self._get_etherscan_price(token_address)

    async def _get_etherscan_price(self, token_address: str) -> TokenPriceData:
        """Holt Token-Preis von Etherscan mit korrektem Rate-Limiting"""
        try:
            # Rate-Limiting mit korrektem acquire-Aufruf
            max_req, window = self.API_LIMITS["Etherscan"]
            while not await self.rate_limiter.acquire("Etherscan", max_req, window):
                wait_time = self.rate_limiter.get_wait_time("Etherscan", max_req, window)
                logger.warning(f"Rate limit reached for Etherscan, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

            # Etherscan API
            url = "https://api.etherscan.io/api"
            params = {
                'module': 'token',
                'action': 'tokenprice',
                'contractaddress': token_address,
                'apikey': os.getenv('ETHERSCAN_API_KEY', 'YourApiKeyToken')
            }

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get('status') == '1' and data.get('result'):
                    result = data['result']
                    return TokenPriceData(
                        price=float(result.get('ethusd', 0)),
                        market_cap=0,  # Nicht verfügbar
                        volume_24h=0,  # Nicht verfügbar
                        price_change_percentage_24h=0,  # Nicht verfügbar
                        source="Etherscan"
                    )
        except Exception as e:
            logger.error(f"Error fetching from Etherscan: {e}")

        return TokenPriceData(price=0, market_cap=0, volume_24h=0)

    async def _get_bscscan_price(self, token_address: str) -> TokenPriceData:
        """Holt Token-Preis von BscScan mit korrektem Rate-Limiting"""
        try:
            # Rate-Limiting mit korrektem acquire-Aufruf
            max_req, window = self.API_LIMITS["BscScan"]
            while not await self.rate_limiter.acquire("BscScan", max_req, window):
                wait_time = self.rate_limiter.get_wait_time("BscScan", max_req, window)
                logger.warning(f"Rate limit reached for BscScan, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

            # BscScan API
            url = "https://api.bscscan.com/api"
            params = {
                'module': 'token',
                'action': 'tokenprice',
                'contractaddress': token_address,
                'apikey': os.getenv('BSCSCAN_API_KEY', 'YourApiKeyToken')
            }

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get('status') == '1' and data.get('result'):
                    result = data['result']
                    return TokenPriceData(
                        price=float(result.get('bnbusd', 0)),
                        market_cap=0,  # Nicht verfügbar
                        volume_24h=0,  # Nicht verfügbar
                        price_change_percentage_24h=0,  # Nicht verfügbar
                        source="BscScan"
                    )
        except Exception as e:
            logger.error(f"Error fetching from BscScan: {e}")

        return TokenPriceData(price=0, market_cap=0, volume_24h=0)

    async def _get_pancakeswap_price(self, token_address: str) -> TokenPriceData:
        """Holt Token-Preis von PancakeSwap mit korrektem Rate-Limiting"""
        try:
            # Rate-Limiting mit korrektem acquire-Aufruf
            max_req, window = self.API_LIMITS["PancakeSwap"]
            while not await self.rate_limiter.acquire("PancakeSwap", max_req, window):
                wait_time = self.rate_limiter.get_wait_time("PancakeSwap", max_req, window)
                logger.warning(f"Rate limit reached for PancakeSwap, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

            # PancakeSwap V2 API
            url = "https://api.pancakeswap.info/api/v2/tokens/" + token_address

            async with self.session.get(url) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get('data'):
                    token_data = data['data']
                    return TokenPriceData(
                        price=float(token_data.get('price_BNB', 0)),
                        market_cap=0,  # Nicht verfügbar
                        volume_24h=0,  # Nicht verfügbar
                        price_change_percentage_24h=0,  # Nicht verfügbar
                        source="PancakeSwap"
                    )
        except Exception as e:
            logger.error(f"Error fetching from PancakeSwap: {e}")

        return TokenPriceData(price=0, market_cap=0, volume_24h=0)

    async def _get_solana_token_price(self, token_address: str) -> TokenPriceData:
        """Preis für Solana Token"""
        # Prüfe Cache
        cache_key = f"solana:{token_address}"
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            logger.debug(f"Using cached data for Solana token {token_address}")
            return cached_data

        # Zuerst mit CoinGecko versuchen
        if self.api_key_valid:
            try:
                token_data = await self._get_from_coingecko(token_address, 'solana')
                if token_data:
                    # Speichere im Cache
                    self._save_to_cache(cache_key, token_data)
                    return token_data
            except Exception as e:
                logger.error(f"Error fetching from CoinGecko: {e}")

        # Fallback: Jupiter API
        logger.warning(f"Falling back to Jupiter API for {token_address}")
        return await self._get_solana_price_jupiter(token_address)

    async def _get_solana_price_jupiter(self, token_address: str) -> TokenPriceData:
        """Fallback: Jupiter API für Solana Token Preise mit korrektem Rate-Limiting"""
        try:
            # Rate-Limiting mit korrektem acquire-Aufruf
            max_req, window = self.API_LIMITS["Jupiter"]
            while not await self.rate_limiter.acquire("Jupiter", max_req, window):
                wait_time = self.rate_limiter.get_wait_time("Jupiter", max_req, window)
                logger.warning(f"Rate limit reached for Jupiter, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

            url = f"https://price.jup.ag/v4/price"
            params = {'ids': token_address}

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                token_data = data.get('data', {}).get(token_address, {})

                if token_data:
                    result = TokenPriceData(
                        price=token_data.get('price', 0),
                        market_cap=0,  # Jupiter API bietet keine Market Cap
                        volume_24h=0,
                        source="Jupiter"
                    )
                    # Speichere im Cache
                    self._save_to_cache(f"solana:{token_address}", result)
                    return result
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching Solana price from Jupiter: {e}")

        return TokenPriceData(price=0, market_cap=0, volume_24h=0)

    async def _get_sui_token_price(self, token_address: str) -> TokenPriceData:
        """Preis für Sui Token"""
        # Implementierung für Sui-Preisabfrage
        # Da Sui relativ neu ist, könnte hier eine spezielle API nötig sein
        # Placeholder-Implementierung
        logger.warning(f"Sui token price fetching not fully implemented for {token_address}")
        return TokenPriceData(price=0, market_cap=0, volume_24h=0)

    async def get_low_cap_tokens(self, max_market_cap: float = 5_000_000, limit: int = 250) -> List[Token]:
        """Holt Low-Cap Tokens von CoinGecko"""
        url = f"{self.base_url}/coins/markets"
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': limit,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '24h'
        }

        # Header mit API-Schlüssel vorbereiten
        headers = {}
        if self.api_key_valid:
            headers['x-cg-pro-api-key'] = self.coingecko_api_key
            logger.debug("Using CoinGecko API key for low-cap tokens request")
        else:
            logger.warning("No CoinGecko API key provided for low-cap tokens request")

        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                logger.debug(f"Low-cap tokens response status: {response.status}")

                if response.status == 429:
                    error_text = await response.text()
                    logger.error(f"Rate limit exceeded for low-cap tokens. Response: {error_text}")
                    raise RateLimitExceededException(
                        "CoinGecko",
                        50,  # Annahme: 50 Anfragen pro Minute
                        "minute"
                    )

                response.raise_for_status()
                data = await response.json()

                logger.debug(f"Low-cap tokens response: {len(data)} tokens returned")

                tokens = []
                for coin in data:
                    if coin.get('market_cap') and coin['market_cap'] < max_market_cap:
                        # Prüfen, ob der Token auf einer unterstützten Blockchain läuft
                        chain, address = self._extract_chain_and_address(coin)
                        if chain and address:
                            token = Token(
                                address=address,
                                name=coin['name'],
                                symbol=coin['symbol'].upper(),
                                market_cap=coin.get('market_cap', 0),
                                volume_24h=coin.get('total_volume', 0),
                                liquidity=0,  # Wird später berechnet
                                holders_count=0,  # Wird später geholt
                                contract_verified=False,  # Wird später geprüft
                                creation_date=None,  # Placeholder, needs fetching
                                chain=chain
                            )
                            tokens.append(token)

                logger.info(f"Found {len(tokens)} low-cap tokens")
                return tokens

        except aiohttp.ClientError as e:
            logger.error(f"Error fetching low-cap tokens: {e}")
            raise APIException(f"Failed to fetch low-cap tokens: {str(e)}")

    def _extract_chain_and_address(self, coin_data: Dict[str, Any]) -> tuple:
        """Extrahiert Blockchain und Adresse aus CoinGecko-Daten"""
        platforms = coin_data.get('platforms', {})

        # Ethereum hat Priorität
        if 'ethereum' in platforms and platforms['ethereum']:
            return 'ethereum', platforms['ethereum']

        # Dann BSC
        if 'binance-smart-chain' in platforms and platforms['binance-smart-chain']:
            return 'bsc', platforms['binance-smart-chain']

        # Dann Solana
        if 'solana' in platforms and platforms['solana']:
            return 'solana', platforms['solana']

        # Dann Sui
        if 'sui' in platforms and platforms['sui']:
            return 'sui', platforms['sui']

        return None, None
