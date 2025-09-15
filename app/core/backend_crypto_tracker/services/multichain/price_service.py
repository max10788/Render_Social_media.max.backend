# services/multichain/price_service.py
import aiohttp
import logging
import os
import time
import json
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, RateLimitExceededException
from app.core.backend_crypto_tracker.config.blockchain_api_keys import chain_config
from app.core.backend_crypto_tracker.processor.database.models.token import Token

logger = get_logger(__name__)

@dataclass
class TokenPriceData:
    price: float
    market_cap: float
    volume_24h: float
    price_change_percentage_24h: Optional[float] = None

class PriceService:
    def __init__(self, coingecko_api_key: Optional[str] = None):
        # Wenn kein API-Schlüssel übergeben wird, versuche, ihn aus den Umgebungsvariablen zu lesen
        self.coingecko_api_key = coingecko_api_key or os.getenv('COINGECKO_API_KEY')
        self.session = None
        self.api_key_valid = False
        self.cache = {}  # Einfacher Cache für Token-Preise
        self.cache_expiry = 300  # 5 Minuten Cache
        
        # Standardmäßig öffentliche API verwenden
        self.base_url = "https://api.coingecko.com/api/v3"
        
        # Protokolliere den Status des API-Schlüssels (maskiert für Sicherheit)
        if self.coingecko_api_key:
            # Zeige nur die ersten 4 und letzten 4 Zeichen des Schlüssels
            if len(self.coingecko_api_key) > 8:
                masked_key = self.coingecko_api_key[:4] + "..." + self.coingecko_api_key[-4:]
            else:
                masked_key = "***"
            logger.info(f"CoinGecko API key configured: {masked_key}")
        else:
            logger.warning("No CoinGecko API key configured - using public API with rate limits")
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        # Validiere den API-Schlüssel beim Start
        await self._validate_api_key()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
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
                    self.base_url = "https://api.coingecko.com/api/v3"
                    logger.info("CoinGecko API key is valid with public API")
                else:
                    error_text = await response.text()
                    logger.error(f"CoinGecko API key validation failed with public API: {response.status} - {error_text}")
                    
                    # Pro-API testen
                    url = "https://pro-api.coingecko.com/api/v3/ping"
                    async with self.session.get(url, headers=headers) as response2:
                        if response2.status == 200:
                            self.api_key_valid = True
                            self.base_url = "https://pro-api.coingecko.com/api/v3"
                            logger.info("CoinGecko API key is valid with Pro API")
                        else:
                            error_text2 = await response2.text()
                            logger.error(f"CoinGecko API key validation failed with Pro API: {response2.status} - {error_text2}")
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
    
    async def get_token_price(self, token_address: str, chain: str) -> TokenPriceData:
        """Holt Preisinformationen für ein Token basierend auf der Blockchain"""
        if not chain_config.is_supported(chain):
            raise ValueError(f"Unsupported chain: {chain}")
        
        # Prüfe Cache
        cache_key = f"{chain}:{token_address}"
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            logger.debug(f"Using cached data for {token_address}")
            return cached_data
        
        if chain in ['ethereum', 'bsc']:
            return await self._get_evm_token_price(token_address, chain)
        elif chain == 'solana':
            return await self._get_solana_token_price(token_address)
        elif chain == 'sui':
            return await self._get_sui_token_price(token_address)
        else:
            return TokenPriceData(price=0, market_cap=0, volume_24h=0)
    
    async def _get_evm_token_price(self, token_address: str, chain: str) -> TokenPriceData:
        """Preis für EVM-basierte Token (Ethereum, BSC)"""
        platform_id = 'ethereum' if chain == 'ethereum' else 'binance-smart-chain'
        
        # Zuerst mit CoinGecko versuchen
        if self.api_key_valid:
            try:
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
        """Holt Tokendaten von CoinGecko"""
        # Verwende die korrekte Basis-URL
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
                    raise RateLimitExceededException("CoinGecko", 50, "minute")
                
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
        """Holt Tokendaten von alternativen Quellen für EVM-Chains"""
        # Für Ethereum können wir DEX APIs wie Uniswap oder 1inch verwenden
        # Für BSC können wir PancakeSwap verwenden
        
        if chain == 'ethereum':
            # Versuche, den Preis von Uniswap zu holen
            return await self._get_uniswap_price(token_address)
        elif chain == 'bsc':
            # Versuche, den Preis von PancakeSwap zu holen
            return await self._get_pancakeswap_price(token_address)
        
        return TokenPriceData(price=0, market_cap=0, volume_24h=0)
    
    async def _get_uniswap_price(self, token_address: str) -> TokenPriceData:
        """Holt Token-Preis von Uniswap"""
        try:
            # Uniswap V3 API
            url = "https://api.uniswap.org/v1/quote"
            params = {
                'tokenIn': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',  # WETH
                'tokenOut': token_address,
                'amount': '1000000000000000000',  # 1 WETH
                'type': 'exactIn'
            }
            
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                
                if data.get('quote'):
                    # Berechne den Preis basierend auf dem Quote
                    price = float(data['quote']) / 1000000000000000000  # WETH to token
                    return TokenPriceData(
                        price=price,
                        market_cap=0,  # Nicht verfügbar
                        volume_24h=0,  # Nicht verfügbar
                        price_change_percentage_24h=0  # Nicht verfügbar
                    )
        except Exception as e:
            logger.error(f"Error fetching from Uniswap: {e}")
        
        # Fallback: Etherscan
        return await self._get_etherscan_price(token_address)
    
    async def _get_etherscan_price(self, token_address: str) -> TokenPriceData:
        """Holt Token-Preis von Etherscan"""
        try:
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
                        price_change_percentage_24h=0  # Nicht verfügbar
                    )
        except Exception as e:
            logger.error(f"Error fetching from Etherscan: {e}")
        
        return TokenPriceData(price=0, market_cap=0, volume_24h=0)
    
    async def _get_pancakeswap_price(self, token_address: str) -> TokenPriceData:
        """Holt Token-Preis von PancakeSwap"""
        try:
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
                        price_change_percentage_24h=0  # Nicht verfügbar
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
        """Fallback: Jupiter API für Solana Token Preise"""
        url = f"https://price.jup.ag/v4/price"
        params = {'ids': token_address}
        
        try:
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                token_data = data.get('data', {}).get(token_address, {})
                
                if token_data:
                    result = TokenPriceData(
                        price=token_data.get('price', 0),
                        market_cap=0,  # Jupiter API bietet keine Market Cap
                        volume_24h=0
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
