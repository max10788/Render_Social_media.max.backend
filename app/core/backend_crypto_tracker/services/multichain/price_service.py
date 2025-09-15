# services/multichain/price_service.py
import aiohttp
import logging
import os
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
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_token_price(self, token_address: str, chain: str) -> TokenPriceData:
        """Holt Preisinformationen für ein Token basierend auf der Blockchain"""
        if not chain_config.is_supported(chain):
            raise ValueError(f"Unsupported chain: {chain}")
        
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
        
        # URL für die API-Anfrage
        url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform_id}"
        
        # Parameter für die API-Anfrage
        params = {
            'contract_addresses': token_address,
            'vs_currencies': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true'
        }
        
        # Header mit API-Schlüssel vorbereiten
        headers = {}
        if self.coingecko_api_key:
            headers['x-cg-pro-api-key'] = self.coingecko_api_key
            # Protokolliere den Header (maskiere den API-Schlüssel)
            masked_headers = dict(headers)
            if 'x-cg-pro-api-key' in masked_headers:
                key = masked_headers['x-cg-pro-api-key']
                if len(key) > 8:
                    masked_headers['x-cg-pro-api-key'] = key[:4] + "..." + key[-4:]
                else:
                    masked_headers['x-cg-pro-api-key'] = "***"
            logger.debug(f"Using headers for CoinGecko API: {masked_headers}")
        else:
            logger.warning(f"No CoinGecko API key provided for {token_address}")
        
        try:
            # Protokolliere die vollständige Anfrage
            logger.debug(f"Making request to: {url}")
            logger.debug(f"Request params: {params}")
            
            async with self.session.get(url, params=params, headers=headers) as response:
                # Protokolliere den Antwortstatus
                logger.debug(f"Response status: {response.status}")
                
                # Bei Rate-Limit, protokolliere die Antwort
                if response.status == 429:
                    error_text = await response.text()
                    logger.error(f"Rate limit exceeded. Response: {error_text}")
                    raise RateLimitExceededException(
                        "CoinGecko", 
                        50,  # Annahme: 50 Anfragen pro Minute
                        "minute"
                    )
                
                response.raise_for_status()
                data = await response.json()
                
                # Protokolliere die Antwort (gekürzt)
                logger.debug(f"CoinGecko response (truncated): {str(data)[:200]}...")
                
                # Überprüfe, ob die Antwort die erwarteten Daten enthält
                if not data:
                    logger.error(f"Empty response from CoinGecko for token {token_address}")
                    raise ValueError(f"No data returned from CoinGecko for token {token_address}")
                
                # Extrahiere die Tokendaten
                token_data = data.get(token_address.lower(), {})
                if not token_data:
                    logger.error(f"No token data found in response for {token_address}. Response: {data}")
                    raise ValueError(f"Token data not found for {token_address}")
                
                return TokenPriceData(
                    price=token_data.get('usd', 0),
                    market_cap=token_data.get('usd_market_cap', 0),
                    volume_24h=token_data.get('usd_24h_vol', 0),
                    price_change_percentage_24h=token_data.get('usd_24h_change')
                )
                
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching EVM token price: {e}")
            # Versuche, die Fehlerantwort zu protokollieren
            if hasattr(e, 'response') and e.response:
                try:
                    error_response = await e.response.text()
                    logger.error(f"Error response: {error_response}")
                except:
                    pass
            raise APIException(f"Failed to fetch token price: {str(e)}")
        
        return TokenPriceData(price=0, market_cap=0, volume_24h=0)
    
    async def _get_solana_token_price(self, token_address: str) -> TokenPriceData:
        """Preis für Solana Token"""
        # CoinGecko unterstützt auch Solana Token
        url = f"https://api.coingecko.com/api/v3/simple/token_price/solana"
        params = {
            'contract_addresses': token_address,
            'vs_currencies': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true'
        }
        
        # Header mit API-Schlüssel vorbereiten
        headers = {}
        if self.coingecko_api_key:
            headers['x-cg-pro-api-key'] = self.coingecko_api_key
            logger.debug(f"Using CoinGecko API key for Solana token {token_address}")
        else:
            logger.warning(f"No CoinGecko API key provided for Solana token {token_address}")
        
        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                logger.debug(f"Solana token response status: {response.status}")
                
                if response.status == 429:
                    error_text = await response.text()
                    logger.error(f"Rate limit exceeded for Solana token. Response: {error_text}")
                    raise RateLimitExceededException(
                        "CoinGecko", 
                        50,  # Annahme: 50 Anfragen pro Minute
                        "minute"
                    )
                
                response.raise_for_status()
                data = await response.json()
                
                logger.debug(f"Solana token response (truncated): {str(data)[:200]}...")
                
                token_data = data.get(token_address, {})
                return TokenPriceData(
                    price=token_data.get('usd', 0),
                    market_cap=token_data.get('usd_market_cap', 0),
                    volume_24h=token_data.get('usd_24h_vol', 0),
                    price_change_percentage_24h=token_data.get('usd_24h_change')
                )
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching Solana token price: {e}")
            # Fallback: Jupiter API für Solana Preise
            return await self._get_solana_price_jupiter(token_address)
        
        return TokenPriceData(price=0, market_cap=0, volume_24h=0)
    
    async def _get_solana_price_jupiter(self, token_address: str) -> TokenPriceData:
        """Fallback: Jupiter API für Solana Token Preise"""
        url = f"https://price.jup.ag/v4/price"
        params = {'ids': token_address}
        
        try:
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                token_data = data.get('data', {}).get(token_address, {})
                return TokenPriceData(
                    price=token_data.get('price', 0),
                    market_cap=0,  # Jupiter API bietet keine Market Cap
                    volume_24h=0
                )
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching Solana price from Jupiter: {e}")
            raise APIException(f"Failed to fetch token price from Jupiter: {str(e)}")
    
    async def _get_sui_token_price(self, token_address: str) -> TokenPriceData:
        """Preis für Sui Token"""
        # Implementierung für Sui-Preisabfrage
        # Da Sui relativ neu ist, könnte hier eine spezielle API nötig sein
        # Placeholder-Implementierung
        logger.warning(f"Sui token price fetching not fully implemented for {token_address}")
        return TokenPriceData(price=0, market_cap=0, volume_24h=0)
    
    async def get_low_cap_tokens(self, max_market_cap: float = 5_000_000, limit: int = 250) -> List[Token]:
        """Holt Low-Cap Tokens von CoinGecko"""
        url = "https://api.coingecko.com/api/v3/coins/markets"
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
        if self.coingecko_api_key:
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
