# services/multichain/price_service.py
import aiohttp
import logging
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
        self.coingecko_api_key = coingecko_api_key
        self.session = None
        
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
        
        url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform_id}"
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
        
        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 429:
                    raise RateLimitExceededException(
                        "CoinGecko", 
                        50,  # Annahme: 50 Anfragen pro Minute
                        "minute"
                    )
                
                response.raise_for_status()
                data = await response.json()
                
                token_data = data.get(token_address.lower(), {})
                return TokenPriceData(
                    price=token_data.get('usd', 0),
                    market_cap=token_data.get('usd_market_cap', 0),
                    volume_24h=token_data.get('usd_24h_vol', 0),
                    price_change_percentage_24h=token_data.get('usd_24h_change')
                )
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching EVM token price: {e}")
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
        
        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 429:
                    raise RateLimitExceededException(
                        "CoinGecko", 
                        50,  # Annahme: 50 Anfragen pro Minute
                        "minute"
                    )
                
                response.raise_for_status()
                data = await response.json()
                
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
        
        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 429:
                    raise RateLimitExceededException(
                        "CoinGecko", 
                        50,  # Annahme: 50 Anfragen pro Minute
                        "minute"
                    )
                
                response.raise_for_status()
                data = await response.json()
                
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
