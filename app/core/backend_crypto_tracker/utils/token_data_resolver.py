import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio

from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData
from app.core.backend_crypto_tracker.processor.database.models.token import Token

logger = logging.getLogger(__name__)

class TokenDataResolver:
    """Klasse zur Auflösung von Token-Adressen zu Token-Daten"""
    
    def __init__(self, api_manager):
        self.api_manager = api_manager
        self.token_cache = {}  # Einfacher Cache für Token-Daten
        
    async def resolve_token_data(self, token_address: str, chain: str) -> Optional[Token]:
        """
        Löst eine Token-Adresse zu Token-Daten auf
        
        Args:
            token_address: Die Token-Adresse
            chain: Die Blockchain
            
        Returns:
            Token-Objekt oder None wenn nicht gefunden
        """
        try:
            # Cache-Schlüssel
            cache_key = f"{chain}:{token_address}"
            
            # Prüfe Cache
            if cache_key in self.token_cache:
                logger.info(f"Returning cached token data for {token_address}")
                return self.token_cache[cache_key]
            
            # Versuche, Token-Daten von verschiedenen Quellen zu erhalten
            token_data = await self._resolve_from_multiple_sources(token_address, chain)
            
            if token_data:
                # Speichere im Cache
                self.token_cache[cache_key] = token_data
                return token_data
            else:
                logger.warning(f"No token data found for {token_address} on {chain}")
                return self._create_unknown_token(token_address, chain)
                
        except Exception as e:
            logger.error(f"Error resolving token data for {token_address}: {e}")
            return self._create_unknown_token(token_address, chain)
    
    async def _resolve_from_multiple_sources(self, token_address: str, chain: str) -> Optional[Token]:
        """Versucht, Token-Daten von verschiedenen Quellen zu erhalten"""
        
        # Quelle 1: Direkte Preisabfrage
        price_data = await self._try_get_price_data(token_address, chain)
        if price_data and self._is_valid_price_data(price_data):
            return self._create_token_from_price_data(price_data, token_address, chain)
        
        # Quelle 2: Token-Metadaten
        metadata = await self._try_get_token_metadata(token_address, chain)
        if metadata and self._is_valid_metadata(metadata):
            return self._create_token_from_metadata(metadata, token_address, chain)
        
        # Quelle 3: On-Chain Daten
        onchain_data = await self._try_get_onchain_data(token_address, chain)
        if onchain_data:
            return self._create_token_from_onchain_data(onchain_data, token_address, chain)
        
        return None
    
    async def _try_get_price_data(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Versucht, Preisdaten für den Token von verschiedenen Quellen zu erhalten"""
        try:
            # Zuerst CoinMarketCap versuchen (höheres Rate-Limit: 333/Minute vs. 10/Minute)
            if hasattr(self.api_manager, 'coinmarketcap_provider'):
                logger.info(f"Trying to get price data from CoinMarketCap for {token_address}")
                price_data = await self.api_manager.coinmarketcap_provider.get_token_price_by_address(token_address, chain)
                if price_data and self._is_valid_price_data(price_data):
                    logger.info(f"Successfully got price data from CoinMarketCap for {token_address}")
                    return price_data
            
            # Dann CoinGecko versuchen (als Fallback)
            if hasattr(self.api_manager, 'coingecko_provider'):
                logger.info(f"Trying to get price data from CoinGecko for {token_address}")
                price_data = await self.api_manager.coingecko_provider.get_token_price(token_address, chain)
                if price_data and self._is_valid_price_data(price_data):
                    logger.info(f"Successfully got price data from CoinGecko for {token_address}")
                    return price_data
            
            # Wenn keine der Quellen funktioniert hat, gebe None zurück
            logger.warning(f"No price data found for {token_address} on {chain}")
            return None
        except Exception as e:
            logger.warning(f"Error getting price data for {token_address}: {e}")
            return None
    
    async def _try_get_token_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Versucht, Token-Metadaten zu erhalten"""
        try:
            # Prüfe, ob api_manager ein providers-Attribut hat
            if not hasattr(self.api_manager, 'providers'):
                logger.warning(f"api_manager has no 'providers' attribute")
                
                # VERSUCHE ALTERNATIVE METHODEN
                # Methode 1: Direkter Aufruf von CoinGecko über api_manager
                if hasattr(self.api_manager, 'get_coingecko_metadata'):
                    logger.info(f"Trying direct coingecko metadata call")
                    metadata = await self.api_manager.get_coingecko_metadata(token_address, chain)
                    if metadata and metadata.get('name') != 'Unknown':
                        return metadata
                
                # Methode 2: Prüfe, ob es eine get_provider-Methode gibt
                elif hasattr(self.api_manager, 'get_provider'):
                    logger.info(f"Trying get_provider method")
                    provider = self.api_manager.get_provider('coingecko')
                    if provider and hasattr(provider, 'get_token_metadata'):
                        metadata = await provider.get_token_metadata(token_address, chain)
                        if metadata and metadata.get('name') != 'Unknown':
                            return metadata
                
                # Methode 3: Prüfe, ob es separate Provider-Attribute gibt
                elif hasattr(self.api_manager, 'coingecko_provider'):
                    logger.info(f"Trying direct coingecko_provider attribute")
                    provider = self.api_manager.coingecko_provider
                    if hasattr(provider, 'get_token_metadata'):
                        metadata = await provider.get_token_metadata(token_address, chain)
                        if metadata and metadata.get('name') != 'Unknown':
                            return metadata
                
                # Keine alternative Methode gefunden
                return None
                
            # Original-Code, falls providers-Attribut existiert
            else:
                providers = ['coingecko', 'coinmarketcap']
                for provider_name in providers:
                    if provider_name in self.api_manager.providers:
                        provider = self.api_manager.providers[provider_name]
                        if hasattr(provider, 'get_token_metadata'):
                            metadata = await provider.get_token_metadata(token_address, chain)
                            if metadata and metadata.get('name') != 'Unknown':
                                return metadata
                return None
                
        except Exception as e:
            logger.warning(f"Error getting token metadata for {token_address}: {e}")
            return None
    
    async def _try_get_onchain_data(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Versucht, On-Chain-Daten für den Token zu erhalten"""
        try:
            # Dies würde eine separate On-Chain-Analyse erfordern
            # Für jetzt geben wir None zurück
            return None
        except Exception as e:
            logger.warning(f"Error getting onchain data for {token_address}: {e}")
            return None
    
    # === ANGEPASSTE METHODE - LÖSUNG 1 ===
    def _is_valid_price_data(self, price_data: TokenPriceData) -> bool:
        """Prüft, ob die Preisdaten gültig sind"""
        if not price_data:
            return False
        
        # Prüfe nur, ob der Preis sinnvoll ist
        if price_data.price <= 0:
            return False
        
        # Name und Symbol sind optional - entferne diese Prüfungen
        return True
    
    def _is_valid_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Prüft, ob die Metadaten gültig sind"""
        if not metadata or not isinstance(metadata, dict):
            return False
        
        # Prüfe, ob Name vorhanden ist
        name = metadata.get('name')
        if not name or name == 'Unknown':
            return False
        
        return True
    
    def _create_token_from_price_data(self, price_data: TokenPriceData, token_address: str, chain: str) -> Token:
        """Erstellt ein Token-Objekt aus Preisdaten"""
        return Token(
            address=token_address,
            name=getattr(price_data, 'name', 'Unknown'),  # Standardwert 'Unknown'
            symbol=getattr(price_data, 'symbol', 'UNKNOWN'),  # Standardwert 'UNKNOWN'
            chain=chain,
            market_cap=getattr(price_data, 'market_cap', 0),
            volume_24h=getattr(price_data, 'volume_24h', 0),
            liquidity=0,  # Wird später berechnet
            holders_count=0,  # Wird später geholt
            contract_verified=False,  # Wird später geprüft
            creation_date=None,  # Wird später geholt
            token_score=0  # Wird später berechnet
        )
    
    def _create_token_from_metadata(self, metadata: Dict[str, Any], token_address: str, chain: str) -> Token:
        """Erstellt ein Token-Objekt aus Metadaten"""
        return Token(
            address=token_address,
            name=metadata.get('name', 'Unknown'),
            symbol=metadata.get('symbol', 'UNKNOWN'),
            chain=chain,
            market_cap=0,
            volume_24h=0,
            liquidity=0,
            holders_count=0,
            contract_verified=False,
            creation_date=None,
            token_score=0
        )
    
    def _create_token_from_onchain_data(self, onchain_data: Dict[str, Any], token_address: str, chain: str) -> Token:
        """Erstellt ein Token-Objekt aus On-Chain-Daten"""
        return Token(
            address=token_address,
            name=onchain_data.get('name', 'Unknown'),
            symbol=onchain_data.get('symbol', 'UNKNOWN'),
            chain=chain,
            market_cap=0,
            volume_24h=0,
            liquidity=0,
            holders_count=0,
            contract_verified=onchain_data.get('contract_verified', False),
            creation_date=onchain_data.get('creation_date'),
            token_score=0
        )
    
    def _create_unknown_token(self, token_address: str, chain: str) -> Token:
        """Erstellt ein Token-Objekt für unbekannte Tokens"""
        return Token(
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
