"""
Token Data Resolver - Refactored to use new blockchain data system
Resolves token addresses to token data using direct function imports
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

# Import data models
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData
from app.core.backend_crypto_tracker.processor.database.models.token import Token

# Import aggregator functions
from app.core.backend_crypto_tracker.blockchain.aggregators.coingecko.get_token_market_data import get_token_market_data as coingecko_get_market_data
from app.core.backend_crypto_tracker.blockchain.aggregators.coinmarketcap.get_token_quote import get_token_quote as coinmarketcap_get_quote

# Import blockchain-specific functions
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_token_price import get_token_price as ethereum_get_price
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_token_price import get_token_price as solana_get_price
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_token_metadata import get_token_metadata as solana_get_metadata
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_token_price import get_token_price as sui_get_price
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_coin_metadata import get_coin_metadata as sui_get_metadata

logger = logging.getLogger(__name__)


class TokenDataResolver:
    """
    Klasse zur Auflösung von Token-Adressen zu Token-Daten
    Refactored to use direct function imports instead of api_manager
    """
    
    def __init__(self):
        """Initialisiert den TokenDataResolver ohne api_manager"""
        self.token_cache = {}  # Einfacher Cache für Token-Daten
        logger.info("TokenDataResolver initialisiert mit neuem Blockchain-Daten-System")
        
    async def resolve_token_data(self, token_address: str, chain: str) -> Optional[Token]:
        """
        Löst eine Token-Adresse zu Token-Daten auf
        
        Args:
            token_address: Die Token-Adresse
            chain: Die Blockchain (ethereum, bsc, solana, sui)
            
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
            
            # Normalisiere Chain-Name
            chain = chain.lower()
            
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
        """
        Versucht, Token-Daten von verschiedenen Quellen zu erhalten
        Neue Implementierung mit direkten Funktionsaufrufen
        """
        
        # Quelle 1: Chain-spezifische Preisabfrage
        price_data = await self._try_get_price_data(token_address, chain)
        if price_data and self._is_valid_price_data(price_data):
            return self._create_token_from_price_data(price_data, token_address, chain)
        
        # Quelle 2: Token-Metadaten (Aggregatoren)
        metadata = await self._try_get_token_metadata(token_address, chain)
        if metadata and self._is_valid_metadata(metadata):
            return self._create_token_from_metadata(metadata, token_address, chain)
        
        # Quelle 3: Chain-spezifische Metadaten
        chain_metadata = await self._try_get_chain_metadata(token_address, chain)
        if chain_metadata and self._is_valid_metadata(chain_metadata):
            return self._create_token_from_metadata(chain_metadata, token_address, chain)
        
        return None
    
    async def _try_get_price_data(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """
        Versucht, Preisdaten für den Token zu erhalten
        Neue Implementierung mit direkten Chain-spezifischen Funktionen
        """
        try:
            logger.info(f"Trying to get price data for {token_address} on {chain}")
            
            # Chain-spezifische Preisabfrage
            if chain in ['ethereum', 'bsc']:
                price_data = await ethereum_get_price(token_address)
            elif chain == 'solana':
                price_data = await solana_get_price(token_address)
            elif chain == 'sui':
                price_data = await sui_get_price(token_address)
            else:
                logger.warning(f"Unsupported chain: {chain}")
                return None
            
            if price_data and self._is_valid_price_data(price_data):
                logger.info(f"Successfully got price data for {token_address} on {chain}")
                return price_data
            
            logger.warning(f"No valid price data found for {token_address} on {chain}")
            return None
            
        except Exception as e:
            logger.warning(f"Error getting price data for {token_address} on {chain}: {e}")
            return None
    
    async def _try_get_token_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """
        Versucht, Token-Metadaten von Aggregatoren zu erhalten
        Priorisiert CoinMarketCap (höheres Rate-Limit)
        """
        try:
            logger.info(f"Trying to get token metadata from aggregators for {token_address}")
            
            # Versuch 1: CoinMarketCap (höheres Rate-Limit: 333/min)
            try:
                metadata = await coinmarketcap_get_quote(token_address, chain)
                if metadata and metadata.get('name') != 'Unknown':
                    logger.info(f"Successfully got metadata from CoinMarketCap for {token_address}")
                    return metadata
            except Exception as e:
                logger.debug(f"CoinMarketCap metadata fetch failed: {e}")
            
            # Versuch 2: CoinGecko (Fallback)
            try:
                metadata = await coingecko_get_market_data(token_address, chain)
                if metadata and metadata.get('name') != 'Unknown':
                    logger.info(f"Successfully got metadata from CoinGecko for {token_address}")
                    return metadata
            except Exception as e:
                logger.debug(f"CoinGecko metadata fetch failed: {e}")
            
            logger.warning(f"No metadata found from aggregators for {token_address}")
            return None
            
        except Exception as e:
            logger.warning(f"Error getting token metadata for {token_address}: {e}")
            return None
    
    async def _try_get_chain_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """
        Versucht, Chain-spezifische Metadaten zu erhalten
        """
        try:
            logger.info(f"Trying to get chain-specific metadata for {token_address} on {chain}")
            
            if chain == 'solana':
                metadata = await solana_get_metadata(token_address)
                if metadata and metadata.get('name') != 'Unknown':
                    logger.info(f"Successfully got Solana metadata for {token_address}")
                    return metadata
            
            elif chain == 'sui':
                metadata = await sui_get_metadata(token_address)
                if metadata and metadata.get('name') != 'Unknown':
                    logger.info(f"Successfully got Sui metadata for {token_address}")
                    return metadata
            
            return None
            
        except Exception as e:
            logger.warning(f"Error getting chain metadata for {token_address} on {chain}: {e}")
            return None
    
    def _is_valid_price_data(self, price_data: TokenPriceData) -> bool:
        """Prüft, ob die Preisdaten gültig sind"""
        if not price_data:
            return False
        
        # Prüfe nur, ob der Preis sinnvoll ist
        if hasattr(price_data, 'price') and price_data.price <= 0:
            return False
        
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
            name=getattr(price_data, 'name', 'Unknown'),
            symbol=getattr(price_data, 'symbol', 'UNKNOWN'),
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
            market_cap=metadata.get('market_cap', 0),
            volume_24h=metadata.get('volume_24h', 0),
            liquidity=0,
            holders_count=0,
            contract_verified=False,
            creation_date=None,
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
    
    def clear_cache(self):
        """Leert den Token-Cache"""
        self.token_cache.clear()
        logger.info("Token cache cleared")
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Gibt Cache-Statistiken zurück"""
        return {
            'cached_tokens': len(self.token_cache)
        }
