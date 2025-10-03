from datetime import datetime
from typing import Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


async def execute_get_token_price(provider, token_address: str, chain: str) -> Optional[TokenPriceData]:
    """Ethereum-spezifische Token-Preisabfrage"""
    try:
        # Versuche zuerst, den Preis über CoinGecko zu erhalten (genauere Daten)
        if provider.coingecko_provider:
            coingecko_price = await provider.coingecko_provider.get_token_price(token_address, chain)
            if coingecko_price:
                return coingecko_price
            
        # Fallback auf Etherscan
        params = {
            'module': 'stats',
            'action': 'tokenprice',
            'contractaddress': token_address,
            'apikey': provider.api_key
        }
        
        data = await provider._make_request(provider.base_url, params)
        
        # Prüfe, ob die Antwort gültig ist
        if not data or data.get('status') != '1':
            logger.warning(f"Ungültige Antwort von Etherscan für Token {token_address}")
            return None
            
        result = data.get('result', {})
        if not result or not result.get('ethusd'):
            logger.warning(f"Keine Preisdaten von Etherscan für Token {token_address}")
            return None
            
        return TokenPriceData(
            price=float(result.get('ethusd', 0)),
            market_cap=0,  # Nicht verfügbar
            volume_24h=0,  # Nicht verfügbar
            price_change_percentage_24h=0,  # Nicht verfügbar
            source="Etherscan",
            last_updated=datetime.now()
        )
    except Exception as e:
        logger.error(f"Error fetching Ethereum token price: {e}")
    
    return None
