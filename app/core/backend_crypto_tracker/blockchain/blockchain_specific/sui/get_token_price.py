from typing import Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


async def execute_get_token_price(provider, token_address: str, chain: str) -> Optional[TokenPriceData]:
    """Sui-spezifische Token-Preisabfrage"""
    try:
        # Sui hat keine native Token-Preis-API, daher leere Implementierung
        # In der Praxis würde man hier eine externe API verwenden
        return None
    except Exception as e:
        logger.error(f"Error fetching Sui token price: {e}")
    
    return None
