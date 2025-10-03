from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_total_supply(provider, coin_type: str) -> Optional[Dict[str, Any]]:
    """Holt die Gesamtmenge eines Coins"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getTotalSupply',
            'params': [coin_type]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            return {
                'coin_type': coin_type,
                'total_supply': data['result'].get('value'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui total supply: {e}")
    
    return None
