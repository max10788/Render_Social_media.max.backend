from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_coin_supply(provider, coin_type: str) -> Optional[Dict[str, Any]]:
    """Holt die Coin-Supply für einen bestimmten Coin-Typ"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getTotalSupply',
            'params': [coin_type]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            supply_info = data['result']
            return {
                'coin_type': coin_type,
                'total_supply': supply_info.get('value'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui coin supply: {e}")
    
    return None
