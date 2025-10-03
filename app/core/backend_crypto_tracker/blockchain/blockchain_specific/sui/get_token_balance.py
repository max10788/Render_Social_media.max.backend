from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_balance(provider, address: str, coin_type: str) -> Optional[Dict[str, Any]]:
    """Holt den Token-Balance für eine Adresse und einen Coin-Typ"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getBalance',
            'params': [address, coin_type]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            balance_info = data['result']
            return {
                'address': address,
                'coin_type': coin_type,
                'balance': int(balance_info.get('totalBalance', 0)),
                'coin_object_count': balance_info.get('coinObjectCount', 0),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui token balance: {e}")
    
    return None
