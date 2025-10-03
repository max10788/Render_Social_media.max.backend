from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_account_balance(provider, address: str) -> Optional[Dict[str, Any]]:
    """Holt den Kontostand einer Sui-Adresse"""
    try:
        url = f"{provider.base_url}"
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getBalance',
            'params': [address]
        }
        
        data = await provider._make_post_request(url, params)
        
        if data and data.get('result'):
            balance_info = data['result']
            return {
                'address': address,
                'balance': int(balance_info.get('totalBalance', 0)) / 10**9,  # MIST zu SUI
                'balance_mist': int(balance_info.get('totalBalance', 0)),
                'coin_object_count': balance_info.get('coinObjectCount', 0),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui account balance: {e}")
    
    return None
