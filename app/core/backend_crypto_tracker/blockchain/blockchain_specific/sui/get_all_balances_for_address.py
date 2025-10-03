from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_all_balances_for_address(provider, address: str) -> Optional[List[Dict[str, Any]]]:
    """Holt alle Token-Balances für eine Adresse"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getAllBalances',
            'params': [address]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            balances = []
            for balance_info in data['result']:
                balances.append({
                    'address': address,
                    'coin_type': balance_info.get('coinType'),
                    'balance': int(balance_info.get('totalBalance', 0)),
                    'coin_object_count': balance_info.get('coinObjectCount', 0),
                    'last_updated': datetime.now()
                })
            
            return balances
    except Exception as e:
        logger.error(f"Error fetching Sui all balances for address: {e}")
    
    return None
