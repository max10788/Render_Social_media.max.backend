from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_account_balance(provider, address: str) -> Optional[Dict[str, Any]]:
    """Holt den SOL-Kontostand einer Adresse"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getBalance',
            'params': [address]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            balance_info = data['result']
            return {
                'address': address,
                'balance': balance_info.get('value', 0) / 10**9,  # Lamports zu SOL
                'balance_lamports': balance_info.get('value', 0),
                'context': balance_info.get('context'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Solana account balance: {e}")
    
    return None
