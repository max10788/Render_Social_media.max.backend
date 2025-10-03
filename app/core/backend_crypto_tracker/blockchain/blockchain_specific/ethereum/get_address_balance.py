from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_address_balance(provider, address: str) -> Optional[Dict[str, Any]]:
    """Holt den Kontostand einer Ethereum-Adresse"""
    try:
        params = {
            'module': 'account',
            'action': 'balance',
            'address': address,
            'tag': 'latest',
            'apikey': provider.api_key
        }
        
        data = await provider._make_request(provider.base_url, params)
        
        if data and data.get('status') == '1':
            balance_wei = int(data.get('result', 0))
            return {
                'address': address,
                'balance': balance_wei / 10**18,  # Wei zu ETH
                'balance_wei': balance_wei,
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Ethereum address balance: {e}")
    
    return None
