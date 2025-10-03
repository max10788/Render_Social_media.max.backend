from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_contract_abi(provider, contract_address: str) -> Optional[Dict[str, Any]]:
    """Holt das ABI eines Smart Contracts"""
    try:
        params = {
            'module': 'contract',
            'action': 'getabi',
            'address': contract_address,
            'apikey': provider.api_key
        }
        
        data = await provider._make_request(provider.base_url, params)
        
        if data and data.get('status') == '1':
            return {
                'contract_address': contract_address,
                'abi': data.get('result'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Ethereum contract ABI: {e}")
    
    return None
