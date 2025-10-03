from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_coin_info(provider, coin_type: str) -> Optional[Dict[str, Any]]:
    """Holt detaillierte Coin-Informationen"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getCoinMetadata',
            'params': [coin_type]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            metadata = data['result']
            return {
                'coin_type': coin_type,
                'name': metadata.get('name'),
                'symbol': metadata.get('symbol'),
                'description': metadata.get('description'),
                'icon_url': metadata.get('iconUrl'),
                'decimals': metadata.get('decimals'),
                'total_supply': metadata.get('totalSupply'),
                'exponent': metadata.get('exponent'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui coin info: {e}")
    
    return None
