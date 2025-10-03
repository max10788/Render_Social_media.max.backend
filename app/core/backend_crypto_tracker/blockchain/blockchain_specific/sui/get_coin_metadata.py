from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_coin_metadata(provider, coin_type: str) -> Optional[Dict[str, Any]]:
    """Holt Metadaten für einen Coin-Typ"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getObject',
            'params': [
                coin_type,
                {
                    'showType': True,
                    'showOwner': True,
                    'showPreviousTransaction': True,
                    'showContent': True,
                    'showDisplay': True
                }
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            obj = data['result']
            content = obj.get('content', {})
            
            return {
                'coin_type': coin_type,
                'name': content.get('fields', {}).get('name'),
                'symbol': content.get('fields', {}).get('symbol'),
                'decimals': content.get('fields', {}).get('decimals'),
                'description': content.get('fields', {}).get('description'),
                'icon_url': content.get('fields', {}).get('icon_url'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui coin metadata: {e}")
    
    return None
