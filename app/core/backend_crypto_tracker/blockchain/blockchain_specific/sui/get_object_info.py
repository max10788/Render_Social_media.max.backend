from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_object_info(provider, object_id: str) -> Optional[Dict[str, Any]]:
    """Holt Informationen zu einem Sui-Objekt"""
    try:
        url = f"{provider.base_url}"
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getObject',
            'params': [
                object_id,
                {'showType': True, 'showOwner': True, 'showPreviousTransaction': True}
            ]
        }
        
        data = await provider._make_post_request(url, params)
        
        if data and data.get('result'):
            obj = data['result']
            return {
                'object_id': obj.get('objectId'),
                'type': obj.get('type'),
                'owner': obj.get('owner'),
                'previous_transaction': obj.get('previousTransaction'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui object info: {e}")
    
    return None
