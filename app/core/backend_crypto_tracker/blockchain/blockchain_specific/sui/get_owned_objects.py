from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_owned_objects(provider, address: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """Holt alle Objekte, die einer Adresse gehören"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getOwnedObjects',
            'params': [
                address,
                {
                    'options': {
                        'showType': True,
                        'showOwner': True,
                        'showPreviousTransaction': True,
                        'showContent': True
                    }
                }
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            objects = []
            for obj in data['result'].get('data', []):
                objects.append({
                    'object_id': obj.get('objectId'),
                    'type': obj.get('type'),
                    'owner': obj.get('owner'),
                    'content': obj.get('content'),
                    'last_updated': datetime.now()
                })
            
            return objects[:limit]
    except Exception as e:
        logger.error(f"Error fetching Sui owned objects: {e}")
    
    return None
