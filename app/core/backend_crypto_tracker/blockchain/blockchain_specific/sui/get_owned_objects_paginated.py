from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_owned_objects_paginated(
    provider, 
    address: str, 
    cursor: Optional[str] = None, 
    limit: int = 50
) -> Optional[Dict[str, Any]]:
    """Holt alle Objekte, die einer Adresse gehören (mit Pagination)"""
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
                    },
                    'cursor': cursor,
                    'limit': limit
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
            
            return {
                'data': objects,
                'has_next_page': data['result'].get('hasNextPage', False),
                'next_cursor': data['result'].get('nextCursor'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui owned objects paginated: {e}")
    
    return None
