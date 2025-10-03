from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_account_objects(provider, address: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """Holt alle Objekte einer Sui-Adresse"""
    try:
        url = f"{provider.base_url}"
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getObjectsOwnedByAddress',
            'params': [address]
        }
        
        data = await provider._make_post_request(url, params)
        
        if data and data.get('result'):
            objects = data['result']
            object_list = []
            
            for obj_id in objects[:limit]:  # Limitieren
                from .get_object_info import execute_get_object_info
                obj_info = await execute_get_object_info(provider, obj_id)
                if obj_info:
                    object_list.append(obj_info)
            
            return object_list
    except Exception as e:
        logger.error(f"Error fetching Sui account objects: {e}")
    
    return None
