from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_dynamic_fields(provider, parent_object_id: str) -> Optional[List[Dict[str, Any]]]:
    """Holt dynamische Felder für ein Objekt"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getDynamicFields',
            'params': [parent_object_id]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            fields = []
            for field in data['result'].get('data', []):
                fields.append({
                    'parentId': field.get('parentId'),
                    'name': field.get('name'),
                    'fieldType': field.get('fieldType'),
                    'objectId': field.get('objectId'),
                    'last_updated': datetime.now()
                })
            
            return fields
    except Exception as e:
        logger.error(f"Error fetching Sui dynamic fields: {e}")
    
    return None
