from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_normalized_move_struct(provider, package_id: str, module: str, struct_name: str) -> Optional[Dict[str, Any]]:
    """Holt normierte Move-Struct"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getNormalizedMoveStruct',
            'params': [package_id, module, struct_name]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            return {
                'package_id': package_id,
                'module': module,
                'struct_name': struct_name,
                'struct_info': data['result'],
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui normalized move struct: {e}")
    
    return None
