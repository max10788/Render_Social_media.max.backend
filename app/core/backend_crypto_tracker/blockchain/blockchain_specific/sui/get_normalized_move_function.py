from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_normalized_move_function(provider, package_id: str, module: str, function: str) -> Optional[Dict[str, Any]]:
    """Holt normierte Move-Funktion"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getNormalizedMoveFunction',
            'params': [package_id, module, function]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            return {
                'package_id': package_id,
                'module': module,
                'function': function,
                'function_info': data['result'],
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui normalized move function: {e}")
    
    return None
