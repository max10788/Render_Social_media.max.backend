from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_normalized_move_modules_by_package(provider, package_id: str) -> Optional[Dict[str, Any]]:
    """Holt normierte Move-Module für ein Package"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getNormalizedMoveModulesByPackage',
            'params': [package_id]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            return {
                'package_id': package_id,
                'modules': data['result'],
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui normalized move modules: {e}")
    
    return None
