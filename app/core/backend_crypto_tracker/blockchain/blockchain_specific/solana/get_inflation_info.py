from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_inflation_info(provider) -> Optional[Dict[str, Any]]:
    """Holt Inflationsinformationen"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getInflation',
            'params': []
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            inflation_info = data['result']
            return {
                'current_rate': inflation_info.get('current'),
                'foundation_rate': inflation_info.get('foundation'),
                'validator_rate': inflation_info.get('validator'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Solana inflation info: {e}")
    
    return None
