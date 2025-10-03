from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_account_info(provider, address: str) -> Optional[Dict[str, Any]]:
    """Holt detaillierte Account-Informationen"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getAccountInfo',
            'params': [
                address,
                {'encoding': 'jsonParsed'}
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            account_info = data['result'].get('value', {})
            return {
                'address': address,
                'lamports': account_info.get('lamports'),
                'owner': account_info.get('owner'),
                'executable': account_info.get('executable'),
                'rent_epoch': account_info.get('rentEpoch'),
                'data': account_info.get('data'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Solana account info: {e}")
    
    return None
