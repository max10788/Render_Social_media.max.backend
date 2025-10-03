from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_epoch_info(provider) -> Optional[Dict[str, Any]]:
    """Holt Epoch-Informationen"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getEpochInfo',
            'params': []
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            epoch_info = data['result']
            return {
                'epoch': epoch_info.get('epoch'),
                'slot_index': epoch_info.get('slotIndex'),
                'slots_in_epoch': epoch_info.get('slotsInEpoch'),
                'absolute_slot': epoch_info.get('absoluteSlot'),
                'block_height': epoch_info.get('blockHeight'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Solana epoch info: {e}")
    
    return None
