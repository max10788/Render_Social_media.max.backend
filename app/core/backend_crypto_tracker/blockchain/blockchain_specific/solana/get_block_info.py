from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_block_info(provider, slot: int) -> Optional[Dict[str, Any]]:
    """Holt Block-Informationen für einen bestimmten Slot"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getBlock',
            'params': [
                slot,
                {'encoding': 'jsonParsed', 'transactionDetails': 'full', 'rewards': False}
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            block = data['result']
            return {
                'slot': slot,
                'block_height': block.get('blockHeight'),
                'block_time': datetime.fromtimestamp(block.get('blockTime', 0)) if block.get('blockTime') else None,
                'parent_slot': block.get('parentSlot'),
                'blockhash': block.get('blockhash'),
                'transactions': len(block.get('transactions', [])),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Solana block info: {e}")
    
    return None
