from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_transaction_blocks(provider, digest: str) -> Optional[Dict[str, Any]]:
    """Holt Transaction Block für eine Digest"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getTransactionBlock',
            'params': [
                digest,
                {
                    'showInput': True,
                    'showRawInput': True,
                    'showEffects': True,
                    'showEvents': True,
                    'showObjectChanges': True,
                    'showBalanceChanges': True
                }
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            tx_block = data['result']
            return {
                'digest': digest,
                'transaction_block': tx_block.get('transaction'),
                'effects': tx_block.get('effects'),
                'events': tx_block.get('events'),
                'object_changes': tx_block.get('objectChanges'),
                'balance_changes': tx_block.get('balanceChanges'),
                'timestamp_ms': tx_block.get('timestampMs'),
                'checkpoint': tx_block.get('checkpoint'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui transaction block: {e}")
    
    return None
