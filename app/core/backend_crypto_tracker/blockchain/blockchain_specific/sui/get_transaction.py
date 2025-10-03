from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_transaction_details(provider, tx_digest: str) -> Optional[Dict[str, Any]]:
    """Holt Details zu einer spezifischen Transaktion"""
    try:
        url = f"{provider.base_url}"
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getTransaction',
            'params': [tx_digest]
        }
        
        data = await provider._make_post_request(url, params)
        
        if data and data.get('result'):
            tx = data['result']
            return {
                'transaction_digest': tx.get('digest'),
                'timestamp': datetime.fromtimestamp(int(tx.get('timestampMs', 0)) / 1000) if tx.get('timestampMs') else None,
                'status': tx.get('status', {}).get('status'),
                'effects': tx.get('effects', {}),
                'events': tx.get('events', []),
                'object_changes': tx.get('objectChanges', []),
                'balance_changes': tx.get('balanceChanges', []),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui transaction details: {e}")
    
    return None
