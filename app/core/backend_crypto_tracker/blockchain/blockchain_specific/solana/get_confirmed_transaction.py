from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_confirmed_transaction(provider, signature: str) -> Optional[Dict[str, Any]]:
    """Holt eine bestätigte Transaktion"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getConfirmedTransaction',
            'params': [
                signature,
                {'encoding': 'jsonParsed'}
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            tx = data['result']
            return {
                'signature': signature,
                'slot': tx.get('slot'),
                'block_time': datetime.fromtimestamp(tx.get('blockTime', 0)) if tx.get('blockTime') else None,
                'transaction': tx.get('transaction'),
                'meta': tx.get('meta'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Solana confirmed transaction: {e}")
    
    return None
