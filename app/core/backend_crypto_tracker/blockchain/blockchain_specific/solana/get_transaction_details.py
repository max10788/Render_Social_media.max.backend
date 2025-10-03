from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_transaction_details(provider, signature: str) -> Optional[Dict[str, Any]]:
    """Holt Details zu einer Solana-Transaktion"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getTransaction',
            'params': [
                signature,
                {'encoding': 'jsonParsed', 'maxSupportedTransactionVersion': 0}
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
        logger.error(f"Error fetching Solana transaction details: {e}")
    
    return None
