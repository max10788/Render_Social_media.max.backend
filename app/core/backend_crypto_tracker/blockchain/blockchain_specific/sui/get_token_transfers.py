from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_transfers(provider, address: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """Holt Token-Transfers für eine Adresse"""
    try:
        # Verwende Events, um Token-Transfers zu finden
        query = {
            'MoveEventModule': {
                'package': '0x2',
                'module': 'coin'
            }
        }
        
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getEvents',
            'params': [query, None, limit, True]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            transfers = []
            for event in data['result'].get('data', []):
                parsed_json = event.get('parsedJson', {})
                if parsed_json.get('type') and 'Transfer' in parsed_json['type']:
                    transfers.append({
                        'transaction_digest': event.get('id', {}).get('txDigest'),
                        'sender': event.get('sender'),
                        'recipient': parsed_json.get('to'),
                        'amount': parsed_json.get('amount'),
                        'coin_type': parsed_json.get('coin_type'),
                        'timestamp_ms': event.get('timestampMs'),
                        'last_updated': datetime.now()
                    })
            
            return transfers
    except Exception as e:
        logger.error(f"Error fetching Sui token transfers: {e}")
    
    return None
