from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_events(provider, query: Dict[str, Any], limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """Holt Events basierend auf einer Query"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getEvents',
            'params': [query, None, limit, True]  # cursor, limit, descending_order
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            events = []
            for event in data['result'].get('data', []):
                events.append({
                    'id': event.get('id'),
                    'package_id': event.get('packageId'),
                    'transaction_module': event.get('transactionModule'),
                    'sender': event.get('sender'),
                    'type': event.get('type'),
                    'parsed_json': event.get('parsedJson'),
                    'bcs': event.get('bcs'),
                    'timestamp_ms': event.get('timestampMs'),
                    'last_updated': datetime.now()
                })
            
            return events
    except Exception as e:
        logger.error(f"Error fetching Sui events: {e}")
    
    return None
