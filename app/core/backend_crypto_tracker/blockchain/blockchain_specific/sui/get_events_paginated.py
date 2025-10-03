from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_events_paginated(
    provider, 
    query: Dict[str, Any], 
    cursor: Optional[str] = None, 
    limit: int = 50
) -> Optional[Dict[str, Any]]:
    """Holt Events mit Pagination"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getEvents',
            'params': [query, cursor, limit, True]
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
                    'timestamp_ms': event.get('timestampMs'),
                    'last_updated': datetime.now()
                })
            
            return {
                'data': events,
                'has_next_page': data['result'].get('hasNextPage', False),
                'next_cursor': data['result'].get('nextCursor'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching Sui events paginated: {e}")
    
    return None
