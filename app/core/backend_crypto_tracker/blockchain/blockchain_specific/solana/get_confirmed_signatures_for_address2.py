from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_confirmed_signatures_for_address2(provider, address: str, limit: int = 1000) -> Optional[List[Dict[str, Any]]]:
    """Holt bestätigte Signaturen für eine Adresse (v2 Methode)"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getConfirmedSignaturesForAddress2',
            'params': [
                address,
                {'limit': limit}
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            signatures = []
            for sig in data['result']:
                signatures.append({
                    'signature': sig.get('signature'),
                    'slot': sig.get('slot'),
                    'block_time': datetime.fromtimestamp(sig.get('blockTime', 0)) if sig.get('blockTime') else None,
                    'confirmation_status': sig.get('confirmationStatus'),
                    'err': sig.get('err'),
                    'memo': sig.get('memo'),
                    'last_updated': datetime.now()
                })
            
            return signatures
    except Exception as e:
        logger.error(f"Error fetching Solana confirmed signatures for address2: {e}")
    
    return None
