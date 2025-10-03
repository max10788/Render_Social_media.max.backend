from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_transfers(provider, address: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
    """Holt Token-Transfers für eine Adresse"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getTokenLargestAccounts',
            'params': [address]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            transfers = []
            for account in data['result'].get('value', []):
                transfers.append({
                    'mint': account.get('address'),
                    'amount': account.get('uiAmount'),
                    'decimals': account.get('decimals'),
                    'last_updated': datetime.now()
                })
            
            return transfers
    except Exception as e:
        logger.error(f"Error fetching Solana token transfers: {e}")
    
    return None
