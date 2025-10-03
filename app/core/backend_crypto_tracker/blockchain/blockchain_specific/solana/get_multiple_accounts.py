from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_multiple_accounts(provider, addresses: List[str]) -> Optional[List[Dict[str, Any]]]:
    """Holt Informationen für mehrere Accounts"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getMultipleAccounts',
            'params': [
                addresses,
                {'encoding': 'jsonParsed'}
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            accounts = []
            for i, account_info in enumerate(data['result'].get('value', [])):
                accounts.append({
                    'address': addresses[i],
                    'lamports': account_info.get('lamports') if account_info else None,
                    'owner': account_info.get('owner') if account_info else None,
                    'executable': account_info.get('executable') if account_info else None,
                    'rent_epoch': account_info.get('rentEpoch') if account_info else None,
                    'last_updated': datetime.now()
                })
            
            return accounts
    except Exception as e:
        logger.error(f"Error fetching Solana multiple accounts: {e}")
    
    return None
