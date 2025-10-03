from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_program_accounts(provider, program_id: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
    """Holt alle Accounts für ein Programm"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getProgramAccounts',
            'params': [
                program_id,
                {
                    'encoding': 'jsonParsed',
                    'limit': limit
                }
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            accounts = []
            for account in data['result']:
                accounts.append({
                    'account_address': account.get('pubkey'),
                    'account_info': account.get('account'),
                    'last_updated': datetime.now()
                })
            
            return accounts
    except Exception as e:
        logger.error(f"Error fetching Solana program accounts: {e}")
    
    return None
