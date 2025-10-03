from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_stake_accounts(provider, address: str) -> Optional[List[Dict[str, Any]]]:
    """Holt Stake-Accounts für eine Adresse"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getStakeAccountsByDelegate',
            'params': [address]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            accounts = []
            for account in data['result'].get('value', []):
                accounts.append({
                    'account_address': account.get('pubkey'),
                    'stake_account_info': account.get('account'),
                    'last_updated': datetime.now()
                })
            
            return accounts
    except Exception as e:
        logger.error(f"Error fetching Solana stake accounts: {e}")
    
    return None
