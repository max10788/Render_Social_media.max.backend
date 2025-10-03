from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_vote_accounts(provider) -> Optional[Dict[str, List[Dict[str, Any]]]]:
    """Holt Vote-Accounts (Validator-Informationen)"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getVoteAccounts',
            'params': []
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            vote_accounts = {
                'current': [],
                'delinquent': []
            }
            
            for account_type in ['current', 'delinquent']:
                for account in data['result'].get(account_type, []):
                    vote_accounts[account_type].append({
                        'vote_address': account.get('votePubkey'),
                        'node_address': account.get('nodePubkey'),
                        'activated_stake': account.get('activatedStake'),
                        'commission': account.get('commission'),
                        'last_updated': datetime.now()
                    })
            
            return vote_accounts
    except Exception as e:
        logger.error(f"Error fetching Solana vote accounts: {e}")
    
    return None
