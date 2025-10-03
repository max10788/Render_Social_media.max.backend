from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_largest_accounts(provider, mint_address: str) -> Optional[List[Dict[str, Any]]]:
    """Holt die größten Token-Accounts für einen Mint"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getTokenLargestAccounts',
            'params': [mint_address]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            accounts = []
            for account in data['result'].get('value', []):
                accounts.append({
                    'address': account.get('address'),
                    'amount': account.get('uiAmount'),
                    'decimals': account.get('decimals'),
                    'percentage': account.get('percentage'),
                    'last_updated': datetime.now()
                })
            
            return accounts
    except Exception as e:
        logger.error(f"Error fetching Solana token largest accounts: {e}")
    
    return None
