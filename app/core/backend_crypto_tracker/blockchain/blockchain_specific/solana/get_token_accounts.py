from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_accounts(provider, address: str, mint_address: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """Holt Token-Accounts für eine Adresse"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getTokenAccountsByOwner',
            'params': [
                address,
                {'programId': 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'} if not mint_address else {'mint': mint_address},
                {'encoding': 'jsonParsed'}
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            accounts = []
            for account in data['result'].get('value', []):
                account_info = account.get('account', {}).get('data', {})
                parsed_info = account_info.get('parsed', {})
                
                accounts.append({
                    'account_address': account.get('pubkey'),
                    'mint': parsed_info.get('info', {}).get('mint'),
                    'amount': float(parsed_info.get('info', {}).get('tokenAmount', {}).get('uiAmount', 0)),
                    'decimals': parsed_info.get('info', {}).get('tokenAmount', {}).get('decimals'),
                    'last_updated': datetime.now()
                })
            
            return accounts
    except Exception as e:
        logger.error(f"Error fetching Solana token accounts: {e}")
    
    return None
