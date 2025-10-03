from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_holders(provider, mint_address: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
    """Holt Token-Holder für einen Mint"""
    try:
        # Verwende get_token_largest_accounts als Grundlage für Holder-Informationen
        largest_accounts = await execute_get_token_largest_accounts(provider, mint_address)
        
        if largest_accounts:
            holders = []
            for account in largest_accounts[:limit]:
                holders.append({
                    'address': account.get('address'),
                    'balance': account.get('amount'),
                    'percentage': account.get('percentage'),
                    'last_updated': datetime.now()
                })
            
            return holders
    except Exception as e:
        logger.error(f"Error fetching Solana token holders: {e}")
    
    return None
