from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_address_transactions(
    provider, 
    address: str, 
    start_block: int = 0, 
    end_block: int = 99999999, 
    sort: str = 'asc'
) -> Optional[List[Dict[str, Any]]]:
    """Holt Transaktionen für eine Ethereum-Adresse"""
    try:
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': start_block,
            'endblock': end_block,
            'sort': sort,
            'apikey': provider.api_key
        }
        
        data = await provider._make_request(provider.base_url, params)
        
        if data and data.get('status') == '1' and data.get('result'):
            transactions = []
            for tx in data['result']:
                transactions.append({
                    'tx_hash': tx.get('hash'),
                    'block_number': int(tx.get('blockNumber', 0)),
                    'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                    'from_address': tx.get('from'),
                    'to_address': tx.get('to'),
                    'value': int(tx.get('value', 0)) / 10**18,
                    'gas': int(tx.get('gas', 0)),
                    'gas_price': int(tx.get('gasPrice', 0)) / 10**9,  # Wei zu Gwei
                    'gas_used': int(tx.get('gasUsed', 0)),
                    'contract_address': tx.get('contractAddress'),
                    'nonce': int(tx.get('nonce', 0)),
                    'transaction_index': int(tx.get('transactionIndex', 0)),
                    'confirmations': int(tx.get('confirmations', 0))
                })
            
            return transactions
    except Exception as e:
        logger.error(f"Error fetching Ethereum address transactions: {e}")
    
    return None
