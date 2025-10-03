from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_transfers(
    provider, 
    address: str, 
    contract_address: Optional[str] = None
) -> Optional[List[Dict[str, Any]]]:
    """Holt Token-Transfers für eine Adresse"""
    try:
        params = {
            'module': 'account',
            'action': 'tokentx',
            'address': address,
            'sort': 'desc',
            'apikey': provider.api_key
        }
        
        if contract_address:
            params['contractaddress'] = contract_address
        
        data = await provider._make_request(provider.base_url, params)
        
        if data and data.get('status') == '1' and data.get('result'):
            transfers = []
            for tx in data['result']:
                transfers.append({
                    'tx_hash': tx.get('hash'),
                    'block_number': int(tx.get('blockNumber', 0)),
                    'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                    'from_address': tx.get('from'),
                    'to_address': tx.get('to'),
                    'contract_address': tx.get('contractAddress'),
                    'token_symbol': tx.get('tokenSymbol'),
                    'token_name': tx.get('tokenName'),
                    'token_decimal': int(tx.get('tokenDecimal', 18)),
                    'value': int(tx.get('value', 0)) / (10 ** int(tx.get('tokenDecimal', 18))),
                    'transaction_index': int(tx.get('transactionIndex', 0)),
                    'gas': int(tx.get('gas', 0)),
                    'gas_price': int(tx.get('gasPrice', 0)) / 10**9,
                    'gas_used': int(tx.get('gasUsed', 0)),
                    'confirmations': int(tx.get('confirmations', 0))
                })
            
            return transfers
    except Exception as e:
        logger.error(f"Error fetching Ethereum token transfers: {e}")
    
    return None
