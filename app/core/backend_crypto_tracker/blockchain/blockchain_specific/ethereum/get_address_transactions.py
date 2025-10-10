# app/core/backend_crypto_tracker/blockchain/blockchain_specific/ethereum/get_address_transactions.py

from datetime import datetime
from typing import Any, Dict, List, Optional
import aiohttp
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

async def execute_get_address_transactions(
    address: str,
    api_key: str,
    start_block: int = 0,
    end_block: int = 99999999,
    sort: str = 'asc',
    base_url: str = "https://api.etherscan.io/api"
) -> Optional[List[Dict[str, Any]]]:
    """
    Holt Transaktionen für eine Ethereum-Adresse direkt von Etherscan
    
    Args:
        address: Ethereum-Adresse
        api_key: Etherscan API Key
        start_block: Startblock
        end_block: Endblock
        sort: Sortierung ('asc' oder 'desc')
        base_url: Etherscan API URL (default: mainnet)
    
    Returns:
        Liste von Transaktionen oder None bei Fehler
    """
    try:
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': start_block,
            'endblock': end_block,
            'sort': sort,
            'apikey': api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as response:
                if response.status != 200:
                    logger.error(f"HTTP Error {response.status}: {await response.text()}")
                    return None
                
                data = await response.json()
        
        if data and data.get('status') == '1' and data.get('result'):
            transactions = []
            for tx in data['result']:
                transactions.append({
                    'hash': tx.get('hash'),  # ✅ Geändert von 'tx_hash' zu 'hash'
                    'tx_hash': tx.get('hash'),
                    'block_number': int(tx.get('blockNumber', 0)),
                    'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                    'from': tx.get('from'),  # ✅ Hinzugefügt
                    'to': tx.get('to'),  # ✅ Hinzugefügt
                    'from_address': tx.get('from'),
                    'to_address': tx.get('to'),
                    'value': int(tx.get('value', 0)) / 10**18,
                    'gas': int(tx.get('gas', 0)),
                    'gas_price': int(tx.get('gasPrice', 0)) / 10**9,
                    'gas_used': int(tx.get('gasUsed', 0)),
                    'contract_address': tx.get('contractAddress'),
                    'nonce': int(tx.get('nonce', 0)),
                    'transaction_index': int(tx.get('transactionIndex', 0)),
                    'confirmations': int(tx.get('confirmations', 0)),
                    'is_error': tx.get('isError', '0') == '1',
                    # Füge leere inputs/outputs hinzu für Kompatibilität
                    'inputs': [],
                    'outputs': []
                })
            
            logger.info(f"Erfolgreich {len(transactions)} Transaktionen für {address} abgerufen")
            return transactions
        else:
            logger.warning(f"Keine Transaktionen gefunden oder API-Fehler: {data.get('message')}")
            return []
            
    except Exception as e:
        logger.error(f"Error fetching Ethereum address transactions: {e}")
        return None
