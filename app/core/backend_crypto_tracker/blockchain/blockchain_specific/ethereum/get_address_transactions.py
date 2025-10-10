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
        
        logger.info(f"🔍 Etherscan API Request: {base_url}")
        logger.info(f"   Address: {address}")
        logger.info(f"   Blocks: {start_block} bis {end_block}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as response:
                if response.status != 200:
                    logger.error(f"❌ HTTP Error {response.status}: {await response.text()}")
                    return None
                
                data = await response.json()
                
                # ✅ Bessere Fehlerbehandlung
                if not data:
                    logger.error("❌ Keine Daten von Etherscan API erhalten")
                    return None
                
                logger.info(f"📥 Etherscan Response Status: {data.get('status')}, Message: {data.get('message')}")
        
        # ✅ Prüfe auf erfolgreiche API-Antwort
        if data.get('status') == '1' and data.get('result'):
            transactions = []
            for tx in data['result']:
                transactions.append({
                    'hash': tx.get('hash'),
                    'tx_hash': tx.get('hash'),
                    'block_number': int(tx.get('blockNumber', 0)),
                    'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                    'from': tx.get('from'),
                    'to': tx.get('to'),
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
                    'inputs': [],
                    'outputs': []
                })
            
            logger.info(f"✅ {len(transactions)} Transaktionen für {address} abgerufen")
            return transactions
        
        # ✅ Detaillierte Fehlerausgabe
        elif data.get('status') == '0':
            message = data.get('message', 'Unknown error')
            result = data.get('result', '')
            
            # Leere Adresse ist OK (keine Transaktionen)
            if 'No transactions found' in str(result) or message == 'No transactions found':
                logger.info(f"ℹ️  Keine Transaktionen für {address} gefunden (neue Adresse?)")
                return []
            
            # Andere Fehler
            logger.error(f"❌ Etherscan API Error: {message} - {result}")
            return None
        
        else:
            logger.warning(f"⚠️  Unerwartete API-Antwort: {data}")
            return []
            
    except Exception as e:
        logger.error(f"❌ Exception beim Abrufen von Ethereum-Transaktionen: {e}", exc_info=True)
        return None
