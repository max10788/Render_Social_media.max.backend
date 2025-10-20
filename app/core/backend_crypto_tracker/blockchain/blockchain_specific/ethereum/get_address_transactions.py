"""
Ethereum Address Transactions Fetcher
✅ FIXED: V2 API, Auto API Key loading, Rate limiting
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
import aiohttp
import os
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# ✅ Global Rate Limiter
_last_request_time = 0
_rate_limit_delay = 0.22  # 220ms = ~4.5 calls/sec (safe for 5 calls/sec limit)


async def execute_get_address_transactions(
    address: str,
    api_key: Optional[str] = None,  # ✅ NOW OPTIONAL
    start_block: int = 0,
    end_block: int = 99999999,
    sort: str = 'asc',
    base_url: str = "https://api.etherscan.io/v2/api",
    chainid: int = 1,
    limit: int = 25
) -> Optional[List[Dict[str, Any]]]:
    """
    Holt Transaktionen für eine Ethereum-Adresse von Etherscan API V2
    ✅ FIXED: Auto-loads API key from environment if not provided
    ✅ FIXED: Built-in rate limiting (5 calls/sec)
    
    Args:
        address: Ethereum-Adresse
        api_key: Etherscan API Key (optional - lädt aus ETHERSCAN_API_KEY env var)
        start_block: Startblock
        end_block: Endblock
        sort: Sortierung ('asc' oder 'desc')
        base_url: Etherscan API V2 URL
        chainid: Blockchain Chain ID (default: 1 = Ethereum Mainnet)
        limit: Maximale Anzahl von Transaktionen (default: 25)
    
    Returns:
        Liste von Transaktionen oder None bei Fehler
    """
    global _last_request_time
    
    try:
        # ✅ FIX 1: Auto-load API Key if not provided
        if not api_key:
            api_key = os.getenv('ETHERSCAN_API_KEY')
            if not api_key:
                logger.error("❌ No Etherscan API key provided and ETHERSCAN_API_KEY env var not set!")
                return None
            logger.debug("✅ Using API key from environment")
        
        # ✅ FIX 2: Rate Limiting (5 calls/sec max)
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_request_time
        
        if time_since_last < _rate_limit_delay:
            sleep_time = _rate_limit_delay - time_since_last
            logger.debug(f"⏱️ Rate limiting: waiting {sleep_time:.3f}s")
            await asyncio.sleep(sleep_time)
        
        _last_request_time = asyncio.get_event_loop().time()
        
        # API Request
        params = {
            'chainid': chainid,
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': start_block,
            'endblock': end_block,
            'sort': sort,
            'apikey': api_key,
            'page': 1,
            'offset': limit
        }
        
        logger.debug(f"Etherscan API V2 Request: {base_url}")
        logger.debug(f"   Address: {address}")
        logger.debug(f"   Blocks: {start_block} bis {end_block}")
        logger.debug(f"   Limit: {limit}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.error(f"HTTP Error {response.status}: {await response.text()}")
                    return None
                
                data = await response.json()
                
                if not data:
                    logger.error("Keine Daten von Etherscan API erhalten")
                    return None
                
                logger.debug(f"Etherscan Response Status: {data.get('status')}, Message: {data.get('message')}")
        
        # Success case
        if data.get('status') == '1' and data.get('result'):
            transactions = []
            result = data['result']
            
            # Limitiere auf die gewünschte Anzahl
            if len(result) > limit:
                result = result[:limit]
                logger.debug(f"Limitiert auf {limit} von {len(data['result'])} Transaktionen")
            
            for tx in result:
                try:
                    # Parse transaction data safely
                    block_number = tx.get('blockNumber', '0')
                    timestamp = tx.get('timeStamp', '0')
                    value = tx.get('value', '0')
                    gas = tx.get('gas', '0')
                    gas_price = tx.get('gasPrice', '0')
                    gas_used = tx.get('gasUsed', '0')
                    nonce = tx.get('nonce', '0')
                    tx_index = tx.get('transactionIndex', '0')
                    
                    transactions.append({
                        'hash': tx.get('hash'),
                        'tx_hash': tx.get('hash'),
                        'block_number': int(block_number) if isinstance(block_number, str) and block_number.isdigit() else 0,
                        'timestamp': datetime.fromtimestamp(int(timestamp)) if isinstance(timestamp, str) and timestamp.isdigit() else datetime.now(),
                        'from': tx.get('from'),
                        'to': tx.get('to'),
                        'from_address': tx.get('from'),
                        'to_address': tx.get('to'),
                        'value': int(value) / 10**18 if isinstance(value, str) and value.isdigit() else 0,
                        'gas': int(gas) if isinstance(gas, str) and gas.isdigit() else 0,
                        'gas_price': int(gas_price) / 10**9 if isinstance(gas_price, str) and gas_price.isdigit() else 0,
                        'gas_used': int(gas_used) if isinstance(gas_used, str) and gas_used.isdigit() else 0,
                        'contract_address': tx.get('contractAddress'),
                        'nonce': int(nonce) if isinstance(nonce, str) and nonce.isdigit() else 0,
                        'transaction_index': int(tx_index) if isinstance(tx_index, str) and tx_index.isdigit() else 0,
                        'confirmations': int(tx.get('confirmations', 0)),
                        'is_error': tx.get('isError', '0') == '1',
                        'inputs': [],
                        'outputs': []
                    })
                except Exception as e:
                    logger.warning(f"Error parsing transaction: {e}")
                    continue
            
            logger.debug(f"✅ Erfolgreich {len(transactions)} Ethereum-Transaktionen abgerufen")
            return transactions
        
        # Error cases
        elif data.get('status') == '0':
            message = data.get('message', 'Unknown error')
            result = data.get('result', '')
            
            # No transactions = OK (new address)
            if 'No transactions found' in str(result) or message == 'No transactions found':
                logger.debug(f"Keine Transaktionen für {address} gefunden (neue Adresse?)")
                return []
            
            # API Key errors
            if 'invalid' in message.lower() and 'api' in message.lower():
                logger.error(f"❌ Invalid API Key error: {result}")
                return None
            
            # Rate limit
            if 'rate limit' in message.lower() or 'too many' in str(result).lower():
                logger.warning(f"⏱️ Rate limit hit: {result}")
                return None
            
            # Other errors
            logger.error(f"Etherscan API Error: {message} - {result}")
            return None
        
        else:
            logger.warning(f"Unerwartete API-Antwort: {data}")
            return []
            
    except asyncio.TimeoutError:
        logger.error(f"Timeout beim Abrufen von Ethereum-Transaktionen für {address}")
        return None
    except Exception as e:
        logger.error(f"Exception beim Abrufen von Ethereum-Transaktionen: {e}", exc_info=True)
        return None
