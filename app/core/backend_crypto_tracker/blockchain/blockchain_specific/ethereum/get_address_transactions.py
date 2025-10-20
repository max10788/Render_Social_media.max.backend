"""
Ethereum Address Transactions Fetcher
✅ ULTIMATE VERSION: Moralis (Primary) + Etherscan (Fallback)
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
import aiohttp
import os
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# ✅ Rate Limiters
_last_etherscan_request = 0
_last_moralis_request = 0
_etherscan_delay = 0.22  # 220ms = ~4.5 calls/sec
_moralis_delay = 0.04    # 40ms = ~25 calls/sec


async def get_transactions_moralis(
    address: str,
    api_key: str,
    limit: int = 25
) -> Optional[List[Dict[str, Any]]]:
    """
    ✅ NEW: Get transactions via Moralis API
    Much better than Etherscan!
    """
    global _last_moralis_request
    
    try:
        # Rate limiting
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_moralis_request
        
        if time_since_last < _moralis_delay:
            await asyncio.sleep(_moralis_delay - time_since_last)
        
        _last_moralis_request = asyncio.get_event_loop().time()
        
        # Moralis API V2
        url = f"https://deep-index.moralis.io/api/v2/{address}"
        
        headers = {
            'accept': 'application/json',
            'X-API-Key': api_key
        }
        
        params = {
            'chain': 'eth',
            'limit': limit
        }
        
        logger.debug(f"Moralis API Request: {url}")
        logger.debug(f"   Address: {address}")
        logger.debug(f"   Limit: {limit}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Moralis HTTP Error {response.status}: {error_text}")
                    return None
                
                data = await response.json()
                
                if not data or 'result' not in data:
                    logger.warning("No result field in Moralis response")
                    return None
                
                transactions = []
                
                for tx in data['result'][:limit]:
                    try:
                        # Parse Moralis transaction format
                        transactions.append({
                            'hash': tx.get('hash'),
                            'tx_hash': tx.get('hash'),
                            'block_number': int(tx.get('block_number', 0)),
                            'timestamp': datetime.fromisoformat(tx.get('block_timestamp', '').replace('Z', '+00:00')) if tx.get('block_timestamp') else datetime.now(),
                            'from': tx.get('from_address'),
                            'to': tx.get('to_address'),
                            'from_address': tx.get('from_address'),
                            'to_address': tx.get('to_address'),
                            'value': int(tx.get('value', '0')) / 10**18,
                            'gas': int(tx.get('gas', '0')),
                            'gas_price': int(tx.get('gas_price', '0')) / 10**9,
                            'gas_used': int(tx.get('receipt_gas_used', '0')),
                            'contract_address': tx.get('to_address') if tx.get('input', '0x') != '0x' else None,
                            'nonce': int(tx.get('nonce', '0')),
                            'transaction_index': int(tx.get('transaction_index', '0')),
                            'confirmations': int(tx.get('block_number', 0)),
                            'is_error': False,  # Moralis doesn't return failed txs by default
                            'inputs': [],
                            'outputs': []
                        })
                    except Exception as e:
                        logger.warning(f"Error parsing Moralis transaction: {e}")
                        continue
                
                logger.info(f"✅ Moralis: Successfully fetched {len(transactions)} transactions")
                return transactions
                
    except Exception as e:
        logger.error(f"Moralis API error: {e}")
        return None


async def get_transactions_etherscan(
    address: str,
    api_key: str,
    chainid: int = 1,
    limit: int = 25
) -> Optional[List[Dict[str, Any]]]:
    """
    Etherscan fallback (V2 API)
    """
    global _last_etherscan_request
    
    try:
        # Rate limiting
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_etherscan_request
        
        if time_since_last < _etherscan_delay:
            await asyncio.sleep(_etherscan_delay - time_since_last)
        
        _last_etherscan_request = asyncio.get_event_loop().time()
        
        # Etherscan V2 API
        base_url = "https://api.etherscan.io/v2/api"
        
        params = {
            'chainid': chainid,
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': 0,
            'endblock': 99999999,
            'sort': 'desc',
            'apikey': api_key,
            'page': 1,
            'offset': limit
        }
        
        logger.debug(f"Etherscan API V2 Request")
        logger.debug(f"   Address: {address}")
        logger.debug(f"   Limit: {limit}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.error(f"Etherscan HTTP Error {response.status}")
                    return None
                
                data = await response.json()
                
                if not data:
                    return None
                
                # Success
                if data.get('status') == '1' and data.get('result'):
                    transactions = []
                    
                    for tx in data['result'][:limit]:
                        try:
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
                        except Exception as e:
                            logger.warning(f"Error parsing Etherscan transaction: {e}")
                            continue
                    
                    logger.info(f"✅ Etherscan: Successfully fetched {len(transactions)} transactions")
                    return transactions
                
                # No transactions = OK
                elif data.get('status') == '0':
                    message = data.get('message', '')
                    result = data.get('result', '')
                    
                    if 'No transactions found' in str(result):
                        logger.debug(f"No transactions for {address}")
                        return []
                    
                    logger.warning(f"Etherscan error: {message} - {result}")
                    return None
                
                return None
                
    except Exception as e:
        logger.error(f"Etherscan API error: {e}")
        return None


async def execute_get_address_transactions(
    address: str,
    api_key: Optional[str] = None,
    start_block: int = 0,
    end_block: int = 99999999,
    sort: str = 'asc',
    base_url: str = "https://api.etherscan.io/v2/api",
    chainid: int = 1,
    limit: int = 25
) -> Optional[List[Dict[str, Any]]]:
    """
    Get address transactions with intelligent provider selection:
    1. Try Moralis first (better rate limits)
    2. Fallback to Etherscan if Moralis fails
    
    ✅ Auto-loads API keys from environment
    ✅ Built-in rate limiting for both providers
    """
    try:
        # Load API keys
        moralis_key = os.getenv('MORALIS_API_KEY')
        etherscan_key = api_key or os.getenv('ETHERSCAN_API_KEY')
        
        # Strategy 1: Try Moralis (preferred)
        if moralis_key:
            logger.debug(f"🚀 Trying Moralis API (25 req/sec)")
            result = await get_transactions_moralis(address, moralis_key, limit)
            
            if result is not None:  # Success or empty (new address)
                return result
            
            logger.info(f"⚠️ Moralis failed, falling back to Etherscan...")
        else:
            logger.debug(f"ℹ️ No Moralis API key found, using Etherscan only")
        
        # Strategy 2: Fallback to Etherscan
        if etherscan_key:
            logger.debug(f"🔄 Trying Etherscan API (5 req/sec)")
            result = await get_transactions_etherscan(address, etherscan_key, chainid, limit)
            
            if result is not None:
                return result
            
            logger.warning(f"⚠️ Both Moralis and Etherscan failed")
        else:
            logger.error(f"❌ No API keys available (neither Moralis nor Etherscan)")
        
        return None
        
    except Exception as e:
        logger.error(f"Fatal error in execute_get_address_transactions: {e}", exc_info=True)
        return None
