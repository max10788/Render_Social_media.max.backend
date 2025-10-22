"""
Recent Token Traders Fetcher
✅ NEW: Get wallets that interacted with token in last X hours
Perfect for finding recent buyers/sellers!
"""

from typing import List, Dict, Any, Optional
import aiohttp
import os
import asyncio
from datetime import datetime, timedelta
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# Rate limiters
_last_moralis_request = 0
_last_etherscan_request = 0
_moralis_delay = 0.04    # 40ms = 25 calls/sec
_etherscan_delay = 0.22  # 220ms = 5 calls/sec


async def get_recent_transfers_moralis(
    token_address: str,
    api_key: str,
    hours: int = 1,
    limit: int = 1000
) -> Optional[List[Dict[str, Any]]]:
    """
    ✅ Get recent token transfers via Moralis API
    """
    global _last_moralis_request
    
    try:
        # Rate limiting
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_moralis_request
        
        if time_since_last < _moralis_delay:
            await asyncio.sleep(_moralis_delay - time_since_last)
        
        _last_moralis_request = asyncio.get_event_loop().time()
        
        # ✅ FIX: Calculate timestamp with UTC timezone
        from datetime import timezone
        time_ago = datetime.now(timezone.utc) - timedelta(hours=hours)
        from_block = 0  # Moralis uses block numbers, we'll filter by timestamp later
        
        # Moralis Token Transfers API
        url = f"https://deep-index.moralis.io/api/v2.2/erc20/{token_address}/transfers"
        
        headers = {
            'accept': 'application/json',
            'X-API-Key': api_key
        }
        
        # ✅ FIX: Moralis Free Tier limit = 100
        actual_limit = min(limit, 100)
        
        params = {
            'chain': 'eth',
            'limit': actual_limit,
            'order': 'DESC'  # Newest first
        }
        
        logger.info(f"🚀 Moralis: Fetching recent transfers for {token_address} (last {hours}h)")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Moralis HTTP Error {response.status}: {error_text[:200]}")
                    return None
                
                data = await response.json()
                
                if not data or 'result' not in data:
                    logger.warning("No result field in Moralis response")
                    return None
                
                transfers = []
                
                for transfer in data['result']:
                    try:
                        # Parse timestamp
                        block_timestamp = transfer.get('block_timestamp', '')
                        if block_timestamp:
                            tx_time = datetime.fromisoformat(block_timestamp.replace('Z', '+00:00'))
                            
                            # Filter: Only last X hours
                            if tx_time < time_ago:
                                break  # Since sorted DESC, we can stop here
                            
                            transfers.append({
                                'hash': transfer.get('transaction_hash'),
                                'from': transfer.get('from_address'),
                                'to': transfer.get('to_address'),
                                'value': float(transfer.get('value', 0)),
                                'timestamp': int(tx_time.timestamp()),
                                'block_number': int(transfer.get('block_number', 0))
                            })
                    except Exception as e:
                        logger.warning(f"Error parsing transfer: {e}")
                        continue
                
                logger.info(f"✅ Moralis: Found {len(transfers)} transfers in last {hours}h")
                return transfers
                
    except Exception as e:
        logger.error(f"Moralis API error: {e}")
        return None


async def get_recent_transfers_etherscan(
    token_address: str,
    chain: str,
    api_key: str,
    hours: int = 1,
    limit: int = 1000
) -> Optional[List[Dict[str, Any]]]:
    """
    Etherscan V2 fallback for recent transfers
    """
    global _last_etherscan_request
    
    try:
        # Rate limiting
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_etherscan_request
        
        if time_since_last < _etherscan_delay:
            await asyncio.sleep(_etherscan_delay - time_since_last)
        
        _last_etherscan_request = asyncio.get_event_loop().time()
        
        # Config
        if chain == 'ethereum':
            base_url = "https://api.etherscan.io/v2/api"
            chainid = '1'
        elif chain == 'bsc':
            base_url = "https://api.bscscan.com/v2/api"
            chainid = '56'
        else:
            logger.warning(f"Unsupported chain: {chain}")
            return None
        
        logger.info(f"🔄 Etherscan: Fetching recent transfers for {token_address} (last {hours}h)")
        
        # Calculate time threshold
        time_ago = datetime.now() - timedelta(hours=hours)
        time_threshold = int(time_ago.timestamp())
        
        params = {
            'chainid': chainid,
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': token_address,
            'page': 1,
            'offset': limit,
            'sort': 'desc',
            'apikey': api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.error(f"Etherscan HTTP Error {response.status}")
                    return None
                
                data = await response.json()
                
                if data.get('status') == '1' and data.get('result'):
                    transfers = []
                    
                    for tx in data['result']:
                        try:
                            timestamp = int(tx.get('timeStamp', 0))
                            
                            # Filter: Only last X hours
                            if timestamp < time_threshold:
                                break  # Sorted DESC, can stop here
                            
                            transfers.append({
                                'hash': tx.get('hash'),
                                'from': tx.get('from'),
                                'to': tx.get('to'),
                                'value': float(tx.get('value', 0)),
                                'timestamp': timestamp,
                                'block_number': int(tx.get('blockNumber', 0))
                            })
                        except Exception as e:
                            logger.warning(f"Error parsing transfer: {e}")
                            continue
                    
                    logger.info(f"✅ Etherscan: Found {len(transfers)} transfers in last {hours}h")
                    return transfers
                else:
                    logger.warning(f"Etherscan error: {data.get('message', 'Unknown')}")
                    return None
                
    except Exception as e:
        logger.error(f"Etherscan API error: {e}")
        return None


def extract_unique_traders(transfers: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Extract unique trader addresses with their activity stats
    """
    traders = {}
    
    for transfer in transfers:
        from_addr = transfer.get('from', '').lower()
        to_addr = transfer.get('to', '').lower()
        value = transfer.get('value', 0)
        timestamp = transfer.get('timestamp', 0)
        
        # Skip zero address
        zero_addr = '0x0000000000000000000000000000000000000000'
        
        # Process FROM address (seller)
        if from_addr and from_addr != zero_addr:
            if from_addr not in traders:
                traders[from_addr] = {
                    'address': from_addr,
                    'buy_count': 0,
                    'sell_count': 0,
                    'total_bought': 0,
                    'total_sold': 0,
                    'first_tx': timestamp,
                    'last_tx': timestamp,
                    'tx_count': 0
                }
            
            traders[from_addr]['sell_count'] += 1
            traders[from_addr]['total_sold'] += value
            traders[from_addr]['tx_count'] += 1
            traders[from_addr]['last_tx'] = max(traders[from_addr]['last_tx'], timestamp)
            traders[from_addr]['first_tx'] = min(traders[from_addr]['first_tx'], timestamp)
        
        # Process TO address (buyer)
        if to_addr and to_addr != zero_addr:
            if to_addr not in traders:
                traders[to_addr] = {
                    'address': to_addr,
                    'buy_count': 0,
                    'sell_count': 0,
                    'total_bought': 0,
                    'total_sold': 0,
                    'first_tx': timestamp,
                    'last_tx': timestamp,
                    'tx_count': 0
                }
            
            traders[to_addr]['buy_count'] += 1
            traders[to_addr]['total_bought'] += value
            traders[to_addr]['tx_count'] += 1
            traders[to_addr]['last_tx'] = max(traders[to_addr]['last_tx'], timestamp)
            traders[to_addr]['first_tx'] = min(traders[to_addr]['first_tx'], timestamp)
    
    return traders


async def execute_get_recent_traders(
    token_address: str,
    chain: str,
    hours: int = 1
) -> Dict[str, Any]:
    """
    Get wallets that recently traded this token
    
    Returns:
    {
        'traders': [list of trader dicts],
        'summary': {
            'total_traders': int,
            'total_transfers': int,
            'timeframe_hours': int,
            'buyers_only': int,
            'sellers_only': int,
            'both': int
        }
    }
    """
    try:
        logger.info(f"Fetching recent traders for {token_address} on {chain} (last {hours}h)")
        
        # Load API keys
        moralis_key = os.getenv('MORALIS_API_KEY')
        
        if chain == 'ethereum':
            etherscan_key = os.getenv('ETHERSCAN_API_KEY')
        elif chain == 'bsc':
            etherscan_key = os.getenv('BSCSCAN_API_KEY')
        else:
            logger.warning(f"Unsupported chain: {chain}")
            return {'traders': [], 'summary': {}}
        
        transfers = None
        
        # Strategy 1: Try Moralis
        if moralis_key and chain == 'ethereum':
            logger.info(f"🚀 Trying Moralis API...")
            transfers = await get_recent_transfers_moralis(token_address, moralis_key, hours, limit=100)  # ✅ Max 100
            
            if transfers is None:
                logger.info(f"⚠️ Moralis failed, trying Etherscan...")
        
        # Strategy 2: Fallback to Etherscan
        if transfers is None and etherscan_key:
            logger.info(f"🔄 Trying Etherscan fallback...")
            transfers = await get_recent_transfers_etherscan(token_address, chain, etherscan_key, hours, limit=100)  # ✅ Match Moralis
        
        if not transfers:
            logger.warning(f"No recent transfers found")
            return {
                'traders': [],
                'summary': {
                    'total_traders': 0,
                    'total_transfers': 0,
                    'timeframe_hours': hours,
                    'buyers_only': 0,
                    'sellers_only': 0,
                    'both': 0
                }
            }
        
        # Extract unique traders
        traders_dict = extract_unique_traders(transfers)
        traders_list = list(traders_dict.values())
        
        # Sort by activity (most active first)
        traders_list.sort(key=lambda x: x['tx_count'], reverse=True)
        
        # Calculate summary stats
        buyers_only = len([t for t in traders_list if t['buy_count'] > 0 and t['sell_count'] == 0])
        sellers_only = len([t for t in traders_list if t['sell_count'] > 0 and t['buy_count'] == 0])
        both = len([t for t in traders_list if t['buy_count'] > 0 and t['sell_count'] > 0])
        
        summary = {
            'total_traders': len(traders_list),
            'total_transfers': len(transfers),
            'timeframe_hours': hours,
            'buyers_only': buyers_only,
            'sellers_only': sellers_only,
            'both': both
        }
        
        logger.info(f"✅ Found {len(traders_list)} unique traders in last {hours}h")
        logger.info(f"   Buyers only: {buyers_only}, Sellers only: {sellers_only}, Both: {both}")
        
        return {
            'traders': traders_list,
            'summary': summary
        }
        
    except Exception as e:
        logger.error(f"Error fetching recent traders: {e}", exc_info=True)
        return {'traders': [], 'summary': {}}
