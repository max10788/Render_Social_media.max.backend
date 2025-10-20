"""
Token Holders Fetcher
✅ ULTIMATE VERSION: Moralis (Primary) + Etherscan (Fallback)
"""

from typing import List, Dict, Any, Optional
import aiohttp
import os
import asyncio
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# Rate limiters
_last_moralis_request = 0
_last_etherscan_request = 0
_moralis_delay = 0.04    # 40ms = 25 calls/sec
_etherscan_delay = 0.22  # 220ms = 5 calls/sec


async def get_holders_moralis(
    token_address: str,
    api_key: str,
    limit: int = 100
) -> Optional[List[Dict[str, Any]]]:
    """
    ✅ Get token holders via Moralis API
    """
    global _last_moralis_request
    
    try:
        # Rate limiting
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_moralis_request
        
        if time_since_last < _moralis_delay:
            await asyncio.sleep(_moralis_delay - time_since_last)
        
        _last_moralis_request = asyncio.get_event_loop().time()
        
        # Moralis Token API
        url = f"https://deep-index.moralis.io/api/v2/erc20/{token_address}/owners"
        
        headers = {
            'accept': 'application/json',
            'X-API-Key': api_key
        }
        
        params = {
            'chain': 'eth',
            'limit': limit,
            'order': 'DESC'  # Sort by balance
        }
        
        logger.info(f"🚀 Moralis: Fetching token holders for {token_address}")
        
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
                
                holders = []
                total_supply = 0
                
                # First pass: calculate total supply
                for holder in data['result']:
                    try:
                        balance = float(holder.get('balance', 0))
                        total_supply += balance
                    except (ValueError, TypeError):
                        continue
                
                # Second pass: create holder list with percentages
                for holder in data['result']:
                    try:
                        balance = float(holder.get('balance', 0))
                        percentage = (balance / total_supply * 100) if total_supply > 0 else 0
                        
                        holders.append({
                            'address': holder.get('owner_address', ''),
                            'balance': balance,
                            'percentage': round(percentage, 4)
                        })
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error parsing holder: {e}")
                        continue
                
                logger.info(f"✅ Moralis: Found {len(holders)} token holders")
                return holders
                
    except Exception as e:
        logger.error(f"Moralis API error: {e}")
        return None


async def get_holders_etherscan_v2(
    token_address: str,
    chain: str,
    api_key: str,
    limit: int = 100
) -> Optional[List[Dict[str, Any]]]:
    """
    Etherscan V2 fallback
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
        
        # Try token transfers as fallback (tokenholderlist is PRO only)
        logger.info(f"🔄 Etherscan: Analyzing token transfers for holders...")
        
        params = {
            'chainid': chainid,
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': token_address,
            'page': 1,
            'offset': 1000,
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
                    # Aggregate holders from transfers
                    holder_balances = {}
                    
                    for tx in data['result']:
                        to_addr = tx.get('to', '').lower()
                        from_addr = tx.get('from', '').lower()
                        
                        try:
                            value = float(tx.get('value', 0))
                        except (ValueError, TypeError):
                            continue
                        
                        # Track transfers
                        if to_addr and to_addr != '0x0000000000000000000000000000000000000000':
                            holder_balances[to_addr] = holder_balances.get(to_addr, 0) + value
                        
                        if from_addr and from_addr != '0x0000000000000000000000000000000000000000':
                            holder_balances[from_addr] = holder_balances.get(from_addr, 0) - value
                    
                    # Filter positive balances
                    holders = [
                        {'address': addr, 'balance': balance, 'percentage': 0}
                        for addr, balance in holder_balances.items()
                        if balance > 0
                    ]
                    
                    # Sort by balance
                    holders.sort(key=lambda x: x['balance'], reverse=True)
                    
                    # Calculate percentages
                    total_supply = sum(h['balance'] for h in holders)
                    for holder in holders:
                        holder['percentage'] = round((holder['balance'] / total_supply * 100) if total_supply > 0 else 0, 4)
                    
                    logger.info(f"✅ Etherscan: Found {len(holders)} holders from transfers")
                    return holders[:limit]
                else:
                    logger.warning(f"Etherscan error: {data.get('message', 'Unknown')}")
                    return None
                
    except Exception as e:
        logger.error(f"Etherscan API error: {e}")
        return None


async def execute_get_token_holders(token_address: str, chain: str) -> List[Dict[str, Any]]:
    """
    Get token holders with intelligent provider selection:
    1. Try Moralis first (better, no PRO needed)
    2. Fallback to Etherscan transfer analysis
    
    ✅ Auto-loads API keys from environment
    """
    try:
        logger.info(f"Fetching token holders for {token_address} on {chain}")
        
        # Load API keys
        moralis_key = os.getenv('MORALIS_API_KEY')
        
        if chain == 'ethereum':
            etherscan_key = os.getenv('ETHERSCAN_API_KEY')
        elif chain == 'bsc':
            etherscan_key = os.getenv('BSCSCAN_API_KEY')
        else:
            logger.warning(f"Unsupported chain: {chain}")
            return []
        
        # Strategy 1: Try Moralis (preferred)
        if moralis_key and chain == 'ethereum':  # Moralis only supports ETH mainnet in free tier
            logger.info(f"🚀 Trying Moralis API...")
            result = await get_holders_moralis(token_address, moralis_key, limit=100)
            
            if result is not None and len(result) > 0:
                return result
            
            logger.info(f"⚠️ Moralis failed or returned no holders, trying Etherscan...")
        else:
            logger.debug(f"ℹ️ No Moralis key or non-ETH chain, using Etherscan")
        
        # Strategy 2: Fallback to Etherscan
        if etherscan_key:
            logger.info(f"🔄 Trying Etherscan fallback...")
            result = await get_holders_etherscan_v2(token_address, chain, etherscan_key, limit=100)
            
            if result is not None:
                return result
            
            logger.warning(f"⚠️ Both providers failed")
        else:
            logger.error(f"❌ No Etherscan API key found")
        
        return []
        
    except Exception as e:
        logger.error(f"Error fetching token holders: {e}", exc_info=True)
        return []
