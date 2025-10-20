"""
Ethereum Token Holders Fetcher
✅ FIXED: Updated to Etherscan API V2
"""

from typing import List, Dict, Any, Optional
import aiohttp
import os
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def test_api_key_v2(api_key: str, chain: str) -> Dict[str, Any]:
    """
    ✅ V2: Test if API key is valid using V2 endpoint
    """
    if chain == 'ethereum':
        base_url = "https://api.etherscan.io/v2/api"
    elif chain == 'bsc':
        base_url = "https://api.bscscan.com/v2/api"
    else:
        return {'valid': False, 'message': 'Unsupported chain', 'result': ''}
    
    # Simple test request - get ETH balance for a known address (Vitalik's address)
    test_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
    params = {
        'chainid': '1',  # V2 requires chainid
        'module': 'account',
        'action': 'balance',
        'address': test_address,
        'tag': 'latest',
        'apikey': api_key
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'valid': data.get('status') == '1',
                        'message': data.get('message', ''),
                        'result': data.get('result', '')
                    }
    except Exception as e:
        logger.error(f"API key test failed: {e}")
    
    return {'valid': False, 'message': 'Test request failed', 'result': ''}


async def get_holders_from_transfers_v2(token_address: str, chain: str, api_key: str, base_url: str, chainid: str) -> List[Dict[str, Any]]:
    """
    ✅ V2: Fallback method - Get holders by analyzing token transfers
    """
    try:
        logger.info(f"Trying fallback method: analyzing token transfers (V2 API)...")
        
        # Get recent token transfers
        params = {
            'chainid': chainid,
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': token_address,
            'page': 1,
            'offset': 1000,  # Get 1000 recent transfers
            'sort': 'desc',
            'apikey': api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 200:
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
                            if to_addr and to_addr not in ['0x0000000000000000000000000000000000000000']:
                                holder_balances[to_addr] = holder_balances.get(to_addr, 0) + value
                            
                            if from_addr and from_addr not in ['0x0000000000000000000000000000000000000000']:
                                holder_balances[from_addr] = holder_balances.get(from_addr, 0) - value
                        
                        # Filter positive balances and sort
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
                        
                        logger.info(f"✅ Found {len(holders)} holders from transfers (approximate)")
                        return holders[:100]  # Return top 100
                    else:
                        logger.warning(f"Transfer method failed: {data.get('message', 'Unknown')}")
                        return []
                
                return []
                
    except Exception as e:
        logger.error(f"Fallback transfer method failed: {e}")
        return []


async def execute_get_token_holders(token_address: str, chain: str) -> List[Dict[str, Any]]:
    """
    Holt Token Holders für einen ERC20 Token auf Ethereum
    ✅ UPDATED: Now uses Etherscan API V2
    
    Args:
        token_address: ERC20 Token Contract Adresse
        chain: Blockchain ('ethereum' oder 'bsc')
    
    Returns:
        Liste von Holder-Dictionaries mit address, balance, percentage
    """
    try:
        logger.info(f"Fetching token holders for {token_address} on {chain}")
        
        # Configure API endpoints and chain IDs
        if chain == 'ethereum':
            api_key = os.getenv('ETHERSCAN_API_KEY')
            base_url = "https://api.etherscan.io/v2/api"
            chainid = '1'  # Ethereum Mainnet
        elif chain == 'bsc':
            api_key = os.getenv('BSCSCAN_API_KEY')
            base_url = "https://api.bscscan.com/v2/api"
            chainid = '56'  # BSC Mainnet
        else:
            logger.warning(f"Unsupported chain for token holders: {chain}")
            return []
        
        if not api_key:
            logger.warning(f"⚠️ No API key found for {chain} - cannot fetch holders")
            logger.info(f"Please set {'ETHERSCAN_API_KEY' if chain == 'ethereum' else 'BSCSCAN_API_KEY'} environment variable")
            return []
        
        # ✅ V2: Test API key first
        logger.info(f"Testing {chain} API key (V2)...")
        key_test = await test_api_key_v2(api_key, chain)
        if not key_test['valid']:
            logger.error(f"❌ API key validation failed for {chain}!")
            logger.error(f"API Response: {key_test['message']}")
            logger.info(f"Get a free API key at: https://{'etherscan.io' if chain == 'ethereum' else 'bscscan.com'}/register")
            return []
        else:
            logger.info(f"✅ API key is valid for {chain} (V2)")
        
        # Method 1: Try tokenholderlist endpoint (V2)
        logger.info(f"Method 1: Trying tokenholderlist endpoint (V2)...")
        params = {
            'chainid': chainid,
            'module': 'token',
            'action': 'tokenholderlist',
            'contractaddress': token_address,
            'page': 1,
            'offset': 100,  # Top 100 Holders
            'apikey': api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                base_url, 
                params=params, 
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Log API response
                    logger.info(f"API V2 Response Status: {data.get('status')}")
                    logger.info(f"API V2 Response Message: {data.get('message')}")
                    
                    # Success case
                    if data.get('status') == '1' and data.get('result'):
                        holders = []
                        total_supply = 0
                        
                        # Calculate Total Supply from all Holders
                        for holder in data['result']:
                            try:
                                balance = float(holder.get('TokenHolderQuantity', 0))
                                total_supply += balance
                            except (ValueError, TypeError):
                                continue
                        
                        # Create Holder list with Percentage
                        for holder in data['result']:
                            try:
                                balance = float(holder.get('TokenHolderQuantity', 0))
                                percentage = (balance / total_supply * 100) if total_supply > 0 else 0
                                
                                holders.append({
                                    'address': holder.get('TokenHolderAddress', ''),
                                    'balance': balance,
                                    'percentage': round(percentage, 4)
                                })
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Error parsing holder data: {e}")
                                continue
                        
                        logger.info(f"✅ Successfully fetched {len(holders)} holders for {token_address}")
                        return holders
                    
                    # Error case
                    elif data.get('status') == '0':
                        error_msg = data.get('message', 'Unknown error')
                        result = data.get('result', '')
                        
                        logger.warning(f"❌ Etherscan API V2 Error:")
                        logger.warning(f"   Status: {data.get('status')}")
                        logger.warning(f"   Message: {error_msg}")
                        logger.warning(f"   Result: {result}")
                        
                        # Check specific error types
                        if 'rate limit' in error_msg.lower():
                            logger.warning("⏱️ Rate limit exceeded - please wait before retry")
                            return []
                        
                        if 'invalid' in error_msg.lower() and 'api' in error_msg.lower():
                            logger.error("❌ Invalid API key - check your ETHERSCAN_API_KEY")
                            return []
                        
                        # If tokenholderlist fails, try fallback method
                        if 'notok' in error_msg.lower() or 'no transactions found' in str(result).lower() or 'no data found' in str(result).lower():
                            logger.warning(f"Token {token_address} might not have holder data on Etherscan")
                            logger.info(f"Trying fallback method (analyze transfers)...")
                            
                            # Try fallback method
                            return await get_holders_from_transfers_v2(token_address, chain, api_key, base_url, chainid)
                        
                        return []
                    
                    else:
                        logger.warning(f"Unexpected Etherscan V2 response: {data}")
                        return []
                
                elif response.status == 429:
                    logger.warning(f"⏱️ Rate limit exceeded for {chain} API")
                    return []
                
                elif response.status == 403:
                    logger.error(f"❌ Access forbidden - check your API key permissions")
                    return []
                
                else:
                    logger.warning(f"{chain} API returned HTTP {response.status}")
                    text = await response.text()
                    logger.debug(f"Response body: {text[:200]}")
                    return []
                    
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching token holders: {e}")
        return []
    
    except Exception as e:
        logger.error(f"Error fetching token holders for {token_address}: {e}", exc_info=True)
        return []
