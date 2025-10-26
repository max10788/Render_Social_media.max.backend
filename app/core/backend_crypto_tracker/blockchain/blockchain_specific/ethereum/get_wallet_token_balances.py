"""
Wallet Token Balances Fetcher - ULTIMATE FALLBACK VERSION
✅ Primary Moralis Key
✅ Fallback Moralis Key (neu!)
✅ Etherscan as final fallback
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


async def get_token_balances_moralis(
    wallet_address: str,
    api_key: str,
    chain: str = 'eth',
    key_label: str = "Primary"
) -> Optional[List[Dict[str, Any]]]:
    """
    Get wallet token balances via Moralis API
    
    Args:
        key_label: "Primary" or "Fallback" for logging
    """
    global _last_moralis_request
    
    try:
        # Rate limiting
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_moralis_request
        
        if time_since_last < _moralis_delay:
            await asyncio.sleep(_moralis_delay - time_since_last)
        
        _last_moralis_request = asyncio.get_event_loop().time()
        
        url = f"https://deep-index.moralis.io/api/v2/{wallet_address}/erc20"
        
        headers = {
            'accept': 'application/json',
            'X-API-Key': api_key
        }
        
        params = {'chain': chain}
        
        logger.info(f"🚀 Moralis ({key_label}): Fetching token balances for {wallet_address[:10]}...")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 502 or response.status == 503:
                    logger.warning(f"⚠️ Moralis ({key_label}) server error {response.status}")
                    return None
                elif response.status == 429:
                    logger.warning(f"⚠️ Moralis ({key_label}) rate limit exceeded")
                    return None
                elif response.status == 401:
                    logger.error(f"❌ Moralis ({key_label}) authentication failed")
                    return None
                elif response.status != 200:
                    error_text = await response.text()
                    logger.error(f"❌ Moralis ({key_label}) HTTP Error {response.status}: {error_text[:200]}")
                    return None
                
                data = await response.json()
                
                if not data:
                    logger.warning(f"⚠️ Moralis ({key_label}): No data in response")
                    return None
                
                balances = []
                
                for token in data:
                    try:
                        balance_raw = token.get('balance', '0')
                        decimals = int(token.get('decimals', 18))
                        balance = float(balance_raw) / (10 ** decimals)
                        
                        if balance <= 0:
                            continue
                        
                        balances.append({
                            'token_address': token.get('token_address', '').lower(),
                            'symbol': token.get('symbol', 'UNKNOWN'),
                            'name': token.get('name', 'Unknown Token'),
                            'balance': balance,
                            'balance_raw': balance_raw,
                            'decimals': decimals,
                            'logo': token.get('logo'),
                            'thumbnail': token.get('thumbnail')
                        })
                    except (ValueError, TypeError):
                        continue
                
                logger.info(f"✅ Moralis ({key_label}): Found {len(balances)} token balances")
                return balances
                
    except asyncio.TimeoutError:
        logger.warning(f"⏱️ Moralis ({key_label}) API timeout")
        return None
    except aiohttp.ClientError as e:
        logger.warning(f"🌐 Moralis ({key_label}) network error: {type(e).__name__}")
        return None
    except Exception as e:
        logger.error(f"❌ Moralis ({key_label}) API error: {e}")
        return None


async def get_token_balances_etherscan(
    wallet_address: str,
    chain: str,
    api_key: str
) -> Optional[List[Dict[str, Any]]]:
    """
    Etherscan fallback - uses token transfer analysis
    """
    global _last_etherscan_request
    
    try:
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_etherscan_request
        
        if time_since_last < _etherscan_delay:
            await asyncio.sleep(_etherscan_delay - time_since_last)
        
        _last_etherscan_request = asyncio.get_event_loop().time()
        
        if chain == 'ethereum':
            base_url = "https://api.etherscan.io/api"
        elif chain == 'bsc':
            base_url = "https://api.bscscan.com/api"
        else:
            logger.warning(f"⚠️ Unsupported chain: {chain}")
            return None
        
        logger.info(f"🔄 Etherscan: Fetching token transfers for balance calculation...")
        
        params = {
            'module': 'account',
            'action': 'tokentx',
            'address': wallet_address,
            'page': 1,
            'offset': 1000,
            'sort': 'desc',
            'apikey': api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.error(f"❌ Etherscan HTTP Error {response.status}")
                    return None
                
                data = await response.json()
                
                if data.get('status') == '1' and data.get('result'):
                    token_balances = {}
                    token_info = {}
                    
                    for tx in data['result']:
                        token_addr = tx.get('contractAddress', '').lower()
                        to_addr = tx.get('to', '').lower()
                        from_addr = tx.get('from', '').lower()
                        
                        try:
                            value_raw = float(tx.get('value', 0))
                            decimals = int(tx.get('tokenDecimal', 18))
                            value = value_raw / (10 ** decimals)
                        except (ValueError, TypeError):
                            continue
                        
                        if token_addr not in token_info:
                            token_info[token_addr] = {
                                'symbol': tx.get('tokenSymbol', 'UNKNOWN'),
                                'name': tx.get('tokenName', 'Unknown Token'),
                                'decimals': decimals
                            }
                        
                        if token_addr not in token_balances:
                            token_balances[token_addr] = 0
                        
                        if to_addr == wallet_address.lower():
                            token_balances[token_addr] += value
                        elif from_addr == wallet_address.lower():
                            token_balances[token_addr] -= value
                    
                    balances = []
                    for token_addr, balance in token_balances.items():
                        if balance > 0:
                            info = token_info.get(token_addr, {})
                            balances.append({
                                'token_address': token_addr,
                                'symbol': info.get('symbol', 'UNKNOWN'),
                                'name': info.get('name', 'Unknown Token'),
                                'balance': balance,
                                'balance_raw': str(int(balance * (10 ** info.get('decimals', 18)))),
                                'decimals': info.get('decimals', 18),
                                'logo': None,
                                'thumbnail': None
                            })
                    
                    balances.sort(key=lambda x: x['balance'], reverse=True)
                    
                    logger.info(f"✅ Etherscan: Found {len(balances)} token balances")
                    return balances
                else:
                    logger.warning(f"⚠️ Etherscan error: {data.get('message', 'Unknown')}")
                    return None
                
    except asyncio.TimeoutError:
        logger.error(f"⏱️ Etherscan API timeout")
        return None
    except Exception as e:
        logger.error(f"❌ Etherscan API error: {e}")
        return None


async def execute_get_wallet_token_balances(
    wallet_address: str, 
    chain: str = 'ethereum'
) -> List[Dict[str, Any]]:
    """
    Get all ERC20 token balances with TRIPLE FALLBACK system
    
    Strategy:
    1. Try Moralis with PRIMARY key
    2. Try Moralis with FALLBACK key (🆕)
    3. Try Etherscan as final fallback
    
    ✅ Maximum reliability with dual Moralis keys
    """
    try:
        logger.info(f"📊 Fetching token balances for wallet {wallet_address[:10]}... on {chain}")
        
        # Normalize chain names
        chain_map = {
            'ethereum': 'eth',
            'eth': 'eth',
            'bsc': '0x38',
            'binance': '0x38'
        }
        moralis_chain = chain_map.get(chain.lower(), 'eth')
        
        # Load API keys
        moralis_key_primary = os.getenv('MORALIS_API_KEY')
        moralis_key_fallback = os.getenv('MORALIS_API_KEY_FALLBACK')  # 🆕
        
        if chain.lower() in ['ethereum', 'eth']:
            etherscan_key = os.getenv('ETHERSCAN_API_KEY')
        elif chain.lower() in ['bsc', 'binance']:
            etherscan_key = os.getenv('BSCSCAN_API_KEY')
        else:
            logger.warning(f"⚠️ Unsupported chain: {chain}")
            return []
        
        # ===== STRATEGY 1: Try PRIMARY Moralis Key =====
        if moralis_key_primary:
            logger.info(f"🚀 Trying Moralis API (Primary Key)...")
            result = await get_token_balances_moralis(
                wallet_address, 
                moralis_key_primary, 
                moralis_chain,
                key_label="Primary"
            )
            
            if result is not None and len(result) > 0:
                logger.info(f"✅ Primary Moralis succeeded with {len(result)} token balances")
                return result
            
            logger.info(f"⚠️ Primary Moralis failed, trying fallback key...")
        else:
            logger.warning(f"⚠️ No primary Moralis API key found")
        
        # ===== STRATEGY 2: Try FALLBACK Moralis Key 🆕 =====
        if moralis_key_fallback:
            logger.info(f"🔄 Trying Moralis API (Fallback Key)...")
            result = await get_token_balances_moralis(
                wallet_address, 
                moralis_key_fallback, 
                moralis_chain,
                key_label="Fallback"
            )
            
            if result is not None and len(result) > 0:
                logger.info(f"✅ Fallback Moralis succeeded with {len(result)} token balances")
                return result
            
            logger.info(f"⚠️ Fallback Moralis also failed, trying Etherscan...")
        else:
            logger.warning(f"⚠️ No fallback Moralis API key found")
        
        # ===== STRATEGY 3: Try Etherscan (Final Fallback) =====
        if etherscan_key:
            logger.info(f"🔄 Trying Etherscan API (final fallback)...")
            result = await get_token_balances_etherscan(wallet_address, chain, etherscan_key)
            
            if result is not None:
                if len(result) > 0:
                    logger.info(f"✅ Etherscan succeeded with {len(result)} token balances")
                else:
                    logger.info(f"ℹ️ Etherscan succeeded but no token balances found")
                return result
            
            logger.warning(f"⚠️ All three providers failed")
        else:
            logger.error(f"❌ No Etherscan API key found")
        
        logger.warning(f"⚠️ All providers exhausted - returning empty list")
        return []
        
    except Exception as e:
        logger.error(f"❌ Critical error fetching token balances: {e}", exc_info=True)
        return []
