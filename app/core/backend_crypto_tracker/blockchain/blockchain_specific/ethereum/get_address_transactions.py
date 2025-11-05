"""
Address Transactions Fetcher - ULTIMATE FALLBACK VERSION v2
✅ Primary Moralis Key
✅ Fallback Moralis Key #1
✅ Fallback Moralis Key #2 (🆕 THIRD KEY)
✅ Etherscan as final fallback
✅ Includes token_transfers[] for each transaction
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


async def get_transactions_moralis(
    wallet_address: str,
    api_key: str,
    chain: str = 'eth',
    limit: int = 100,
    key_label: str = "Primary"
) -> Optional[List[Dict[str, Any]]]:
    """
    Get wallet transactions via Moralis API
    ✅ FIXED: Now uses /erc20/transfers endpoint
    
    Args:
        key_label: "Primary", "Fallback-1", or "Fallback-2" for logging
    """
    global _last_moralis_request
    
    try:
        # Rate limiting
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_moralis_request
        
        if time_since_last < _moralis_delay:
            await asyncio.sleep(_moralis_delay - time_since_last)
        
        _last_moralis_request = asyncio.get_event_loop().time()
        
        # ✅ FIXED: Use ERC20 transfers endpoint
        url = f"https://deep-index.moralis.io/api/v2/{wallet_address}/erc20/transfers"
        
        headers = {
            'accept': 'application/json',
            'X-API-Key': api_key
        }
        
        params = {
            'chain': chain,
            'limit': limit
        }
        
        logger.info(f"🚀 Moralis ({key_label}): Fetching transactions for {wallet_address[:10]}...")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 502 or response.status == 503:
                    logger.warning(f"⚠️ Moralis ({key_label}) server error {response.status} - service unavailable")
                    return None
                elif response.status == 429:
                    logger.warning(f"⚠️ Moralis ({key_label}) rate limit exceeded")
                    return None
                elif response.status == 401:
                    logger.error(f"❌ Moralis ({key_label}) authentication failed - invalid API key")
                    return None
                elif response.status != 200:
                    error_text = await response.text()
                    logger.error(f"❌ Moralis ({key_label}) HTTP Error {response.status}: {error_text[:200]}")
                    return None
                
                data = await response.json()
                
                if not data or 'result' not in data:
                    logger.warning(f"⚠️ Moralis ({key_label}): No result field in response")
                    return None
                
                transactions = []
                
                # ✅ Parse ERC20 transfers as transactions
                for transfer in data['result']:
                    try:
                        # Extract timestamp
                        timestamp_str = transfer.get('block_timestamp', '')
                        if timestamp_str:
                            from datetime import datetime
                            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            timestamp = int(dt.timestamp())
                        else:
                            timestamp = 0
                        
                        # Extract value with decimals
                        decimals = int(transfer.get('token_decimals', 18))
                        value_raw = float(transfer.get('value', 0))
                        value = value_raw / (10 ** decimals)
                        
                        # Build transaction with token transfer
                        parsed_tx = {
                            'hash': transfer.get('transaction_hash', ''),
                            'from': transfer.get('from_address', '').lower(),
                            'to': transfer.get('to_address', '').lower(),
                            'value': 0,  # ERC20 transfers don't have native ETH value
                            'timestamp': timestamp,
                            'block_number': int(transfer.get('block_number', 0)),
                            'gas_price': 0,
                            'gas_used': 0,
                            'input': '',
                            'nonce': 0,
                            'transaction_index': int(transfer.get('transaction_index', 0)),
                            'token_transfers': [{
                                'token_address': transfer.get('address', '').lower(),
                                'token_symbol': transfer.get('token_symbol', ''),
                                'token_name': transfer.get('token_name', ''),
                                'from': transfer.get('from_address', '').lower(),
                                'to': transfer.get('to_address', '').lower(),
                                'value': value,
                                'value_raw': str(int(value_raw)),
                                'decimals': decimals,
                                'value_usd': transfer.get('value_usd', 0)
                            }]
                        }
                        
                        parsed_tx['input_count'] = 1
                        parsed_tx['output_count'] = 1
                        
                        transactions.append(parsed_tx)
                        
                    except (ValueError, TypeError) as e:
                        logger.debug(f"⚠️ Error parsing transfer: {e}")
                        continue
                
                logger.info(f"✅ Moralis ({key_label}): Successfully fetched {len(transactions)} transactions")
                return transactions
                
    except asyncio.TimeoutError:
        logger.warning(f"⏱️ Moralis ({key_label}) API timeout")
        return None
    except aiohttp.ClientError as e:
        logger.warning(f"🌐 Moralis ({key_label}) network error: {type(e).__name__}")
        return None
    except Exception as e:
        logger.error(f"❌ Moralis ({key_label}) API error: {e}")
        return None


async def get_transactions_etherscan(
    wallet_address: str,
    chain: str,
    api_key: str,
    limit: int = 100
) -> Optional[List[Dict[str, Any]]]:
    """
    Etherscan fallback with token transfers
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
            base_url = "https://api.etherscan.io/api"
        elif chain == 'bsc':
            base_url = "https://api.bscscan.com/api"
        else:
            logger.warning(f"⚠️ Unsupported chain: {chain}")
            return None
        
        logger.info(f"🔄 Etherscan: Fetching transactions...")
        
        # Get normal transactions
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': wallet_address,
            'page': 1,
            'offset': limit,
            'sort': 'desc',
            'apikey': api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.error(f"❌ Etherscan HTTP Error {response.status}")
                    return None
                
                data = await response.json()
                
                if data.get('status') != '1' or not data.get('result'):
                    logger.warning(f"⚠️ Etherscan error: {data.get('message', 'Unknown')}")
                    return None
                
                transactions = []
                
                for tx in data['result']:
                    try:
                        parsed_tx = {
                            'hash': tx.get('hash', ''),
                            'from': tx.get('from', '').lower(),
                            'to': tx.get('to', '').lower(),
                            'value': float(tx.get('value', 0)) / 1e18,
                            'timestamp': int(tx.get('timeStamp', 0)),
                            'block_number': int(tx.get('blockNumber', 0)),
                            'gas_price': float(tx.get('gasPrice', 0)),
                            'gas_used': int(tx.get('gasUsed', 0)),
                            'input': tx.get('input', ''),
                            'nonce': int(tx.get('nonce', 0)),
                            'transaction_index': int(tx.get('transactionIndex', 0)),
                            'token_transfers': []
                        }
                        transactions.append(parsed_tx)
                    except (ValueError, TypeError):
                        continue
                
                # Fetch token transfers
                await asyncio.sleep(_etherscan_delay)
                
                params_tokens = {
                    'module': 'account',
                    'action': 'tokentx',
                    'address': wallet_address,
                    'page': 1,
                    'offset': limit * 5,
                    'sort': 'desc',
                    'apikey': api_key
                }
                
                async with session.get(base_url, params=params_tokens, timeout=aiohttp.ClientTimeout(total=30)) as response2:
                    if response2.status == 200:
                        token_data = await response2.json()
                        
                        if token_data.get('status') == '1' and token_data.get('result'):
                            token_transfers_by_hash = {}
                            
                            for token_tx in token_data['result']:
                                tx_hash = token_tx.get('hash', '')
                                if tx_hash not in token_transfers_by_hash:
                                    token_transfers_by_hash[tx_hash] = []
                                
                                try:
                                    decimals = int(token_tx.get('tokenDecimal', 18))
                                    value_raw = float(token_tx.get('value', 0))
                                    value = value_raw / (10 ** decimals)
                                    
                                    token_transfer = {
                                        'token_address': token_tx.get('contractAddress', '').lower(),
                                        'token_symbol': token_tx.get('tokenSymbol', ''),
                                        'token_name': token_tx.get('tokenName', ''),
                                        'from': token_tx.get('from', '').lower(),
                                        'to': token_tx.get('to', '').lower(),
                                        'value': value,
                                        'value_raw': str(int(value_raw)),
                                        'decimals': decimals
                                    }
                                    token_transfers_by_hash[tx_hash].append(token_transfer)
                                except (ValueError, TypeError):
                                    continue
                            
                            for tx in transactions:
                                tx_hash = tx['hash']
                                if tx_hash in token_transfers_by_hash:
                                    tx['token_transfers'] = token_transfers_by_hash[tx_hash]
                
                # Calculate counts
                for tx in transactions:
                    tx['input_count'] = 1
                    tx['output_count'] = len(tx['token_transfers']) + (1 if tx['value'] > 0 else 0)
                
                logger.info(f"✅ Etherscan: Successfully processed {len(transactions)} transactions")
                return transactions
                
    except asyncio.TimeoutError:
        logger.error(f"⏱️ Etherscan API timeout")
        return None
    except Exception as e:
        logger.error(f"❌ Etherscan API error: {e}")
        return None


async def execute_get_address_transactions(
    wallet_address: str, 
    chain: str = 'ethereum',
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Get wallet transactions with QUADRUPLE FALLBACK system
    
    Strategy:
    1. Try Moralis with PRIMARY key
    2. Try Moralis with FALLBACK key #1
    3. Try Moralis with FALLBACK key #2 (🆕 THIRD KEY)
    4. Try Etherscan as final fallback
    
    ✅ Maximum reliability with TRIPLE Moralis keys
    """
    try:
        logger.info(f"📡 Fetching transactions for wallet {wallet_address[:10]}... on {chain}")
        
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
        moralis_key_fallback1 = os.getenv('MORALIS_API_KEY_FALLBACK')
        moralis_key_fallback2 = os.getenv('MORALIS_API_KEY_FALLBACK2')  # 🆕 THIRD KEY
        
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
            result = await get_transactions_moralis(
                wallet_address, 
                moralis_key_primary, 
                moralis_chain, 
                limit,
                key_label="Primary"
            )
            
            if result is not None and len(result) > 0:
                logger.info(f"✅ Primary Moralis succeeded with {len(result)} transactions")
                return result
            
            logger.info(f"⚠️ Primary Moralis failed, trying fallback key #1...")
        else:
            logger.warning(f"⚠️ No primary Moralis API key found")
        
        # ===== STRATEGY 2: Try FALLBACK Moralis Key #1 =====
        if moralis_key_fallback1:
            logger.info(f"🔄 Trying Moralis API (Fallback Key #1)...")
            result = await get_transactions_moralis(
                wallet_address, 
                moralis_key_fallback1, 
                moralis_chain, 
                limit,
                key_label="Fallback-1"
            )
            
            if result is not None and len(result) > 0:
                logger.info(f"✅ Fallback-1 Moralis succeeded with {len(result)} transactions")
                return result
            
            logger.info(f"⚠️ Fallback-1 Moralis also failed, trying fallback key #2...")
        else:
            logger.warning(f"⚠️ No fallback-1 Moralis API key found")
        
        # ===== STRATEGY 3: Try FALLBACK Moralis Key #2 🆕 =====
        if moralis_key_fallback2:
            logger.info(f"🔄 Trying Moralis API (Fallback Key #2)...")
            result = await get_transactions_moralis(
                wallet_address, 
                moralis_key_fallback2, 
                moralis_chain, 
                limit,
                key_label="Fallback-2"
            )
            
            if result is not None and len(result) > 0:
                logger.info(f"✅ Fallback-2 Moralis succeeded with {len(result)} transactions")
                return result
            
            logger.info(f"⚠️ All Moralis keys exhausted, trying Etherscan...")
        else:
            logger.warning(f"⚠️ No fallback-2 Moralis API key found")
        
        # ===== STRATEGY 4: Try Etherscan (Final Fallback) =====
        if etherscan_key:
            logger.info(f"🔄 Trying Etherscan API (final fallback)...")
            result = await get_transactions_etherscan(wallet_address, chain, etherscan_key, limit)
            
            if result is not None:
                if len(result) > 0:
                    logger.info(f"✅ Etherscan succeeded with {len(result)} transactions")
                else:
                    logger.info(f"ℹ️ Etherscan succeeded but no transactions found")
                return result
            
            logger.warning(f"⚠️ All four providers failed")
        else:
            logger.error(f"❌ No Etherscan API key found")
        
        logger.warning(f"⚠️ All providers exhausted - returning empty list")
        return []
        
    except Exception as e:
        logger.error(f"❌ Critical error fetching transactions: {e}", exc_info=True)
        return []
