"""
Address Transactions Fetcher - ENHANCED VERSION
✅ Includes token_transfers[] for each transaction
✅ Essential for DEX detection and portfolio analysis
✅ Moralis (Primary) + Etherscan (Fallback)
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
    limit: int = 100
) -> Optional[List[Dict[str, Any]]]:
    """
    ✅ Get wallet transactions via Moralis API
    ✅ INCLUDES token transfers for each transaction
    """
    global _last_moralis_request
    
    try:
        # Rate limiting
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_moralis_request
        
        if time_since_last < _moralis_delay:
            await asyncio.sleep(_moralis_delay - time_since_last)
        
        _last_moralis_request = asyncio.get_event_loop().time()
        
        # Moralis Wallet Transactions API
        url = f"https://deep-index.moralis.io/api/v2/{wallet_address}"
        
        headers = {
            'accept': 'application/json',
            'X-API-Key': api_key
        }
        
        params = {
            'chain': chain,
            'limit': limit,
            'include': 'internal_transactions'  # 🔥 This includes token transfers!
        }
        
        logger.info(f"🚀 Moralis: Fetching transactions for {wallet_address}")
        
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
                
                for tx in data['result']:
                    try:
                        # Parse transaction
                        parsed_tx = {
                            'hash': tx.get('hash', ''),
                            'from': tx.get('from_address', '').lower(),
                            'to': tx.get('to_address', '').lower(),
                            'value': float(tx.get('value', 0)) / 1e18,  # Convert from Wei
                            'timestamp': int(tx.get('block_timestamp', 0)),
                            'block_number': int(tx.get('block_number', 0)),
                            'gas_price': float(tx.get('gas_price', 0)),
                            'gas_used': int(tx.get('receipt_gas_used', 0)),
                            'input': tx.get('input', ''),
                            'nonce': int(tx.get('nonce', 0)),
                            'transaction_index': int(tx.get('transaction_index', 0)),
                            'token_transfers': []  # 🔥 Will be populated below
                        }
                        
                        # 🔥 Parse internal transactions (token transfers)
                        internal_txs = tx.get('internal_transactions', [])
                        if internal_txs:
                            for internal in internal_txs:
                                try:
                                    token_transfer = {
                                        'token_address': internal.get('token_address', '').lower(),
                                        'token_symbol': internal.get('token_symbol', ''),
                                        'token_name': internal.get('token_name', ''),
                                        'from': internal.get('from_address', '').lower(),
                                        'to': internal.get('to_address', '').lower(),
                                        'value': float(internal.get('value', 0)),
                                        'value_raw': internal.get('value', '0'),
                                        'decimals': int(internal.get('token_decimals', 18))
                                    }
                                    parsed_tx['token_transfers'].append(token_transfer)
                                except (ValueError, TypeError) as e:
                                    logger.debug(f"Error parsing internal tx: {e}")
                                    continue
                        
                        # Additional metadata
                        parsed_tx['input_count'] = 1  # Moralis doesn't provide this directly
                        parsed_tx['output_count'] = len(parsed_tx['token_transfers']) + 1 if parsed_tx['value'] > 0 else len(parsed_tx['token_transfers'])
                        
                        transactions.append(parsed_tx)
                        
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error parsing transaction: {e}")
                        continue
                
                logger.info(f"✅ Moralis: Successfully fetched {len(transactions)} transactions")
                return transactions
                
    except Exception as e:
        logger.error(f"Moralis API error: {e}")
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
            logger.warning(f"Unsupported chain: {chain}")
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
                    logger.error(f"Etherscan HTTP Error {response.status}")
                    return None
                
                data = await response.json()
                
                if data.get('status') != '1' or not data.get('result'):
                    logger.warning(f"Etherscan error: {data.get('message', 'Unknown')}")
                    return None
                
                transactions = []
                tx_hashes = []
                
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
                            'token_transfers': []  # Will be populated
                        }
                        
                        transactions.append(parsed_tx)
                        tx_hashes.append(tx.get('hash', ''))
                        
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error parsing transaction: {e}")
                        continue
                
                # 🔥 Now fetch token transfers for these transactions
                logger.info(f"🔄 Etherscan: Fetching token transfers...")
                
                await asyncio.sleep(_etherscan_delay)
                
                params_tokens = {
                    'module': 'account',
                    'action': 'tokentx',
                    'address': wallet_address,
                    'page': 1,
                    'offset': limit * 5,  # More token txs than normal txs
                    'sort': 'desc',
                    'apikey': api_key
                }
                
                async with session.get(base_url, params=params_tokens, timeout=aiohttp.ClientTimeout(total=30)) as response2:
                    if response2.status == 200:
                        token_data = await response2.json()
                        
                        if token_data.get('status') == '1' and token_data.get('result'):
                            # Map token transfers to transactions
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
                            
                            # Attach token transfers to transactions
                            for tx in transactions:
                                tx_hash = tx['hash']
                                if tx_hash in token_transfers_by_hash:
                                    tx['token_transfers'] = token_transfers_by_hash[tx_hash]
                    
                    else:
                        logger.warning(f"Failed to fetch token transfers from Etherscan")
                
                # Calculate input/output counts
                for tx in transactions:
                    tx['input_count'] = 1
                    tx['output_count'] = len(tx['token_transfers']) + (1 if tx['value'] > 0 else 0)
                
                logger.info(f"✅ Etherscan: Successfully fetched {len(transactions)} transactions")
                return transactions
                
    except Exception as e:
        logger.error(f"Etherscan API error: {e}")
        return None


async def execute_get_address_transactions(
    wallet_address: str, 
    chain: str = 'ethereum',
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Get wallet transactions with token transfers included
    
    Strategy:
    1. Try Moralis first (includes token transfers automatically)
    2. Fallback to Etherscan (needs separate call for token transfers)
    
    ✅ Auto-loads API keys from environment
    ✅ Each transaction includes token_transfers[] array
    
    Returns:
    [
        {
            'hash': '0x...',
            'from': '0x...',
            'to': '0x...',
            'value': 0.5,  # ETH/BNB value
            'timestamp': 1234567890,
            'block_number': 12345678,
            'gas_price': 20000000000,
            'gas_used': 21000,
            'input': '0x...',
            'nonce': 42,
            'transaction_index': 5,
            'token_transfers': [  # 🔥 CRITICAL FOR DEX/PORTFOLIO ANALYSIS
                {
                    'token_address': '0x...',
                    'token_symbol': 'USDT',
                    'token_name': 'Tether USD',
                    'from': '0x...',
                    'to': '0x...',
                    'value': 1000.5,
                    'value_raw': '1000500000',
                    'decimals': 6
                },
                ...
            ],
            'input_count': 1,
            'output_count': 2
        },
        ...
    ]
    """
    try:
        logger.info(f"Fetching transactions for wallet {wallet_address} on {chain}")
        
        # Normalize chain names
        chain_map = {
            'ethereum': 'eth',
            'eth': 'eth',
            'bsc': '0x38',
            'binance': '0x38'
        }
        moralis_chain = chain_map.get(chain.lower(), 'eth')
        
        # Load API keys
        moralis_key = os.getenv('MORALIS_API_KEY')
        
        if chain.lower() in ['ethereum', 'eth']:
            etherscan_key = os.getenv('ETHERSCAN_API_KEY')
        elif chain.lower() in ['bsc', 'binance']:
            etherscan_key = os.getenv('BSCSCAN_API_KEY')
        else:
            logger.warning(f"Unsupported chain: {chain}")
            return []
        
        # Strategy 1: Try Moralis (preferred - includes token transfers automatically)
        if moralis_key:
            logger.info(f"🚀 Trying Moralis API...")
            result = await get_transactions_moralis(wallet_address, moralis_key, moralis_chain, limit)
            
            if result is not None and len(result) > 0:
                return result
            
            logger.info(f"⚠️ Moralis failed or returned no transactions, trying Etherscan...")
        else:
            logger.debug(f"ℹ️ No Moralis key, using Etherscan")
        
        # Strategy 2: Fallback to Etherscan (requires separate token transfer call)
        if etherscan_key:
            logger.info(f"🔄 Trying Etherscan fallback...")
            result = await get_transactions_etherscan(wallet_address, chain, etherscan_key, limit)
            
            if result is not None:
                return result
            
            logger.warning(f"⚠️ Both providers failed")
        else:
            logger.error(f"❌ No Etherscan API key found")
        
        return []
        
    except Exception as e:
        logger.error(f"Error fetching transactions: {e}", exc_info=True)
        return []
