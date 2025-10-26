"""
Token Price Fetcher (Bulk)
✅ Get USD prices for multiple tokens at once
✅ Moralis (Primary) + CoinGecko (Fallback)
"""

from typing import List, Dict, Any, Optional
import aiohttp
import os
import asyncio
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# Rate limiters
_last_moralis_request = 0
_last_coingecko_request = 0
_moralis_delay = 0.04     # 40ms = 25 calls/sec
_coingecko_delay = 1.5    # 1.5s for free tier


async def get_token_prices_moralis(
    token_addresses: List[str],
    api_key: str,
    chain: str = 'eth'
) -> Dict[str, float]:
    """
    ✅ Get token prices via Moralis (supports bulk)
    """
    global _last_moralis_request
    
    prices = {}
    
    try:
        # Moralis can handle multiple tokens, but we'll do them individually
        # for better error handling
        for token_address in token_addresses:
            try:
                # Rate limiting
                current_time = asyncio.get_event_loop().time()
                time_since_last = current_time - _last_moralis_request
                
                if time_since_last < _moralis_delay:
                    await asyncio.sleep(_moralis_delay - time_since_last)
                
                _last_moralis_request = asyncio.get_event_loop().time()
                
                # Moralis Token Price API
                url = f"https://deep-index.moralis.io/api/v2/erc20/{token_address}/price"
                
                headers = {
                    'accept': 'application/json',
                    'X-API-Key': api_key
                }
                
                params = {
                    'chain': chain,
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=15)) as response:
                        if response.status == 200:
                            data = await response.json()
                            price = float(data.get('usdPrice', 0))
                            
                            if price > 0:
                                prices[token_address.lower()] = price
                                logger.debug(f"✅ Moralis: {token_address} = ${price}")
                        elif response.status == 404:
                            # Token not found, skip
                            logger.debug(f"⚠️ Moralis: Token {token_address} not found")
                        else:
                            error_text = await response.text()
                            logger.warning(f"Moralis HTTP Error {response.status} for {token_address}: {error_text}")
                
            except Exception as e:
                logger.warning(f"Error fetching price for {token_address} via Moralis: {e}")
                continue
        
        logger.info(f"✅ Moralis: Fetched {len(prices)}/{len(token_addresses)} token prices")
        return prices
        
    except Exception as e:
        logger.error(f"Moralis bulk price API error: {e}")
        return prices


async def get_token_prices_coingecko(
    token_addresses: List[str],
    chain: str = 'ethereum'
) -> Dict[str, float]:
    """
    CoinGecko fallback - free tier, supports bulk (up to 250 tokens)
    """
    global _last_coingecko_request
    
    prices = {}
    
    try:
        # Rate limiting
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - _last_coingecko_request
        
        if time_since_last < _coingecko_delay:
            await asyncio.sleep(_coingecko_delay - time_since_last)
        
        _last_coingecko_request = asyncio.get_event_loop().time()
        
        # Map chain to platform
        platform_map = {
            'ethereum': 'ethereum',
            'eth': 'ethereum',
            'bsc': 'binance-smart-chain',
            'binance': 'binance-smart-chain',
            'polygon': 'polygon-pos',
            'avalanche': 'avalanche'
        }
        
        platform = platform_map.get(chain.lower(), 'ethereum')
        
        # CoinGecko simple price endpoint
        url = "https://api.coingecko.com/api/v3/simple/token_price/" + platform
        
        # Join addresses
        addresses_param = ','.join([addr.lower() for addr in token_addresses])
        
        params = {
            'contract_addresses': addresses_param,
            'vs_currencies': 'usd'
        }
        
        logger.info(f"🦎 CoinGecko: Fetching prices for {len(token_addresses)} tokens")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"CoinGecko HTTP Error {response.status}: {error_text}")
                    return prices
                
                data = await response.json()
                
                # Parse response
                for token_addr, price_data in data.items():
                    if isinstance(price_data, dict) and 'usd' in price_data:
                        price = float(price_data['usd'])
                        if price > 0:
                            prices[token_addr.lower()] = price
                
                logger.info(f"✅ CoinGecko: Fetched {len(prices)}/{len(token_addresses)} token prices")
                return prices
        
    except Exception as e:
        logger.error(f"CoinGecko API error: {e}")
        return prices


async def get_hardcoded_prices() -> Dict[str, float]:
    """
    Hardcoded prices for well-known stablecoins and major tokens
    """
    return {
        # Stablecoins
        '0xdac17f958d2ee523a2206206994597c13d831ec7': 1.0,  # USDT (ETH)
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 1.0,  # USDC (ETH)
        '0x6b175474e89094c44da98b954eedeac495271d0f': 1.0,  # DAI (ETH)
        '0x4fabb145d64652a948d72533023f6e7a623c7c53': 1.0,  # BUSD (ETH)
        '0x55d398326f99059ff775485246999027b3197955': 1.0,  # USDT (BSC)
        '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d': 1.0,  # USDC (BSC)
        '0xe9e7cea3dedca5984780bafc599bd69add087d56': 1.0,  # BUSD (BSC)
        '0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3': 1.0,  # DAI (BSC)
        
        # Major tokens (fallback approximations - should be updated dynamically)
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 3500.0,  # WETH
        '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599': 65000.0, # WBTC
    }


async def execute_get_token_prices_bulk(
    token_addresses: List[str], 
    chain: str = 'ethereum'
) -> Dict[str, float]:
    """
    Get USD prices for multiple tokens at once
    
    Strategy:
    1. Start with hardcoded prices for stablecoins
    2. Try Moralis for remaining tokens
    3. Fallback to CoinGecko for missing prices
    
    ✅ Auto-loads API keys from environment
    
    Args:
        token_addresses: List of token contract addresses
        chain: Blockchain name ('ethereum', 'bsc', etc.)
    
    Returns:
        Dict mapping token address (lowercase) to USD price
        {
            '0xdac17f958d2ee523a2206206994597c13d831ec7': 1.0,  # USDT
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 1.0,  # USDC
            ...
        }
    """
    try:
        if not token_addresses:
            logger.warning("No token addresses provided")
            return {}
        
        logger.info(f"Fetching prices for {len(token_addresses)} tokens on {chain}")
        
        # Normalize addresses
        token_addresses = [addr.lower() for addr in token_addresses]
        
        # Start with hardcoded prices
        prices = await get_hardcoded_prices()
        
        # Find missing prices
        missing_addresses = [addr for addr in token_addresses if addr not in prices]
        
        if not missing_addresses:
            logger.info(f"✅ All prices available from hardcoded values")
            return {addr: prices[addr] for addr in token_addresses if addr in prices}
        
        logger.info(f"📊 Need to fetch prices for {len(missing_addresses)} tokens")
        
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
        
        # Strategy 1: Try Moralis
        if moralis_key:
            logger.info(f"🚀 Trying Moralis API...")
            moralis_prices = await get_token_prices_moralis(
                missing_addresses, 
                moralis_key, 
                moralis_chain
            )
            prices.update(moralis_prices)
            
            # Update missing list
            missing_addresses = [addr for addr in missing_addresses if addr not in prices]
        
        # Strategy 2: Try CoinGecko for remaining
        if missing_addresses:
            logger.info(f"🦎 Trying CoinGecko for {len(missing_addresses)} remaining tokens...")
            coingecko_prices = await get_token_prices_coingecko(
                missing_addresses,
                chain
            )
            prices.update(coingecko_prices)
            
            # Final missing count
            missing_addresses = [addr for addr in missing_addresses if addr not in prices]
        
        # Log summary
        found_count = len(token_addresses) - len(missing_addresses)
        logger.info(f"✅ Price fetch complete: {found_count}/{len(token_addresses)} tokens")
        
        if missing_addresses:
            logger.warning(f"⚠️ Missing prices for {len(missing_addresses)} tokens")
            for addr in missing_addresses[:5]:  # Show first 5
                logger.debug(f"   - {addr}")
        
        # Return only requested tokens
        return {addr: prices[addr] for addr in token_addresses if addr in prices}
        
    except Exception as e:
        logger.error(f"Error fetching token prices: {e}", exc_info=True)
        return {}


async def execute_get_token_price(
    token_address: str, 
    chain: str = 'ethereum'
) -> float:
    """
    Get USD price for a single token (convenience wrapper)
    
    Returns:
        USD price as float, or 0.0 if not found
    """
    prices = await execute_get_token_prices_bulk([token_address], chain)
    return prices.get(token_address.lower(), 0.0)
