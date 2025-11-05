"""
Token Price Fetcher (Bulk) - ULTIMATE FALLBACK VERSION v2
✅ Primary Moralis Key
✅ Fallback Moralis Key #1
✅ Fallback Moralis Key #2 (🆕 THIRD KEY)
✅ CoinGecko as final fallback
✅ Hardcoded stablecoin prices
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
    chain: str = 'eth',
    key_label: str = "Primary"
) -> Dict[str, float]:
    """
    Get token prices via Moralis
    
    Args:
        key_label: "Primary", "Fallback-1", or "Fallback-2" for logging
    """
    global _last_moralis_request
    
    prices = {}
    
    try:
        for token_address in token_addresses:
            try:
                # Rate limiting
                current_time = asyncio.get_event_loop().time()
                time_since_last = current_time - _last_moralis_request
                
                if time_since_last < _moralis_delay:
                    await asyncio.sleep(_moralis_delay - time_since_last)
                
                _last_moralis_request = asyncio.get_event_loop().time()
                
                url = f"https://deep-index.moralis.io/api/v2/erc20/{token_address}/price"
                
                headers = {
                    'accept': 'application/json',
                    'X-API-Key': api_key
                }
                
                params = {'chain': chain}
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=15)) as response:
                        if response.status == 200:
                            data = await response.json()
                            price = float(data.get('usdPrice', 0))
                            
                            if price > 0:
                                prices[token_address.lower()] = price
                                logger.debug(f"✅ Moralis ({key_label}): {token_address[:10]}... = ${price}")
                        elif response.status == 404:
                            logger.debug(f"⚠️ Moralis ({key_label}): Token {token_address[:10]}... not found")
                        elif response.status in [502, 503]:
                            logger.warning(f"⚠️ Moralis ({key_label}): Server error {response.status}")
                            return prices  # Return what we have so far
                        elif response.status == 429:
                            logger.warning(f"⚠️ Moralis ({key_label}): Rate limit exceeded")
                            return prices
                        elif response.status == 401:
                            logger.error(f"❌ Moralis ({key_label}): Authentication failed")
                            return prices
                
            except Exception as e:
                logger.debug(f"⚠️ Error fetching price for {token_address[:10]}...: {e}")
                continue
        
        logger.info(f"✅ Moralis ({key_label}): Fetched {len(prices)}/{len(token_addresses)} token prices")
        return prices
        
    except Exception as e:
        logger.error(f"❌ Moralis ({key_label}) bulk price API error: {e}")
        return prices


async def get_token_prices_coingecko(
    token_addresses: List[str],
    chain: str = 'ethereum'
) -> Dict[str, float]:
    """
    CoinGecko fallback - free tier, supports bulk
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
        
        url = "https://api.coingecko.com/api/v3/simple/token_price/" + platform
        
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
                    logger.error(f"❌ CoinGecko HTTP Error {response.status}: {error_text[:200]}")
                    return prices
                
                data = await response.json()
                
                for token_addr, price_data in data.items():
                    if isinstance(price_data, dict) and 'usd' in price_data:
                        price = float(price_data['usd'])
                        if price > 0:
                            prices[token_addr.lower()] = price
                
                logger.info(f"✅ CoinGecko: Fetched {len(prices)}/{len(token_addresses)} token prices")
                return prices
        
    except asyncio.TimeoutError:
        logger.error(f"⏱️ CoinGecko API timeout")
        return prices
    except Exception as e:
        logger.error(f"❌ CoinGecko API error: {e}")
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
        
        # Major tokens (approximations - updated 2025)
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 3500.0,  # WETH
        '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599': 65000.0, # WBTC
    }


async def execute_get_token_prices_bulk(
    token_addresses: List[str],
    chain: str = 'ethereum'
) -> Dict[str, float]:
    """
    Fetch token prices in bulk
    ✅ NEW: Hard limit to prevent API exhaustion
    """
    # ✅ CRITICAL: Hard limit to prevent API exhaustion
    MAX_TOKENS_PER_REQUEST = 100
    
    if len(token_addresses) > MAX_TOKENS_PER_REQUEST:
        logger.warning(f"⚠️ Too many tokens requested ({len(token_addresses)})")
        logger.warning(f"   Limiting to top {MAX_TOKENS_PER_REQUEST} tokens to preserve API limits")
        token_addresses = token_addresses[:MAX_TOKENS_PER_REQUEST]
    
    logger.info(f"💰 Fetching prices for {len(token_addresses)} tokens on {chain}")

    try:
        if not token_addresses:
            logger.warning("⚠️ No token addresses provided")
            return {}
        
        logger.info(f"💰 Fetching prices for {len(token_addresses)} tokens on {chain}")
        
        # Normalize addresses
        token_addresses = [addr.lower() for addr in token_addresses]
        
        # STEP 1: Start with hardcoded prices
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
        moralis_key_primary = os.getenv('MORALIS_API_KEY')
        moralis_key_fallback1 = os.getenv('MORALIS_API_KEY_FALLBACK')
        moralis_key_fallback2 = os.getenv('MORALIS_API_KEY_FALLBACK2')  # 🆕 THIRD KEY
        
        # ===== STRATEGY 2: Try PRIMARY Moralis Key =====
        if moralis_key_primary:
            logger.info(f"🚀 Trying Moralis API (Primary Key)...")
            moralis_prices = await get_token_prices_moralis(
                missing_addresses, 
                moralis_key_primary, 
                moralis_chain,
                key_label="Primary"
            )
            prices.update(moralis_prices)
            
            # Update missing list
            missing_addresses = [addr for addr in missing_addresses if addr not in prices]
            
            if not missing_addresses:
                logger.info(f"✅ All prices fetched with Primary Moralis")
                return {addr: prices[addr] for addr in token_addresses if addr in prices}
            
            logger.info(f"⚠️ {len(missing_addresses)} prices still missing, trying fallback key #1...")
        else:
            logger.warning(f"⚠️ No primary Moralis API key found")
        
        # ===== STRATEGY 3: Try FALLBACK Moralis Key #1 =====
        if moralis_key_fallback1 and missing_addresses:
            logger.info(f"🔄 Trying Moralis API (Fallback Key #1)...")
            moralis_prices = await get_token_prices_moralis(
                missing_addresses, 
                moralis_key_fallback1, 
                moralis_chain,
                key_label="Fallback-1"
            )
            prices.update(moralis_prices)
            
            # Update missing list
            missing_addresses = [addr for addr in missing_addresses if addr not in prices]
            
            if not missing_addresses:
                logger.info(f"✅ All prices fetched with Fallback-1 Moralis")
                return {addr: prices[addr] for addr in token_addresses if addr in prices}
            
            logger.info(f"⚠️ {len(missing_addresses)} prices still missing, trying fallback key #2...")
        else:
            logger.warning(f"⚠️ No fallback-1 Moralis API key found")
        
        # ===== STRATEGY 4: Try FALLBACK Moralis Key #2 🆕 =====
        if moralis_key_fallback2 and missing_addresses:
            logger.info(f"🔄 Trying Moralis API (Fallback Key #2)...")
            moralis_prices = await get_token_prices_moralis(
                missing_addresses, 
                moralis_key_fallback2, 
                moralis_chain,
                key_label="Fallback-2"
            )
            prices.update(moralis_prices)
            
            # Update missing list
            missing_addresses = [addr for addr in missing_addresses if addr not in prices]
            
            if not missing_addresses:
                logger.info(f"✅ All prices fetched with Fallback-2 Moralis")
                return {addr: prices[addr] for addr in token_addresses if addr in prices}
            
            logger.info(f"⚠️ {len(missing_addresses)} prices still missing, trying CoinGecko...")
        else:
            logger.warning(f"⚠️ No fallback-2 Moralis API key found")
        
        # ===== STRATEGY 5: Try CoinGecko (Final Fallback) =====
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
            for addr in missing_addresses[:5]:
                logger.debug(f"   - {addr}")
        
        # Return only requested tokens
        return {addr: prices[addr] for addr in token_addresses if addr in prices}
        
    except Exception as e:
        logger.error(f"❌ Critical error fetching token prices: {e}", exc_info=True)
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
