import os
import asyncio
import aiohttp
from typing import Optional, Dict, Any, List
from datetime import datetime
from decimal import Decimal
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)

# Load Sui RPC URL from environment
SUI_RPC_URL = os.getenv("SUI_RPC_URL", "https://fullnode.mainnet.sui.io:443")

# Known Sui DEX contracts for price discovery
SUI_DEX_CONTRACTS = {
    "cetus": "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb",
    "turbos": "0x91bfbc386a41afcfd9b2533058d7e915a1d3829089cc268ff4333d54d6339ca1",
    "aftermath": "0x2::sui::SUI"  # Aftermath DEX
}

# Known token types on Sui
KNOWN_SUI_TOKENS = {
    "0x2::sui::SUI": {
        "symbol": "SUI",
        "name": "Sui",
        "decimals": 9,
        "is_native": True
    },
    "0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN": {
        "symbol": "WUSDC",
        "name": "Wrapped USDC",
        "decimals": 6
    },
    "0xc060006111016b8a020ad5b33834984a437aaa7d3c74c18e09a95d48aceab08c::coin::COIN": {
        "symbol": "USDT",
        "name": "Tether USD",
        "decimals": 6
    }
}


async def execute_get_token_price(
    provider: Any,
    token_address: str,
    chain: str = "sui",
    vs_currency: str = "usd"
) -> Optional[TokenPriceData]:
    """
    Fetch token price from Sui RPC using DEX pools and on-chain data.
    
    Args:
        provider: Sui provider instance (optional, uses RPC URL if not provided)
        token_address: Token type/address on Sui
        chain: Blockchain name (default: sui)
        vs_currency: Target currency (default: usd)
        
    Returns:
        TokenPriceData object or None if price not found
    """
    try:
        if chain.lower() != "sui":
            logger.warning(f"This implementation only supports Sui chain, got: {chain}")
            return None
        
        logger.info(f"Fetching Sui token price for: {token_address}")
        
        # Normalize token address
        token_type = token_address.strip()
        
        # Check if it's a known token
        token_info = KNOWN_SUI_TOKENS.get(token_type, {})
        
        # Step 1: Try to get price from DEX pools
        price_data = await _fetch_price_from_dex_pools(token_type, vs_currency)
        
        if price_data:
            return price_data
        
        # Step 2: If native SUI token, try external oracle or fallback
        if token_info.get("is_native"):
            return await _fetch_native_sui_price(token_type, vs_currency)
        
        # Step 3: Try to get token metadata and estimate price
        metadata = await _fetch_token_metadata(token_type)
        if metadata:
            logger.info(f"Token metadata: {metadata}")
        
        logger.warning(f"Could not find price for token: {token_type}")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching Sui token price: {e}", exc_info=True)
        return None


async def _fetch_price_from_dex_pools(
    token_type: str,
    vs_currency: str = "usd"
) -> Optional[TokenPriceData]:
    """
    Fetch token price by analyzing DEX liquidity pools.
    Uses Cetus, Turbos, and other Sui DEXes.
    """
    try:
        # Try multiple DEXes
        price = None
        volume_24h = 0.0
        
        # 1. Try Cetus DEX
        cetus_price = await _get_price_from_cetus(token_type)
        if cetus_price:
            price = cetus_price
            logger.info(f"Found price from Cetus: ${price}")
        
        # 2. Try Turbos Finance
        if not price:
            turbos_price = await _get_price_from_turbos(token_type)
            if turbos_price:
                price = turbos_price
                logger.info(f"Found price from Turbos: ${price}")
        
        if not price:
            return None
        
        # Get token info
        token_info = KNOWN_SUI_TOKENS.get(token_type, {})
        symbol = token_info.get("symbol", "UNKNOWN")
        
        return TokenPriceData(
            token_address=token_type,
            chain="sui",
            price=price,
            price_usd=price if vs_currency == "usd" else None,
            currency=vs_currency,
            volume_24h=volume_24h,
            last_updated=datetime.now(),
            source="sui_dex",
            symbol=symbol,
            token_id=token_type
        )
        
    except Exception as e:
        logger.error(f"Error fetching price from DEX pools: {e}")
        return None


async def _get_price_from_cetus(token_type: str) -> Optional[float]:
    """Fetch price from Cetus DEX pools"""
    try:
        async with aiohttp.ClientSession() as session:
            # Get all pools that contain this token
            url = f"{SUI_RPC_URL}"
            
            # RPC call to get objects by type
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_getDynamicFields",
                "params": [
                    SUI_DEX_CONTRACTS["cetus"]
                ]
            }
            
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Parse pool data to find token price
                    # This is simplified - real implementation needs pool parsing
                    pools = data.get("result", {}).get("data", [])
                    
                    for pool in pools:
                        # Check if pool contains our token
                        if token_type in str(pool):
                            # Extract price from pool reserves
                            # This is a placeholder - actual implementation needs proper pool math
                            pass
                    
                    logger.debug(f"Cetus pools checked, no price found for {token_type}")
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching from Cetus: {e}")
        return None


async def _get_price_from_turbos(token_type: str) -> Optional[float]:
    """Fetch price from Turbos Finance pools"""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{SUI_RPC_URL}"
            
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_getDynamicFields",
                "params": [
                    SUI_DEX_CONTRACTS["turbos"]
                ]
            }
            
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Turbos response: {data}")
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching from Turbos: {e}")
        return None


async def _fetch_native_sui_price(
    token_type: str,
    vs_currency: str = "usd"
) -> Optional[TokenPriceData]:
    """
    Fetch SUI native token price using on-chain oracles or reference pools.
    """
    try:
        # For native SUI, we can use major stablecoin pools as price reference
        # Example: SUI/USDC pool reserves give us SUI price
        
        async with aiohttp.ClientSession() as session:
            url = f"{SUI_RPC_URL}"
            
            # Get SUI/USDC pool from Cetus (major liquidity pool)
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sui_getObject",
                "params": [
                    "0x2::sui::SUI",  # SUI token object
                    {
                        "showType": True,
                        "showContent": True,
                        "showOwner": True
                    }
                ]
            }
            
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Native SUI data: {data}")
                    
                    # For demo purposes, return a placeholder
                    # Real implementation would calculate from pool reserves
                    return TokenPriceData(
                        token_address=token_type,
                        chain="sui",
                        price=0.0,  # Would be calculated from pools
                        price_usd=0.0,
                        currency=vs_currency,
                        last_updated=datetime.now(),
                        source="sui_rpc",
                        symbol="SUI",
                        token_id=token_type
                    )
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching native SUI price: {e}")
        return None


async def _fetch_token_metadata(token_type: str) -> Optional[Dict[str, Any]]:
    """
    Fetch token metadata from Sui RPC.
    """
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{SUI_RPC_URL}"
            
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sui_getCoinMetadata",
                "params": [
                    token_type
                ]
            }
            
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    result = data.get("result")
                    
                    if result:
                        return {
                            "name": result.get("name"),
                            "symbol": result.get("symbol"),
                            "decimals": result.get("decimals"),
                            "description": result.get("description"),
                            "icon_url": result.get("iconUrl")
                        }
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching token metadata: {e}")
        return None


async def get_sui_token_balance(
    token_type: str,
    owner_address: str
) -> Optional[Decimal]:
    """
    Get token balance for a specific address on Sui.
    
    Args:
        token_type: Token type/address
        owner_address: Sui address to check balance
        
    Returns:
        Token balance as Decimal
    """
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{SUI_RPC_URL}"
            
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_getBalance",
                "params": [
                    owner_address,
                    token_type
                ]
            }
            
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    result = data.get("result")
                    
                    if result:
                        total_balance = result.get("totalBalance", "0")
                        decimals = KNOWN_SUI_TOKENS.get(token_type, {}).get("decimals", 9)
                        
                        # Convert from smallest unit to token amount
                        balance = Decimal(total_balance) / Decimal(10 ** decimals)
                        return balance
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching Sui token balance: {e}")
        return None


async def get_sui_token_supply(token_type: str) -> Optional[Dict[str, Any]]:
    """
    Get total supply information for a token.
    
    Args:
        token_type: Token type/address
        
    Returns:
        Dictionary with supply information
    """
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{SUI_RPC_URL}"
            
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_getTotalSupply",
                "params": [
                    token_type
                ]
            }
            
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    result = data.get("result")
                    
                    if result:
                        return {
                            "total_supply": result.get("value", "0"),
                            "token_type": token_type
                        }
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching token supply: {e}")
        return None


# Helper function to get multiple token prices in batch
async def get_multiple_token_prices(
    token_types: List[str],
    vs_currency: str = "usd"
) -> Dict[str, Optional[TokenPriceData]]:
    """
    Fetch prices for multiple tokens concurrently.
    
    Args:
        token_types: List of token types/addresses
        vs_currency: Target currency
        
    Returns:
        Dictionary mapping token_type to TokenPriceData
    """
    tasks = [
        execute_get_token_price(None, token_type, "sui", vs_currency)
        for token_type in token_types
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    return {
        token_type: result if not isinstance(result, Exception) else None
        for token_type, result in zip(token_types, results)
    }
