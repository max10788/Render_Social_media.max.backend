"""
Validation utilities for Price Movers API
"""

from typing import Optional
from fastapi import HTTPException, status


def validate_dex_params(dex_exchange: str, symbol: str, timeframe) -> None:
    """
    Validate DEX request parameters
    
    Args:
        dex_exchange: DEX exchange name
        symbol: Trading pair symbol
        timeframe: Candle timeframe
        
    Raises:
        HTTPException: If validation fails
    """
    # Validate DEX exchange
    valid_dexs = ['jupiter', 'raydium', 'orca', 'uniswap', 'pancakeswap', 'sushiswap']
    if dex_exchange.lower() not in valid_dexs:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid DEX exchange. Supported: {', '.join(valid_dexs)}"
        )
    
    # Validate symbol format
    if '/' not in symbol:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid symbol format. Expected format: BASE/QUOTE (e.g., SOL/USDC)"
        )
    
    parts = symbol.split('/')
    if len(parts) != 2 or not all(parts):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid symbol format. Expected format: BASE/QUOTE (e.g., SOL/USDC)"
        )
    
    # Validate timeframe
    valid_timeframes = ['1m', '5m', '15m', '30m', '1h', '4h', '1d']
    timeframe_str = str(timeframe.value) if hasattr(timeframe, 'value') else str(timeframe)
    
    if timeframe_str not in valid_timeframes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid timeframe. Supported: {', '.join(valid_timeframes)}"
        )


def validate_cex_params(exchange: str, symbol: str, timeframe) -> None:
    """
    Validate CEX request parameters
    
    Args:
        exchange: CEX exchange name
        symbol: Trading pair symbol
        timeframe: Candle timeframe
        
    Raises:
        HTTPException: If validation fails
    """
    # Validate CEX exchange
    valid_exchanges = ['binance', 'bitget', 'kraken', 'coinbase', 'okx']
    if exchange.lower() not in valid_exchanges:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid CEX exchange. Supported: {', '.join(valid_exchanges)}"
        )
    
    # Validate symbol format
    if '/' not in symbol:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid symbol format. Expected format: BASE/QUOTE (e.g., BTC/USDT)"
        )
    
    parts = symbol.split('/')
    if len(parts) != 2 or not all(parts):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid symbol format. Expected format: BASE/QUOTE (e.g., BTC/USDT)"
        )
    
    # Validate timeframe
    valid_timeframes = ['1m', '5m', '15m', '30m', '1h', '4h', '1d']
    timeframe_str = str(timeframe.value) if hasattr(timeframe, 'value') else str(timeframe)
    
    if timeframe_str not in valid_timeframes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid timeframe. Supported: {', '.join(valid_timeframes)}"
        )


def validate_wallet_address(address: str, blockchain: Optional[str] = None) -> bool:
    """
    Validate blockchain wallet address format
    
    Args:
        address: Wallet address
        blockchain: Optional blockchain type
        
    Returns:
        bool: True if valid format
    """
    if not address or len(address) < 26:
        return False
    
    # Solana addresses (Base58, 32-44 chars)
    if blockchain == 'solana' or (not blockchain and len(address) >= 32 and len(address) <= 44):
        # Base58 check (no 0, O, I, l)
        import re
        if re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', address):
            return True
    
    # Ethereum addresses (0x prefix, 42 chars)
    if blockchain == 'ethereum' or (not blockchain and address.startswith('0x')):
        import re
        if re.match(r'^0x[a-fA-F0-9]{40}$', address):
            return True
    
    # Bitcoin addresses
    if blockchain == 'bitcoin' or (not blockchain and address[0] in ['1', '3', 'bc1']):
        # Simplified check
        if len(address) >= 26 and len(address) <= 62:
            return True
    
    return False


def validate_time_range(start_time, end_time, max_hours: int = 720) -> None:
    """
    Validate time range parameters
    
    Args:
        start_time: Start datetime
        end_time: End datetime
        max_hours: Maximum allowed time range in hours
        
    Raises:
        HTTPException: If validation fails
    """
    if start_time >= end_time:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_time must be before end_time"
        )
    
    time_diff = (end_time - start_time).total_seconds() / 3600
    if time_diff > max_hours:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Time range too large. Maximum: {max_hours} hours, requested: {time_diff:.1f} hours"
        )
    
    if time_diff < 0.016:  # Less than 1 minute
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Time range too small. Minimum: 1 minute"
        )
