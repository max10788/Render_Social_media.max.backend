# blockchain/utils/format_utils.py
from decimal import Decimal
from typing import Union, Optional

def format_address(address: str, short: bool = False) -> str:
    """Format blockchain address"""
    if not address:
        return ""
    if short and len(address) > 10:
        return f"{address[:6]}...{address[-4:]}"
    return address.lower()

def format_number(number: Union[int, float, Decimal], decimals: int = 2) -> str:
    """Format number with appropriate suffix"""
    num = float(number)
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.{decimals}f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.{decimals}f}M"
    elif num >= 1_000:
        return f"{num/1_000:.{decimals}f}K"
    return f"{num:.{decimals}f}"

def format_percentage(value: float, decimals: int = 2) -> str:
    """Format percentage value"""
    return f"{value:.{decimals}f}%"
