# blockchain/blockchain_specific/__init__.py
"""
Blockchain-specific modules for interacting with different chains.
"""
from .bitcoin import get_block_info
from .ethereum import (
    get_gas_price,
    get_block_by_number,
    get_transaction_by_hash
)
from .solana import get_account_info
from .sui import get_transaction

__all__ = [
    'get_block_info',
    'get_gas_price',
    'get_block_by_number',
    'get_transaction_by_hash',
    'get_account_info',
    'get_transaction'
]
