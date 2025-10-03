# blockchain/blockchain_specific/ethereum/__init__.py
from .get_gas_price import get_gas_price
from .get_block_by_number import get_block_by_number
from .get_transaction_by_hash import get_transaction_by_hash

__all__ = [
    'get_gas_price',
    'get_block_by_number',
    'get_transaction_by_hash'
]
