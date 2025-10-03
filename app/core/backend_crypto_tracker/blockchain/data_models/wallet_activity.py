# blockchain/data_models/wallet_activity.py
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal
from enum import Enum

class TransactionType(Enum):
    TRANSFER = "transfer"
    SWAP = "swap"
    APPROVE = "approve"
    CONTRACT_INTERACTION = "contract_interaction"
    NATIVE_TRANSFER = "native_transfer"

@dataclass
class Transaction:
    """Single blockchain transaction"""
    hash: str
    block_number: int
    timestamp: datetime
    from_address: str
    to_address: str
    value: Decimal
    gas_used: int
    gas_price: Decimal
    status: bool
    type: TransactionType
    method: Optional[str] = None

@dataclass
class TokenTransfer:
    """Token transfer event"""
    transaction_hash: str
    token_address: str
    token_symbol: str
    from_address: str
    to_address: str
    amount: Decimal
    timestamp: datetime
    usd_value: Optional[Decimal] = None

@dataclass
class WalletActivity:
    """Complete wallet activity data"""
    wallet_address: str
    chain: str
    transactions: List[Transaction]
    token_transfers: List[TokenTransfer]
    total_transactions: int
    first_transaction: Optional[datetime]
    last_transaction: Optional[datetime]
    total_gas_spent: Decimal
    unique_contracts_interacted: int
    metadata: Dict[str, Any]
