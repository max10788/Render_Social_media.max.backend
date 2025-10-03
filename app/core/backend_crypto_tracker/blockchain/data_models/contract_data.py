# blockchain/data_models/contract_data.py
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal

@dataclass
class ABIData:
    """Contract ABI information"""
    abi: List[Dict[str, Any]]
    implementation: Optional[str] = None
    is_proxy: bool = False
    verified: bool = False

@dataclass
class ContractMetrics:
    """Contract performance metrics"""
    transaction_count: int
    unique_users: int
    total_value_locked: Optional[Decimal] = None
    daily_active_users: Optional[int] = None
    gas_used_total: Optional[int] = None

@dataclass
class ContractInfo:
    """Complete contract information"""
    address: str
    chain: str
    name: Optional[str]
    symbol: Optional[str]
    decimals: Optional[int]
    creation_date: Optional[datetime]
    creator_address: Optional[str]
    abi: Optional[ABIData]
    metrics: ContractMetrics
    metadata: Dict[str, Any]
