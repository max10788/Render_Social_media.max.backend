from pydantic import BaseModel, Field, validator
from typing import List, Optional
import re
import logging

logger = logging.getLogger(__name__)

class ScanRangeRequest(BaseModel):
    """Request model for /scan/range endpoint."""
    
    from_block: int = Field(..., ge=0, description="Starting block number")
    to_block: int = Field(..., ge=0, description="Ending block number")
    tokens: Optional[List[str]] = Field(None, description="Token addresses to filter")
    min_usd_value: Optional[float] = Field(100000, ge=0, description="Minimum USD value")
    exclude_exchanges: bool = Field(True, description="Exclude known exchange addresses")
    
    @validator('to_block')
    def validate_block_range(cls, v, values):
        if 'from_block' in values and v < values['from_block']:
            raise ValueError('to_block must be >= from_block')
        if 'from_block' in values and (v - values['from_block']) > 10000:
            raise ValueError('Block range cannot exceed 10,000 blocks')
        return v
    
    @validator('tokens')
    def validate_token_addresses(cls, v):
        if v:
            for token in v:
                if not is_valid_ethereum_address(token):
                    raise ValueError(f'Invalid token address: {token}')
        return v


class WalletProfileRequest(BaseModel):
    """Request model for wallet profile endpoint."""
    
    address: str = Field(..., description="Ethereum wallet address")
    include_network_metrics: bool = Field(True)
    include_labels: bool = Field(True)
    
    @validator('address')
    def validate_address(cls, v):
        if not is_valid_ethereum_address(v):
            raise ValueError(f'Invalid Ethereum address: {v}')
        return v.lower()


class FlowTraceRequest(BaseModel):
    """Request model for flow tracing endpoint."""
    
    source_address: str = Field(..., description="Source wallet address")
    target_address: str = Field(..., description="Target wallet address")
    max_hops: int = Field(5, ge=1, le=10, description="Maximum hops to search")
    min_confidence: float = Field(0.0, ge=0.0, le=1.0, description="Minimum confidence threshold")
    
    @validator('source_address', 'target_address')
    def validate_addresses(cls, v):
        if not is_valid_ethereum_address(v):
            raise ValueError(f'Invalid Ethereum address: {v}')
        return v.lower()
    
    @validator('target_address')
    def validate_different_addresses(cls, v, values):
        if 'source_address' in values and v.lower() == values['source_address'].lower():
            raise ValueError('Source and target addresses must be different')
        return v


class TransactionAnalysisRequest(BaseModel):
    """Request model for transaction analysis."""
    
    tx_hash: str = Field(..., description="Transaction hash")
    
    @validator('tx_hash')
    def validate_tx_hash(cls, v):
        if not is_valid_transaction_hash(v):
            raise ValueError(f'Invalid transaction hash: {v}')
        return v.lower()


# Validation helper functions

def is_valid_ethereum_address(address: str) -> bool:
    """
    Validate Ethereum address format.
    
    Must be 42 characters (0x + 40 hex chars)
    """
    if not isinstance(address, str):
        return False
    
    # Check format
    pattern = re.compile(r'^0x[a-fA-F0-9]{40}$')
    return bool(pattern.match(address))


def is_valid_transaction_hash(tx_hash: str) -> bool:
    """
    Validate transaction hash format.
    
    Must be 66 characters (0x + 64 hex chars)
    """
    if not isinstance(tx_hash, str):
        return False
    
    pattern = re.compile(r'^0x[a-fA-F0-9]{64}$')
    return bool(pattern.match(tx_hash))


def validate_ethereum_address(address: str) -> str:
    """
    Validate and normalize Ethereum address.
    
    Raises ValueError if invalid.
    Returns lowercase address.
    """
    if not is_valid_ethereum_address(address):
        logger.error(f"Invalid Ethereum address: {address}")
        raise ValueError(f"Invalid Ethereum address format: {address}")
    
    return address.lower()


def validate_block_range(from_block: int, to_block: int, max_range: int = 10000):
    """
    Validate block range parameters.
    
    Raises ValueError if invalid.
    """
    if from_block < 0:
        raise ValueError("from_block must be >= 0")
    
    if to_block < from_block:
        raise ValueError("to_block must be >= from_block")
    
    if (to_block - from_block) > max_range:
        raise ValueError(f"Block range cannot exceed {max_range} blocks")
    
    logger.info(f"✅ Valid block range: {from_block} to {to_block} ({to_block - from_block + 1} blocks)")
    
    return True


def validate_usd_value(value: float, min_value: float = 0) -> bool:
    """Validate USD value parameter."""
    if value < min_value:
        raise ValueError(f"USD value must be >= {min_value}")
    return True
