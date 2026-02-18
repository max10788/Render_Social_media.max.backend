from sqlalchemy import Column, String, BigInteger, Float, DateTime, Integer, Boolean, Index, Numeric
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'transactions'
    
    # Primary identifiers
    tx_hash = Column(String(66), primary_key=True)
    block_number = Column(BigInteger, nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # Transaction details
    from_address = Column(String(42), nullable=False, index=True)
    to_address = Column(String(42), nullable=False, index=True)
    token_address = Column(String(42), nullable=True, index=True)  # None for native ETH
    
    # Value information
    value = Column(Numeric(36, 18), nullable=True)  # ETH-denominated value matching DB schema
    value_decimal = Column(Float, nullable=False)  # Human-readable amount
    usd_value = Column(Float, nullable=True, index=True)
    
    # Gas information
    gas_used = Column(BigInteger)
    gas_price = Column(Numeric(36, 18))
    
    # Classification
    is_contract_interaction = Column(Boolean, default=False)
    method_id = Column(String(10), nullable=True)  # First 4 bytes of input data
    
    # OTC-specific fields
    otc_score = Column(Float, default=0.0, index=True)
    is_suspected_otc = Column(Boolean, default=False, index=True)
    
    # Metadata
    chain = Column(String(20), nullable=False, default='ethereum')  # Required by DB schema
    chain_id = Column(Integer, nullable=False, default=1)  # 1=Ethereum, 56=BSC, etc.
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_from_timestamp', 'from_address', 'timestamp'),
        Index('idx_to_timestamp', 'to_address', 'timestamp'),
        Index('idx_usd_value_desc', 'usd_value'),
        Index('idx_otc_suspected', 'is_suspected_otc', 'otc_score'),
    )
    
    def __repr__(self):
        return f"<Transaction {self.tx_hash[:10]}... ${self.usd_value:.2f}>"
