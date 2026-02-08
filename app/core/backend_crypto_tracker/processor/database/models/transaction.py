# processor/database/models/transaction.py
# processor/database/models/transaction.py
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Index, Text, JSON, Float, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.backend_crypto_tracker.processor.database.models import Base
import decimal

class Transaction(Base):
    __tablename__ = "transactions"
    
    # Primärschlüssel und Identifikation
    id = Column(Integer, primary_key=True, index=True)
    tx_hash = Column(String(255), unique=True, index=True, nullable=False)
    chain = Column(String(20), nullable=False, index=True)
    block_number = Column(Integer)
    
    # Transaktionsdetails
    from_address = Column(String(255), nullable=False)
    to_address = Column(String(255))
    value = Column(Numeric(36, 18))  # Transaktionswert mit hoher Präzision
    gas_used = Column(Integer)
    gas_price = Column(Numeric(36, 18))
    fee = Column(Numeric(36, 18))
    
    # Token-spezifische Details
    token_address = Column(String(255))
    token_amount = Column(Numeric(36, 18))
    
    # Metadaten
    timestamp = Column(DateTime, nullable=False)
    status = Column(String(20), default="success")  # success, failed, pending
    method = Column(String(100))  # Transaktionsmethode (z.B. transfer, swap)
    
    # Zusätzliche Daten als JSON - umbenannt von 'metadata' zu 'transaction_metadata'
    transaction_metadata = Column(JSON)
    
    # Beziehungen
    # from_address_obj = relationship("Address", foreign_keys=[from_address])
    # to_address_obj = relationship("Address", foreign_keys=[to_address])
    
    # Indexe für Performance-Optimierung
    __table_args__ = (
        Index('idx_transaction_hash', 'tx_hash'),
        Index('idx_transaction_addresses', 'from_address', 'to_address'),
        Index('idx_transaction_token', 'token_address'),
        Index('idx_transaction_timestamp', 'timestamp'),
        Index('idx_transaction_chain_block', 'chain', 'block_number'),
    )
    
    def __repr__(self):
        return f"<Transaction(hash='{self.tx_hash[:10]}...', chain='{self.chain}', status='{self.status}')>"
    
    def to_dict(self):
        """Konvertiert das Transaction-Objekt in ein Dictionary"""
        return {
            'id': self.id,
            'tx_hash': self.tx_hash,
            'chain': self.chain,
            'block_number': self.block_number,
            'from_address': self.from_address,
            'to_address': self.to_address,
            'value': float(self.value) if self.value else None,
            'gas_used': self.gas_used,
            'gas_price': float(self.gas_price) if self.gas_price else None,
            'fee': float(self.fee) if self.fee else None,
            'token_address': self.token_address,
            'token_amount': float(self.token_amount) if self.token_amount else None,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'status': self.status,
            'method': self.method,
            'metadata': self.transaction_metadata,
        }
