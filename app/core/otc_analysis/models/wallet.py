from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Wallet(Base):
    __tablename__ = 'wallets'
    
    # Primary identifier
    address = Column(String(42), primary_key=True)
    
    # Classification
    entity_type = Column(String(50), index=True)  # 'otc_desk', 'exchange', 'whale', 'unknown'
    entity_name = Column(String(255), nullable=True)  # e.g., "Wintermute", "Binance Hot Wallet"
    
    # Activity metrics
    first_seen = Column(DateTime, index=True)
    last_seen = Column(DateTime, index=True)
    total_transactions = Column(Integer, default=0)
    
    # Volume metrics
    total_volume_usd = Column(Float, default=0.0)
    avg_transaction_usd = Column(Float, default=0.0)
    median_transaction_usd = Column(Float, default=0.0)
    
    # Behavioral metrics
    transaction_frequency = Column(Float, default=0.0)  # Tx per day
    unique_counterparties = Column(Integer, default=0)
    counterparty_entropy = Column(Float, default=0.0)  # Shannon entropy
    
    # DeFi interaction flags
    has_defi_interactions = Column(Boolean, default=False)
    has_dex_swaps = Column(Boolean, default=False)
    has_contract_deployments = Column(Boolean, default=False)
    
    # Network position
    betweenness_centrality = Column(Float, default=0.0)
    degree_centrality = Column(Float, default=0.0)
    clustering_coefficient = Column(Float, default=0.0)
    
    # OTC classification
    otc_probability = Column(Float, default=0.0, index=True)
    is_known_otc_desk = Column(Boolean, default=False, index=True)
    cluster_id = Column(String(66), nullable=True, index=True)
    
    # Timing patterns
    active_hours = Column(JSON, default=list)  # [0-23] hours when active
    active_days = Column(JSON, default=list)  # [0-6] days when active
    weekend_activity_ratio = Column(Float, default=0.0)
    
    # Labels and tags
    labels = Column(JSON, default=list)  # From Nansen, Arkham, etc.
    tags = Column(JSON, default=list)  # Custom tags
    
    # Metadata
    chain_id = Column(Integer, default=1)
    confidence_score = Column(Float, default=0.0)
    last_analyzed = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<Wallet {self.address[:10]}... {self.entity_type} OTC:{self.otc_probability:.2f}>"
