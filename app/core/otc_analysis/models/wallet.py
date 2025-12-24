# app/core/otc_analysis/models/wallet.py
from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON, Text
from datetime import datetime

# ✅ WICHTIG: Importiere Base von database.py
from app.core.backend_crypto_tracker.config.database import Base


class OTCWallet(Base):
    """
    OTC Wallet Model
    
    Stores wallet data with OTC classification and behavioral metrics.
    
    ✅ WICHTIG: Klasse heißt "OTCWallet" (nicht "Wallet")
    Das passt zu init_otc_db.py!
    """
    __tablename__ = 'otc_wallets'
    
    # Primary identifier
    address = Column(String(42), primary_key=True)
    
    # Classification
    entity_type = Column(String(50), index=True)  # 'otc_desk', 'market_maker', 'cex', 'prop_trading', 'whale', 'unknown'
    entity_name = Column(String(255), nullable=True)  # e.g., "Wintermute", "Binance"
    label = Column(String(255), nullable=True)  # Display name / custom label
    
    # Activity metrics
    first_seen = Column(DateTime, index=True)
    last_active = Column(DateTime, index=True)  # ✅ WICHTIG: "last_active" nicht "last_seen"
    total_transactions = Column(Integer, default=0)
    transaction_count = Column(Integer, default=0)  # ✅ Alias für Kompatibilität
    
    # Volume metrics  
    total_volume_usd = Column(Float, default=0.0)
    total_volume = Column(Float, default=0.0)  # ✅ WICHTIG: endpoints.py nutzt dieses Feld!
    avg_transaction_usd = Column(Float, default=0.0)
    avg_transaction_size = Column(Float, default=0.0)  # ✅ Alias für Kompatibilität
    median_transaction_usd = Column(Float, default=0.0)
    
    # Behavioral metrics
    transaction_frequency = Column(Float, default=0.0)  # Transactions per day
    unique_counterparties = Column(Integer, default=0)
    counterparty_entropy = Column(Float, default=0.0)  # Shannon entropy
    
    # DeFi interaction flags
    has_defi_interactions = Column(Boolean, default=False)
    has_dex_swaps = Column(Boolean, default=False)
    has_contract_deployments = Column(Boolean, default=False)
    
    # Network position metrics
    betweenness_centrality = Column(Float, default=0.0)
    degree_centrality = Column(Float, default=0.0)
    clustering_coefficient = Column(Float, default=0.0)
    
    # OTC classification
    otc_probability = Column(Float, default=0.0, index=True)
    is_known_otc_desk = Column(Boolean, default=False, index=True)
    is_active = Column(Boolean, default=True)
    cluster_id = Column(String(66), nullable=True, index=True)
    
    # Timing patterns (stored as JSON arrays)
    active_hours = Column(JSON, default=list)  # [0-23] hours when active
    active_days = Column(JSON, default=list)  # [0-6] days when active (Mon=0)
    weekend_activity_ratio = Column(Float, default=0.0)
    
    # Labels and tags (JSON arrays)
    labels = Column(JSON, default=list)  # Labels from Nansen, Arkham, etc.
    tags = Column(JSON, default=list)  # Custom tags like ["market_maker", "verified"]
    
    # Risk assessment
    risk_score = Column(Float, default=0.0)
    
    # Optional notes
    notes = Column(Text, nullable=True)  # ✅ Für init_otc_db.py
    
    # Metadata
    chain_id = Column(Integer, default=1)  # 1 = Ethereum mainnet
    confidence_score = Column(Float, default=0.0)
    last_analyzed = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<OTCWallet {self.address[:10]}... {self.entity_type} OTC:{self.otc_probability:.2f}>"
    
    def to_dict(self):
        """Convert to dict for API response"""
        return {
            'address': self.address,
            'label': self.label,
            'entity_type': self.entity_type,
            'entity_name': self.entity_name,
            'total_volume': self.total_volume,
            'transaction_count': self.transaction_count,
            'avg_transaction_size': self.avg_transaction_size,
            'confidence_score': self.confidence_score,
            'otc_probability': self.otc_probability,
            'is_active': self.is_active,
            'first_seen': self.first_seen.isoformat() if self.first_seen else None,
            'last_active': self.last_active.isoformat() if self.last_active else None,
            'tags': self.tags or []
        }


# ✅ ALIAS: Für Kompatibilität mit Code, der "Wallet" importiert
Wallet = OTCWallet
