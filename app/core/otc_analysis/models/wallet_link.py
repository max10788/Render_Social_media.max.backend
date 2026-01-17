"""
Wallet Link Model
=================

Speichert persistente Verbindungen zwischen Wallets (OTC ↔ OTC, Wallet ↔ OTC).

✅ FEATURES:
- Aggregierte Transaktionsdaten
- Directional (from → to)
- Auto-calculated scores
- Time window tracking
"""

from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON, Index, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class WalletLink(Base):
    __tablename__ = 'wallet_links'
    
    # ================================================================
    # PRIMARY IDENTIFIERS
    # ================================================================
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Directional link: from_address → to_address
    from_address = Column(String(42), nullable=False, index=True)
    to_address = Column(String(42), nullable=False, index=True)
    
    # ================================================================
    # WALLET METADATA
    # ================================================================
    
    from_wallet_type = Column(String(50), nullable=True)  # 'otc_desk', 'high_volume_wallet', etc.
    to_wallet_type = Column(String(50), nullable=True)
    
    from_wallet_label = Column(String(255), nullable=True)
    to_wallet_label = Column(String(255), nullable=True)
    
    # ================================================================
    # AGGREGATED TRANSACTION DATA
    # ================================================================
    
    transaction_count = Column(Integer, default=0, index=True)
    total_volume_usd = Column(Float, default=0.0, index=True)
    avg_transaction_usd = Column(Float, default=0.0)
    
    # Min/Max values for size distribution
    min_transaction_usd = Column(Float, nullable=True)
    max_transaction_usd = Column(Float, nullable=True)
    
    # ================================================================
    # TIME WINDOW
    # ================================================================
    
    first_transaction = Column(DateTime, nullable=True, index=True)
    last_transaction = Column(DateTime, nullable=True, index=True)
    
    # Analysis window (welcher Zeitraum wurde analysiert?)
    analysis_start = Column(DateTime, nullable=False)
    analysis_end = Column(DateTime, nullable=False)
    
    # ================================================================
    # LINK CLASSIFICATION & SCORING
    # ================================================================
    
    link_strength = Column(Float, default=0.0, index=True)  # 0-100 score
    is_suspected_otc = Column(Boolean, default=False, index=True)
    otc_confidence = Column(Float, default=0.0)  # 0-100
    
    # Score breakdown
    volume_score = Column(Float, default=0.0)      # Based on total volume
    frequency_score = Column(Float, default=0.0)   # Based on tx count
    recency_score = Column(Float, default=0.0)     # Based on last transaction
    consistency_score = Column(Float, default=0.0) # Based on regularity
    
    # ================================================================
    # PATTERN DETECTION
    # ================================================================
    
    detected_patterns = Column(JSON, default=list)
    # Examples: ['large_transfers', 'regular_intervals', 'round_numbers', 'off_hours']
    
    # Flow characteristics
    flow_type = Column(String(50), nullable=True)  # 'inbound', 'outbound', 'bidirectional'
    is_bidirectional = Column(Boolean, default=False)
    
    # ================================================================
    # DATA SOURCE & QUALITY
    # ================================================================
    
    data_source = Column(String(50), default='transactions')  # 'transactions', 'discovery', 'mixed'
    data_quality = Column(String(20), default='high')  # 'high', 'medium', 'low'
    
    # Sample transaction hashes for verification
    sample_tx_hashes = Column(JSON, default=list)  # Store up to 10 sample hashes
    
    # ================================================================
    # NETWORK ANALYSIS
    # ================================================================
    
    # Clustering
    from_cluster_id = Column(String(66), nullable=True, index=True)
    to_cluster_id = Column(String(66), nullable=True, index=True)
    
    # Importance metrics
    betweenness_score = Column(Float, nullable=True)  # Graph centrality
    is_critical_path = Column(Boolean, default=False)  # Part of major flow path
    
    # ================================================================
    # ENRICHMENT DATA
    # ================================================================
    
    # Token breakdown (welche Tokens wurden gehandelt?)
    token_distribution = Column(JSON, nullable=True)
    # Format: {"ETH": 50000, "USDT": 100000, "USDC": 75000}
    
    primary_token = Column(String(42), nullable=True)  # Most traded token
    token_diversity = Column(Integer, default=0)  # Number of different tokens
    
    # ================================================================
    # MANUAL REVIEW
    # ================================================================
    
    manually_verified = Column(Boolean, default=False)
    verified_by = Column(String(100), nullable=True)
    verified_at = Column(DateTime, nullable=True)
    notes = Column(JSON, nullable=True)
    
    # ================================================================
    # FLAGS & STATUS
    # ================================================================
    
    is_active = Column(Boolean, default=True, index=True)
    needs_refresh = Column(Boolean, default=False)  # Re-calculate if True
    
    # Alert generation
    alert_triggered = Column(Boolean, default=False)
    alert_type = Column(String(50), nullable=True)
    
    # ================================================================
    # METADATA
    # ================================================================
    
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_calculated = Column(DateTime, nullable=True)
    
    # ================================================================
    # INDEXES FOR PERFORMANCE
    # ================================================================
    
    __table_args__ = (
        # Unique constraint: one link per direction per time window
        Index('idx_unique_link', 'from_address', 'to_address', 'analysis_start', unique=True),
        
        # Query optimization
        Index('idx_volume_strength', 'total_volume_usd', 'link_strength'),
        Index('idx_otc_links', 'is_suspected_otc', 'otc_confidence'),
        Index('idx_active_links', 'is_active', 'last_transaction'),
        Index('idx_wallet_types', 'from_wallet_type', 'to_wallet_type'),
        
        # Network analysis
        Index('idx_clusters', 'from_cluster_id', 'to_cluster_id'),
        Index('idx_critical_paths', 'is_critical_path', 'betweenness_score'),
    )
    
    def __repr__(self):
        return (
            f"<WalletLink {self.from_address[:10]}...→{self.to_address[:10]}... "
            f"${self.total_volume_usd:,.0f} ({self.transaction_count} TXs) "
            f"strength:{self.link_strength:.1f}>"
        )
    
    def to_dict(self):
        """Convert to dictionary for API responses."""
        return {
            "id": self.id,
            "from_address": self.from_address,
            "to_address": self.to_address,
            "from_wallet_type": self.from_wallet_type,
            "to_wallet_type": self.to_wallet_type,
            "from_wallet_label": self.from_wallet_label,
            "to_wallet_label": self.to_wallet_label,
            "transaction_count": self.transaction_count,
            "total_volume_usd": float(self.total_volume_usd or 0),
            "avg_transaction_usd": float(self.avg_transaction_usd or 0),
            "min_transaction_usd": float(self.min_transaction_usd or 0) if self.min_transaction_usd else None,
            "max_transaction_usd": float(self.max_transaction_usd or 0) if self.max_transaction_usd else None,
            "first_transaction": self.first_transaction.isoformat() if self.first_transaction else None,
            "last_transaction": self.last_transaction.isoformat() if self.last_transaction else None,
            "link_strength": float(self.link_strength or 0),
            "is_suspected_otc": self.is_suspected_otc,
            "otc_confidence": float(self.otc_confidence or 0),
            "detected_patterns": self.detected_patterns or [],
            "flow_type": self.flow_type,
            "is_bidirectional": self.is_bidirectional,
            "data_source": self.data_source,
            "sample_tx_hashes": self.sample_tx_hashes or [],
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
