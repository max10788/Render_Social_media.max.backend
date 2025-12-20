
from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class OTCActivity(Base):
    __tablename__ = 'otc_activities'
    
    # Primary identifier
    id = Column(Integer, primary_key=True, autoincrement=True)
    tx_hash = Column(String(66), ForeignKey('transactions.tx_hash'), index=True, unique=True)
    
    # Classification
    confidence_score = Column(Float, nullable=False, index=True)  # 0-100
    classification = Column(String(50), index=True)  # 'high_confidence', 'medium', 'low', 'suspected'
    
    # Score breakdown (matches scoring system from doc)
    transfer_size_score = Column(Float, default=0.0)
    wallet_profile_score = Column(Float, default=0.0)
    network_position_score = Column(Float, default=0.0)
    timing_score = Column(Float, default=0.0)
    known_entity_score = Column(Float, default=0.0)
    
    # Transaction details (denormalized for fast access)
    from_address = Column(String(42), index=True)
    to_address = Column(String(42), index=True)
    usd_value = Column(Float, index=True)
    timestamp = Column(DateTime, index=True)
    
    # OTC desk identification
    involves_known_desk = Column(Boolean, default=False)
    otc_desk_name = Column(String(255), nullable=True)
    desk_address = Column(String(42), nullable=True)
    
    # Pattern matching
    matched_patterns = Column(JSON, default=list)  # ['large_transfer', 'round_number', 'off_hours']
    
    # Anomaly indicators
    is_size_anomaly = Column(Boolean, default=False)
    z_score = Column(Float, nullable=True)
    percentile = Column(Float, nullable=True)  # Relative to 30-day window
    
    # Timing analysis
    hour_of_day = Column(Integer)  # 0-23
    day_of_week = Column(Integer)  # 0-6
    is_off_hours = Column(Boolean, default=False)
    is_weekend = Column(Boolean, default=False)
    
    # Network context
    from_cluster_id = Column(String(66), nullable=True)
    to_cluster_id = Column(String(66), nullable=True)
    hop_distance_to_known_desk = Column(Integer, nullable=True)
    
    # Round number detection
    is_round_number = Column(Boolean, default=False)
    round_number_level = Column(String(20), nullable=True)  # 'million', 'five_million', 'ten_million'
    
    # Flow tracing
    flow_path = Column(JSON, nullable=True)  # Array of addresses in flow
    flow_confidence = Column(Float, nullable=True)
    
    # Alert metadata
    alert_triggered = Column(Boolean, default=False)
    alert_type = Column(String(50), nullable=True)  # 'new_large_transfer', 'cluster_activity', 'desk_interaction'
    alert_sent_at = Column(DateTime, nullable=True)
    
    # Notes and manual classification
    notes = Column(Text, nullable=True)
    manually_verified = Column(Boolean, default=False)
    verified_by = Column(String(100), nullable=True)
    verified_at = Column(DateTime, nullable=True)
    
    # Metadata
    detected_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<OTCActivity {self.tx_hash[:10]}... score:{self.confidence_score:.1f} ${self.usd_value:.0f}>"
