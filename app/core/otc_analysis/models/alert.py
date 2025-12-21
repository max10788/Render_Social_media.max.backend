from sqlalchemy import Column, String, DateTime, Integer, Float, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Alert(Base):
    """
    Alerts for significant OTC activity.
    
    Alert types:
    - new_large_transfer: Large transfer detected
    - cluster_activity: Activity spike in wallet cluster
    - desk_interaction: Transaction with known OTC desk
    """
    __tablename__ = 'alerts'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Alert classification
    alert_type = Column(String(50), index=True, nullable=False)
    # 'new_large_transfer', 'cluster_activity', 'desk_interaction'
    
    severity = Column(String(20), index=True, nullable=False)
    # 'high', 'medium', 'low'
    
    # Core data
    tx_hash = Column(String(66), index=True, nullable=True)  # Transaction hash (if applicable)
    from_address = Column(String(42), index=True, nullable=True)
    to_address = Column(String(42), index=True, nullable=True)
    
    # Values
    usd_value = Column(Float, nullable=True)
    confidence_score = Column(Float, nullable=True)
    
    # Additional context (JSON)
    metadata = Column(JSON, default=dict)
    # Contains extra info depending on alert_type
    
    # User interaction
    user_id = Column(String(255), index=True, nullable=True)  # If user-specific
    is_dismissed = Column(Boolean, default=False, index=True)
    dismissed_at = Column(DateTime, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    def __repr__(self):
        return f"<Alert {self.alert_type} {self.severity} @ {self.created_at}>"
    
    def to_dict(self):
        """Convert to dict for API response."""
        return {
            'id': self.id,
            'type': self.alert_type,
            'severity': self.severity,
            'tx_hash': self.tx_hash,
            'timestamp': self.created_at.isoformat() if self.created_at else None,
            'data': {
                'usd_value': self.usd_value,
                'from_address': self.from_address,
                'to_address': self.to_address,
                'confidence_score': self.confidence_score,
                **self.metadata
            },
            'is_dismissed': self.is_dismissed,
            'dismissed_at': self.dismissed_at.isoformat() if self.dismissed_at else None
        }
