# app/core/otc_analysis/models/alert.py
from sqlalchemy import Column, String, DateTime, Float, JSON, Boolean
from datetime import datetime
from uuid import uuid4

# ✅ WICHTIG: Importiere Base von database.py
from app.core.backend_crypto_tracker.config.database import Base


class Alert(Base):
    """
    Alerts for significant OTC activity.
    
    Alert types:
    - large_transfer: Large transfer detected
    - unusual_pattern: Unusual activity pattern detected
    - new_wallet: New wallet interaction
    - cluster_activity: Activity spike in wallet cluster
    - desk_interaction: Transaction with known OTC desk
    
    ✅ WICHTIG: Feldnamen müssen zu endpoints.py passen:
    - wallet_address (NICHT from_address als Hauptfeld)
    - message (PFLICHTFELD)
    - is_read (NICHT nur is_dismissed)
    - user_id (REQUIRED)
    """
    __tablename__ = 'otc_alerts'
    
    # Primary key - String UUID für Kompatibilität mit endpoints.py
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    
    # ✅ WICHTIG: user_id ist REQUIRED!
    user_id = Column(String(255), index=True, nullable=False)
    
    # Alert classification
    alert_type = Column(String(50), index=True, nullable=False)
    # 'large_transfer', 'unusual_pattern', 'new_wallet', 'cluster_activity', 'desk_interaction'
    
    severity = Column(String(20), index=True, nullable=False)
    # 'high', 'medium', 'low'
    
    # ✅ WICHTIG: "wallet_address" als Hauptfeld!
    wallet_address = Column(String(42), index=True)
    
    # ✅ WICHTIG: "message" ist PFLICHTFELD für endpoints.py!
    message = Column(String(500))
    
    # Additional transaction details (optional)
    tx_hash = Column(String(66), index=True, nullable=True)
    from_address = Column(String(42), index=True, nullable=True)
    to_address = Column(String(42), index=True, nullable=True)
    
    # Values
    usd_value = Column(Float, nullable=True)
    confidence_score = Column(Float, nullable=True)
    
    # ✅ Metadata (kein 'metadata' wegen SQLAlchemy Konflikt)
    alert_metadata = Column(JSON, default=dict)
    # Contains extra info depending on alert_type
    
    # User interaction - ✅ BEIDE Felder für Kompatibilität
    is_read = Column(Boolean, default=False, index=True)  # ✅ Für endpoints.py
    is_dismissed = Column(Boolean, default=False, index=True)  # Für erweiterte Funktionalität
    dismissed_at = Column(DateTime, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    def __repr__(self):
        return f"<Alert {self.alert_type} {self.wallet_address[:10] if self.wallet_address else 'N/A'}... severity={self.severity}>"
    
    def to_dict(self):
        """Convert to dict for API response"""
        return {
            'id': str(self.id),
            'alert_type': self.alert_type,
            'severity': self.severity,
            'wallet_address': self.wallet_address,
            'message': self.message,
            'tx_hash': self.tx_hash,
            'is_read': self.is_read,
            'is_dismissed': self.is_dismissed,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'dismissed_at': self.dismissed_at.isoformat() if self.dismissed_at else None,
            'data': {
                'usd_value': self.usd_value,
                'from_address': self.from_address,
                'to_address': self.to_address,
                'confidence_score': self.confidence_score,
                **self.alert_metadata
            }
        }


# ✅ ALIAS: Für Kompatibilität
OTCAlert = Alert
