# app/core/otc_analysis/models/watchlist.py
from sqlalchemy import Column, String, DateTime, Boolean, Float
from datetime import datetime
from uuid import uuid4

# ✅ WICHTIG: Importiere Base von database.py
from app.core.backend_crypto_tracker.config.database import Base


class WatchlistItem(Base):
    """
    User's watchlist for monitoring specific wallets.
    
    Allows users to save and track wallets of interest.
    
    ✅ WICHTIG: Feldnamen müssen zu endpoints.py passen:
    - wallet_address (NICHT address)
    - notes (NICHT user_note)
    - created_at (NICHT added_at)
    """
    __tablename__ = 'otc_watchlist'
    
    # Primary key - String UUID für Kompatibilität mit endpoints.py
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    
    # User identification
    user_id = Column(String(255), index=True, nullable=False)
    
    # ✅ WICHTIG: "wallet_address" nicht "address"!
    wallet_address = Column(String(42), index=True, nullable=False)
    
    # ✅ WICHTIG: "notes" nicht "user_note"!
    notes = Column(String(500), nullable=True)
    
    # Alert settings
    alert_enabled = Column(Boolean, default=False)
    alert_threshold = Column(Float, nullable=True)  # USD threshold for alerts
    
    # Optional metadata (can be cached from Wallet model)
    label = Column(String(255), nullable=True)  # User's custom label
    entity_type = Column(String(50), nullable=True)
    entity_name = Column(String(255), nullable=True)
    
    # Timestamps - ✅ WICHTIG: "created_at" nicht "added_at"!
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_viewed = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<WatchlistItem user={self.user_id[:10]}... wallet={self.wallet_address[:10]}...>"
    
    def to_dict(self):
        """Convert to dict for API response"""
        return {
            'id': str(self.id),
            'wallet_address': self.wallet_address,
            'notes': self.notes,
            'alert_enabled': self.alert_enabled,
            'alert_threshold': self.alert_threshold,
            'label': self.label,
            'entity_type': self.entity_type,
            'entity_name': self.entity_name,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_viewed': self.last_viewed.isoformat() if self.last_viewed else None
        }


# ✅ ALIAS: Für Kompatibilität
OTCWatchlist = WatchlistItem
