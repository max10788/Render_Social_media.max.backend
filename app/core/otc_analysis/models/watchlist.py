from sqlalchemy import Column, String, DateTime, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class WatchlistItem(Base):
    """
    User's watchlist for monitoring specific wallets.
    
    Allows users to save and track wallets of interest.
    """
    __tablename__ = 'watchlist'
    
    # Composite primary key: user_id + address
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(255), index=True, nullable=False)  # Auth user ID
    address = Column(String(42), index=True, nullable=False)    # Wallet address
    
    # Metadata
    label = Column(String(255), nullable=True)  # User's custom label
    user_note = Column(Text, nullable=True)     # User's notes
    
    # Derived data (cached from Wallet model)
    entity_type = Column(String(50), nullable=True)
    entity_name = Column(String(255), nullable=True)
    
    # Timestamps
    added_at = Column(DateTime, default=datetime.utcnow, index=True)
    last_viewed = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<WatchlistItem {self.address[:10]}... by user {self.user_id[:10]}...>"
    
    def to_dict(self):
        """Convert to dict for API response."""
        return {
            'id': self.id,
            'address': self.address,
            'label': self.label,
            'user_note': self.user_note,
            'entity_type': self.entity_type,
            'entity_name': self.entity_name,
            'added_at': self.added_at.isoformat() if self.added_at else None,
            'last_viewed': self.last_viewed.isoformat() if self.last_viewed else None
        }
