# processor/database/models/wallet.py
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Index, Text, JSON, Float, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.backend_crypto_tracker.processor.database.models import Base
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


class WalletTypeEnum(Enum):
    """
    Enum für Wallet-Typen basierend auf dem 3-Stage-Klassifizierungssystem
    """
    # Neue 5 Haupttypen aus dem Klassifizierungssystem
    DUST_SWEEPER = "DUST_SWEEPER"      # Consolidates small amounts
    HODLER = "HODLER"                   # Long-term holder
    MIXER = "MIXER"                     # Privacy-focused, uses mixers
    TRADER = "TRADER"                   # Active trading
    WHALE = "WHALE"                     # Large holder (>$10M)
    
    # Legacy/Fallback Typen (optional behalten für Rückwärtskompatibilität)
    DEV_WALLET = "DEV_WALLET"           # Developer wallet (frühe Transaktionen)
    SNIPER_WALLET = "SNIPER_WALLET"     # Very early buyer
    RUGPULL_SUSPECT = "RUGPULL_SUSPECT" # Suspicious selling pattern
    
    # Standard Typen
    BURN_WALLET = "BURN_WALLET"         # Burn address
    DEX_CONTRACT = "DEX_CONTRACT"       # DEX smart contract
    CEX_WALLET = "CEX_WALLET"           # Centralized exchange
    UNKNOWN = "UNKNOWN"                 # Unclassified
    
    # Old types for backwards compatibility
    EOA = "EOA"
    CONTRACT = "CONTRACT"
    DEFI_WALLET = "DEFI_WALLET"


class WalletAnalysisModel(Base):
    """SQLAlchemy model for wallet analysis (database table)"""
    __tablename__ = "wallet_analyses"
    
    # Primärschlüssel und Identifikation
    id = Column(Integer, primary_key=True, index=True)
    wallet_address = Column(String(255), nullable=False, index=True)
    chain = Column(String(20), nullable=False, index=True)
    
    # Wallet-Klassifizierung
    wallet_type = Column(SQLEnum(WalletTypeEnum), nullable=False, index=True)
    confidence_score = Column(Float)  # 0-1, wie sicher die Klassifizierung ist
    
    # Token-bezogene Daten
    token_id = Column(Integer, ForeignKey("tokens.id"), nullable=True)
    token_address = Column(String(255))
    balance = Column(Float)
    percentage_of_supply = Column(Float)
    
    # Transaktionsdaten
    transaction_count = Column(Integer, default=0)
    first_transaction = Column(DateTime)
    last_transaction = Column(DateTime)
    
    # Risikoanalyse
    risk_score = Column(Float)  # 0-100
    risk_flags = Column(JSON)  # Liste der Risiko-Flags
    
    # Metadaten
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Beziehungen
    # token = relationship("Token", back_populates="wallet_analyses")
    # address = relationship("Address", back_populates="wallet_analysis")
    
    # Indexe für Performance-Optimierung
    __table_args__ = (
        Index('idx_wallet_analysis_token', 'token_address', 'chain'),
        Index('idx_wallet_analysis_type', 'wallet_type'),
        Index('idx_wallet_analysis_risk', 'risk_score'),
        Index('idx_wallet_analysis_created', 'created_at'),
    )
    
    def __repr__(self):
        return f"<WalletAnalysisModel(address='{self.wallet_address[:8]}...', type='{self.wallet_type.value}', risk={self.risk_score})>"
    
    def to_dict(self):
        """Konvertiert das WalletAnalysis-Objekt in ein Dictionary"""
        return {
            'id': self.id,
            'wallet_address': self.wallet_address,
            'chain': self.chain,
            'wallet_type': self.wallet_type.value if self.wallet_type else None,
            'confidence_score': self.confidence_score,
            'token_id': self.token_id,
            'token_address': self.token_address,
            'balance': self.balance,
            'percentage_of_supply': self.percentage_of_supply,
            'transaction_count': self.transaction_count,
            'first_transaction': self.first_transaction.isoformat() if self.first_transaction else None,
            'last_transaction': self.last_transaction.isoformat() if self.last_transaction else None,
            'risk_score': self.risk_score,
            'risk_flags': self.risk_flags,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }


@dataclass
class WalletAnalysis:
    """
    Data class für Wallet-Analyse-Ergebnisse (in-memory representation)
    """
    wallet_address: str
    wallet_type: WalletTypeEnum
    balance: float
    percentage_of_supply: float
    transaction_count: int
    first_transaction: Optional[datetime] = None
    last_transaction: Optional[datetime] = None
    risk_score: float = 0.0  # 0-100 scale
    
    # Zusätzliche Metriken aus dem 3-Stage-System (optional)
    classification_confidence: Optional[float] = None  # 0-1 scale
    stage1_metrics: Optional[dict] = None
    stage2_metrics: Optional[dict] = None
    stage3_metrics: Optional[dict] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'wallet_address': self.wallet_address,
            'wallet_type': self.wallet_type.value,
            'balance': self.balance,
            'percentage_of_supply': self.percentage_of_supply,
            'transaction_count': self.transaction_count,
            'first_transaction': self.first_transaction.isoformat() if self.first_transaction else None,
            'last_transaction': self.last_transaction.isoformat() if self.last_transaction else None,
            'risk_score': self.risk_score,
            'classification_confidence': self.classification_confidence
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'WalletAnalysis':
        """Create from dictionary"""
        return cls(
            wallet_address=data['wallet_address'],
            wallet_type=WalletTypeEnum(data['wallet_type']),
            balance=data['balance'],
            percentage_of_supply=data['percentage_of_supply'],
            transaction_count=data['transaction_count'],
            first_transaction=datetime.fromisoformat(data['first_transaction']) if data.get('first_transaction') else None,
            last_transaction=datetime.fromisoformat(data['last_transaction']) if data.get('last_transaction') else None,
            risk_score=data.get('risk_score', 0.0),
            classification_confidence=data.get('classification_confidence')
        )


# Helper function für Wallet-Typ-Beschreibungen
def get_wallet_type_description(wallet_type: WalletTypeEnum) -> str:
    """
    Returns a human-readable description of the wallet type
    """
    descriptions = {
        WalletTypeEnum.DUST_SWEEPER: "Consolidates small amounts from multiple sources",
        WalletTypeEnum.HODLER: "Long-term holder with minimal outgoing activity",
        WalletTypeEnum.MIXER: "Uses privacy tools like CoinJoin or Tornado Cash",
        WalletTypeEnum.TRADER: "Active trader with frequent transactions",
        WalletTypeEnum.WHALE: "Large holder with significant portfolio value (>$10M)",
        WalletTypeEnum.DEV_WALLET: "Developer or team wallet (early access)",
        WalletTypeEnum.SNIPER_WALLET: "Very early buyer (within minutes of launch)",
        WalletTypeEnum.RUGPULL_SUSPECT: "Suspicious large selling activity",
        WalletTypeEnum.BURN_WALLET: "Burn address (permanent lock)",
        WalletTypeEnum.DEX_CONTRACT: "Decentralized exchange smart contract",
        WalletTypeEnum.CEX_WALLET: "Centralized exchange wallet",
        WalletTypeEnum.UNKNOWN: "Unclassified wallet"
    }
    return descriptions.get(wallet_type, "Unknown wallet type")


# Helper function für Risk-Level basierend auf Wallet-Typ
def get_wallet_type_risk_level(wallet_type: WalletTypeEnum) -> str:
    """
    Returns risk level associated with wallet type
    Returns: "low", "medium", "high", or "critical"
    """
    risk_levels = {
        WalletTypeEnum.DUST_SWEEPER: "low",
        WalletTypeEnum.HODLER: "low",
        WalletTypeEnum.MIXER: "high",
        WalletTypeEnum.TRADER: "medium",
        WalletTypeEnum.WHALE: "medium",
        WalletTypeEnum.DEV_WALLET: "high",
        WalletTypeEnum.SNIPER_WALLET: "medium",
        WalletTypeEnum.RUGPULL_SUSPECT: "critical",
        WalletTypeEnum.BURN_WALLET: "low",
        WalletTypeEnum.DEX_CONTRACT: "low",
        WalletTypeEnum.CEX_WALLET: "low",
        WalletTypeEnum.UNKNOWN: "medium"
    }
    return risk_levels.get(wallet_type, "medium")
