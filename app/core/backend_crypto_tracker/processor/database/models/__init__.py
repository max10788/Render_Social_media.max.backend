# processor/database/models/__init__.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime, func

# Zentrale Base-Klasse für alle Modelle
Base = declarative_base()

class TimestampMixin:
    """Mixin für Zeitstempel-Felder"""
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

# Importiere alle Modelle
from .address import Address
from .cluster import Cluster
from .custom_analysis import CustomAnalysis
from .scan_result import ScanResult
from .token import Token
from .transaction import Transaction
from .wallet import WalletAnalysisModel, WalletAnalysis, WalletTypeEnum  # Updated import
from .scan_job import ScanJob

# Liste aller Modelle für einfache Referenz
__all__ = [
    'Base',
    'TimestampMixin',
    'Address',
    'Cluster',
    'CustomAnalysis',
    'ScanResult',
    'Token',
    'Transaction',
    'WalletAnalysisModel',  # SQLAlchemy model
    'WalletAnalysis',       # Dataclass
    'WalletTypeEnum',       # Enum
    'ScanJob'
]
