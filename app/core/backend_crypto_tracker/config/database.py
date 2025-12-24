# app/core/backend_crypto_tracker/config/database.py
import os
from urllib.parse import urlparse
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# ============================================================================
# SHARED BASE FOR ALL MODELS - WICHTIG!
# ============================================================================
Base = declarative_base()

class DatabaseConfig:
    def __init__(self):
        # Render.com stellt die Datenbank-URL über die Umgebungsvariable DATABASE_URL bereit
        self.database_url = os.getenv("DATABASE_URL")
        
        if not self.database_url:
            # Fallback für lokale Entwicklung
            logger.warning("DATABASE_URL not found, using fallback configuration")
            self.database_url = os.getenv(
                "POSTGRES_URL",
                "postgresql://postgres:password@localhost:5432/lowcap_analyzer"
            )
        
        # Für SQLAlchemy mit asyncpg
        self.async_database_url = self.database_url.replace("postgresql://", "postgresql+asyncpg://")
        
        # Parse die URL, um einzelne Komponenten zu extrahieren
        parsed_url = urlparse(self.database_url)
        
        self.db_user = parsed_url.username
        self.db_password = parsed_url.password
        self.db_host = parsed_url.hostname
        self.db_port = parsed_url.port or 5432
        self.db_name = parsed_url.path.lstrip('/')
        
        # Connection Pool Einstellungen
        self.pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
        self.max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "20"))
        self.pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "30"))
        self.pool_recycle = int(os.getenv("DB_POOL_RECYCLE", "3600"))
        
        # ✅ ANGEPASST: Schema für OTC Analysis
        # Du kannst zwischen beiden wählen:
        # Option 1: Eigenes Schema für OTC
        self.schema_name = os.getenv("OTC_SCHEMA", "otc_analysis")
        # Option 2: Shared Schema mit Token Analyzer
        # self.schema_name = "token_analyzer"
        
        logger.info(f"Database configuration: host={self.db_host}, port={self.db_port}, database={self.db_name}, schema={self.schema_name}")

# Globale Instanz
database_config = DatabaseConfig()

# Synchrone Engine und Session für FastAPI-Routen
engine = create_engine(
    database_config.database_url,
    pool_size=database_config.pool_size,
    max_overflow=database_config.max_overflow,
    pool_timeout=database_config.pool_timeout,
    pool_recycle=database_config.pool_recycle,
    echo=os.getenv("DB_ECHO", "false").lower() == "true",
    connect_args={"options": f"-csearch_path={database_config.schema_name},public"}
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# Dependency für FastAPI
def get_db() -> Generator[Session, None, None]:
    """Stellt eine Datenbank-Session für FastAPI-Routen bereit"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

def init_db():
    """
    Erstellt alle Tabellen für OTC Analysis.
    
    WICHTIG: Führe diese Funktion einmal aus, um Tables zu erstellen!
    
    Usage:
        from app.core.backend_crypto_tracker.config.database import init_db
        init_db()
    """
    logger.info("🔨 Initialisiere OTC Analysis Datenbank...")
    
    # Import aller Models (damit sie in Base.metadata registriert sind)
    from app.core.otc_analysis.models.wallet import Wallet
    from app.core.otc_analysis.models.watchlist import WatchlistItem
    from app.core.otc_analysis.models.alert import Alert
    
    # Schema erstellen falls nicht existiert
    with engine.connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {database_config.schema_name}")
        conn.commit()
        logger.info(f"✅ Schema '{database_config.schema_name}' bereit")
    
    # Alle Tables erstellen
    Base.metadata.create_all(bind=engine)
    
    logger.info("✅ OTC Analysis Tabellen erstellt:")
    for table in Base.metadata.sorted_tables:
        logger.info(f"   • {table.name}")

def drop_all_tables():
    """
    ⚠️ VORSICHT: Löscht alle OTC Analysis Tabellen!
    
    Nur für Development/Testing!
    """
    logger.warning("⚠️  Lösche alle OTC Analysis Tabellen...")
    
    # Import Models
    from app.core.otc_analysis.models.wallet import Wallet
    from app.core.otc_analysis.models.watchlist import WatchlistItem
    from app.core.otc_analysis.models.alert import Alert
    
    Base.metadata.drop_all(bind=engine)
    logger.info("✅ Alle Tabellen gelöscht")

def check_connection():
    """Test der Datenbankverbindung"""
    try:
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            logger.info("✅ Datenbankverbindung erfolgreich")
            return True
    except Exception as e:
        logger.error(f"❌ Datenbankverbindung fehlgeschlagen: {e}")
        return False
