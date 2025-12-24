# app/core/backend_crypto_tracker/config/database.py
import os
from urllib.parse import urlparse
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

Base = declarative_base()

class DatabaseConfig:
    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL")
        
        if not self.database_url:
            logger.warning("DATABASE_URL not found, using fallback configuration")
            self.database_url = os.getenv(
                "POSTGRES_URL",
                "postgresql://postgres:password@localhost:5432/lowcap_analyzer"
            )
        
        self.async_database_url = self.database_url.replace("postgresql://", "postgresql+asyncpg://")
        
        parsed_url = urlparse(self.database_url)
        
        self.db_user = parsed_url.username
        self.db_password = parsed_url.password
        self.db_host = parsed_url.hostname
        self.db_port = parsed_url.port or 5432
        self.db_name = parsed_url.path.lstrip('/')
        
        self.pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
        self.max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "20"))
        self.pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "30"))
        self.pool_recycle = int(os.getenv("DB_POOL_RECYCLE", "3600"))
        
        self.schema_name = os.getenv("OTC_SCHEMA", "otc_analysis")
        self.ssl_mode = "require"
        
        logger.info(f"Database configuration: host={self.db_host}, port={self.db_port}, database={self.db_name}, schema={self.schema_name}, ssl_mode={self.ssl_mode}")

database_config = DatabaseConfig()

# ✅ KRITISCH: pool_pre_ping=True hinzugefügt!
engine = create_engine(
    database_config.database_url,
    pool_pre_ping=True,  # ✅ FIXES SSL ERROR
    pool_size=database_config.pool_size,
    max_overflow=database_config.max_overflow,
    pool_timeout=database_config.pool_timeout,
    pool_recycle=database_config.pool_recycle,
    echo=os.getenv("DB_ECHO", "false").lower() == "true",
    connect_args={
        "options": f"-csearch_path={database_config.schema_name},public",
        "sslmode": database_config.ssl_mode
    }
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

async_engine = create_async_engine(
    database_config.async_database_url,
    pool_pre_ping=True,  # ✅ FIXES SSL ERROR
    pool_size=database_config.pool_size,
    max_overflow=database_config.max_overflow,
    pool_timeout=database_config.pool_timeout,
    pool_recycle=database_config.pool_recycle,
    echo=os.getenv("DB_ECHO", "false").lower() == "true",
    connect_args={
        "server_settings": {
            "search_path": f"{database_config.schema_name},public"
        },
        "ssl": database_config.ssl_mode
    }
)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False
)

def get_db() -> Generator[Session, None, None]:
    """Stellt eine Datenbank-Session für FastAPI-Routen bereit"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def get_async_db() -> AsyncSession:
    """Stellt eine asynchrone Datenbank-Session für FastAPI-Routen bereit"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

def init_db():
    """Initialize OTC Analysis database tables"""
    logger.info("🔨 Initialisiere OTC Analysis Datenbank...")
    
    try:
        from app.core.otc_analysis.models.wallet import OTCWallet
        from app.core.otc_analysis.models.watchlist import WatchlistItem
        from app.core.otc_analysis.models.alert import Alert
        logger.info("✅ OTC Models importiert")
    except ImportError as e:
        logger.warning(f"⚠️  OTC Models nicht gefunden: {e}")
        return
    
    try:
        with engine.connect() as conn:
            schema = database_config.schema_name
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.commit()
            logger.info(f"✅ Schema '{schema}' bereit")
    except Exception as e:
        logger.warning(f"⚠️  Schema creation skipped: {e}")
    
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ OTC Analysis Tabellen erstellt:")
        for table in Base.metadata.sorted_tables:
            if 'otc' in table.name.lower():
                logger.info(f"   • {table.name}")
    except Exception as e:
        logger.error(f"❌ Fehler beim Erstellen der Tabellen: {e}")
        raise

def check_connection():
    """Test database connection"""
    try:
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            logger.info("✅ Datenbankverbindung erfolgreich")
            return True
    except Exception as e:
        logger.error(f"❌ Datenbankverbindung fehlgeschlagen: {e}")
        return False

def drop_all_tables():
    """⚠️ VORSICHT: Löscht alle OTC Analysis Tabellen!"""
    logger.warning("⚠️  Lösche alle OTC Analysis Tabellen...")
    
    try:
        from app.core.otc_analysis.models.wallet import OTCWallet
        from app.core.otc_analysis.models.watchlist import WatchlistItem
        from app.core.otc_analysis.models.alert import Alert
    except ImportError:
        logger.warning("⚠️  OTC Models nicht gefunden")
        return
    
    Base.metadata.drop_all(bind=engine)
    logger.info("✅ Alle OTC Tabellen gelöscht")
