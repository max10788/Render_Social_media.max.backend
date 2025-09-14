# app/core/backend_crypto_tracker/config/database.py
import os
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from typing import Generator, Optional, AsyncGenerator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
import logging
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

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
        
        # Stelle sicher, dass SSL/TLS in der URL enthalten ist
        self.database_url = self._ensure_ssl_parameter(self.database_url)
        
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
        
        # Schema-Name für dieses Tool
        self.schema_name = "token_analyzer"
        
        logger.info(f"Database configuration: host={self.db_host}, port={self.db_port}, database={self.db_name}, schema={self.schema_name}")
    
    def _ensure_ssl_parameter(self, url: str) -> str:
        """Stellt sicher, dass die URL den SSL-Parameter enthält"""
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        # Wenn sslmode nicht gesetzt ist, setzen wir es auf 'require'
        if 'sslmode' not in query_params:
            query_params['sslmode'] = ['require']
            # Baue den Query-String neu
            new_query = urlencode(query_params, doseq=True)
            # Baue die URL neu
            return urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                new_query,
                parsed_url.fragment
            ))
        
        return url

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

# Asynchrone Engine und Session
async_engine = create_async_engine(
    database_config.async_database_url,
    pool_size=database_config.pool_size,
    max_overflow=database_config.max_overflow,
    pool_timeout=database_config.pool_timeout,
    pool_recycle=database_config.pool_recycle,
    echo=os.getenv("DB_ECHO", "false").lower() == "true",
)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)

# Dependency für FastAPI
def get_db() -> Generator[Session, None, None]:
    """Stellt eine Datenbank-Session für FastAPI-Routen bereit"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Stellt eine asynchrone Datenbank-Session bereit"""
    async with AsyncSessionLocal() as session:
        # Setze den Suchpfad nach dem Verbindungsaufbau
        await session.execute(text(f"SET search_path TO {database_config.schema_name},public"))
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
