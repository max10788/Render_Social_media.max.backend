import os
import logging
from typing import Optional, List, Dict, Any, Union, AsyncGenerator
from datetime import datetime, timedelta
from contextlib import contextmanager, asynccontextmanager
# Import Models
from app.core.backend_crypto_tracker.processor.database.models.token import Token
from app.core.backend_crypto_tracker.processor.database.models.wallet import WalletAnalysis, WalletTypeEnum
from app.core.backend_crypto_tracker.processor.database.models.scan_result import ScanResult
from app.core.backend_crypto_tracker.processor.database.models.scan_job import ScanJob, ScanStatus
from app.core.backend_crypto_tracker.processor.database.models.custom_analysis import CustomAnalysis
# Import SQLAlchemy
from sqlalchemy import create_engine, text, func, and_, or_
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
# Import Exceptions
from app.core.backend_crypto_tracker.utils.exceptions import DatabaseException, InvalidAddressException
from app.core.backend_crypto_tracker.utils.logger import get_logger
# Import the database configuration and sessions
from app.core.backend_crypto_tracker.config.database import database_config, AsyncSessionLocal, SessionLocal

logger = get_logger(__name__)

class DatabaseConfig:
    def __init__(self, database_type: str = "postgresql"):
        self.database_type = database_type
        self.postgres_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'lowcap_analyzer'),
            'username': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'password'),
            'pool_size': int(os.getenv('DB_POOL_SIZE', '10')),
            'max_overflow': int(os.getenv('DB_MAX_OVERFLOW', '20')),
            'pool_timeout': int(os.getenv('DB_POOL_TIMEOUT', '30')),
            'pool_recycle': int(os.getenv('DB_POOL_RECYCLE', '3600')),
            'sslmode': os.getenv('POSTGRES_SSLMODE', 'require')  # SSL-Modus hinzugefügt
        }
        
    def get_postgres_url(self) -> str:
        # Entferne sslmode aus der URL, da wir es in connect_args übergeben
        return f"postgresql://{self.postgres_config['username']}:{self.postgres_config['password']}@{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}"
    
    def get_async_postgres_url(self) -> str:
        # Entferne sslmode aus der URL, da wir es in connect_args übergeben
        return f"postgresql+asyncpg://{self.postgres_config['username']}:{self.postgres_config['password']}@{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}"

class DatabaseManager:
    def __init__(self):
        # Use the existing configuration and sessions
        self.database_config = database_config
        self.AsyncSessionLocal = AsyncSessionLocal
        self.SessionLocal = SessionLocal
        self.engine = None
        
    async def initialize(self):
        """Initializes the database connection and creates tables"""
        try:
            # Get the engine from the AsyncSessionLocal
            self.engine = self.AsyncSessionLocal.kw['bind']
            
            # Create schema and tables
            async with self.engine.begin() as conn:
                # Create schema if it doesn't exist (only for PostgreSQL)
                if hasattr(self.database_config, 'schema_name'):
                    await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.database_config.schema_name}"))
                    # Set search path
                    await conn.execute(text(f"SET search_path TO {self.database_config.schema_name}, public"))
                
                # Create tables
                from app.core.backend_crypto_tracker.processor.database.models import Base
                await conn.run_sync(Base.metadata.create_all)
            
            logger.info("Database Manager initialized and tables created.")
        except Exception as e:
            logger.error(f"Error initializing DatabaseManager: {e}")
            raise DatabaseException(f"Failed to initialize database: {str(e)}")
    
    @contextmanager
    def get_session(self):
        """Context Manager for synchronous database sessions"""
        if not self.SessionLocal:
            raise RuntimeError("DatabaseManager not initialized for synchronous mode.")
        
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise DatabaseException(f"Database operation failed: {str(e)}")
        finally:
            session.close()
    
    @asynccontextmanager
    async def get_async_session(self):
        """Async Context Manager for database sessions"""
        if not self.AsyncSessionLocal:
            raise RuntimeError("DatabaseManager not initialized for asynchronous mode.")
        
        async with self.AsyncSessionLocal() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Database session error: {e}")
                raise DatabaseException(f"Database operation failed: {str(e)}")
    
    async def close(self):
        """Closes the database connection"""
        if self.engine:
            await self.engine.dispose()
            logger.info("Database connection closed.")
    
    # Token-Methoden
    async def get_tokens(self, limit: int = 50, min_score: float = 0, 
                         chain: Optional[str] = None, search: Optional[str] = None,
                         max_market_cap: Optional[float] = None) -> List[Dict[str, Any]]:
        """Holt eine Liste der analysierten Tokens"""
        async with self.get_async_session() as session:
            try:
                query = session.query(Token)
                
                if min_score > 0:
                    query = query.filter(Token.token_score >= min_score)
                
                if chain:
                    query = query.filter(Token.chain == chain)
                
                if max_market_cap is not None:
                    query = query.filter(Token.market_cap <= max_market_cap)
                
                if search:
                    query = query.filter(
                        or_(
                            Token.name.ilike(f"%{search}%"),
                            Token.symbol.ilike(f"%{search}%")
                        )
                    )
                
                tokens = query.order_by(Token.token_score.desc()).limit(limit).all()
                
                return [token.to_dict() for token in tokens]
            except SQLAlchemyError as e:
                logger.error(f"Database error fetching tokens: {e}")
                raise DatabaseException(f"Failed to fetch tokens: {str(e)}")
    
    async def get_token_by_address(self, address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt ein Token anhand seiner Adresse"""
        async with self.get_async_session() as session:
            try:
                token = session.query(Token).filter(
                    and_(Token.address == address, Token.chain == chain)
                ).first()
                
                if not token:
                    return None
                
                return token.to_dict()
            except SQLAlchemyError as e:
                logger.error(f"Database error fetching token by address: {e}")
                raise DatabaseException(f"Failed to fetch token by address: {str(e)}")
    
    async def save_token(self, token_data: Dict[str, Any]) -> Dict[str, Any]:
        """Speichert oder aktualisiert ein Token"""
        async with self.get_async_session() as session:
            try:
                # Prüfen, ob das Token bereits existiert
                existing_token = session.query(Token).filter(
                    and_(Token.address == token_data['address'], Token.chain == token_data['chain'])
                ).first()
                
                if existing_token:
                    # Token aktualisieren
                    for key, value in token_data.items():
                        if hasattr(existing_token, key):
                            setattr(existing_token, key, value)
                    existing_token.updated_at = datetime.utcnow()
                    session.add(existing_token)
                    await session.flush()
                    result = existing_token
                else:
                    # Neues Token erstellen
                    token = Token(**token_data)
                    session.add(token)
                    await session.flush()
                    result = token
                
                await session.commit()
                return result.to_dict()
            except SQLAlchemyError as e:
                logger.error(f"Database error saving token: {e}")
                raise DatabaseException(f"Failed to save token: {str(e)}")
    
    async def update_token_price(self, address: str, chain: str, price_data: Dict[str, Any]) -> bool:
        """Aktualisiert die Preisdaten eines Tokens"""
        async with self.get_async_session() as session:
            try:
                token = session.query(Token).filter(
                    and_(Token.address == address, Token.chain == chain)
                ).first()
                
                if not token:
                    return False
                
                token.market_cap = price_data.get('market_cap', token.market_cap)
                token.volume_24h = price_data.get('volume_24h', token.volume_24h)
                token.last_analyzed = datetime.utcnow()
                
                await session.commit()
                return True
            except SQLAlchemyError as e:
                logger.error(f"Database error updating token price: {e}")
                raise DatabaseException(f"Failed to update token price: {str(e)}")
    
    # Custom Analysis Methoden
    async def save_custom_analysis(self, analysis_data: Dict) -> int:
        """Speichert eine benutzerdefinierte Token-Analyse"""
        async with self.get_async_session() as session:
            try:
                custom_analysis = CustomAnalysis(
                    token_address=analysis_data['token_address'],
                    chain=analysis_data['chain'],
                    token_name=analysis_data.get('token_name'),
                    token_symbol=analysis_data.get('token_symbol'),
                    market_cap=analysis_data.get('market_cap', 0),
                    volume_24h=analysis_data.get('volume_24h', 0),
                    liquidity=analysis_data.get('liquidity', 0),
                    holders_count=analysis_data.get('holders_count', 0),
                    total_score=analysis_data['total_score'],
                    metrics=analysis_data.get('metrics', {}),
                    risk_flags=analysis_data.get('risk_flags', []),
                    user_id=analysis_data.get('user_id'),
                    session_id=analysis_data.get('session_id')
                )
                
                session.add(custom_analysis)
                await session.flush()
                await session.commit()
                
                logger.info(f"Custom analysis saved with ID: {custom_analysis.id}")
                return custom_analysis.id
            except SQLAlchemyError as e:
                await session.rollback()
                logger.error(f"Error saving custom analysis: {e}")
                raise DatabaseException(f"Failed to save custom analysis: {str(e)}")
    
    async def get_custom_analysis_history(self, user_id: str = None, 
                                        session_id: str = None, 
                                        limit: int = 50) -> List[Dict]:
        """Holt Historie der benutzerdefinierten Analysen"""
        async with self.get_async_session() as session:
            try:
                query = session.query(CustomAnalysis)
                
                if user_id:
                    query = query.filter(CustomAnalysis.user_id == user_id)
                elif session_id:
                    query = query.filter(CustomAnalysis.session_id == session_id)
                
                analyses = query.order_by(CustomAnalysis.analysis_date.desc()).limit(limit).all()
                
                return [
                    {
                        'id': analysis.id,
                        'token_address': analysis.token_address,
                        'chain': analysis.chain,
                        'token_name': analysis.token_name,
                        'token_symbol': analysis.token_symbol,
                        'total_score': analysis.total_score,
                        'analysis_date': analysis.analysis_date.isoformat(),
                        'risk_flags': analysis.risk_flags
                    }
                    for analysis in analyses
                ]
            except SQLAlchemyError as e:
                logger.error(f"Error fetching custom analysis history: {e}")
                raise DatabaseException(f"Failed to fetch custom analysis history: {str(e)}")
    
    async def get_chain_statistics(self) -> Dict[str, Dict]:
        """Holt Statistiken für verschiedene Chains"""
        async with self.get_async_session() as session:
            try:
                # Statistiken pro Chain
                stats = await session.execute(
                    session.query(
                        CustomAnalysis.chain,
                        func.count(CustomAnalysis.id).label('total_analyses'),
                        func.avg(CustomAnalysis.total_score).label('avg_score'),
                        func.max(CustomAnalysis.total_score).label('max_score'),
                        func.min(CustomAnalysis.total_score).label('min_score')
                    ).group_by(CustomAnalysis.chain)
                )
                
                result = {}
                for stat in stats:
                    result[stat.chain] = {
                        'total_analyses': stat.total_analyses,
                        'average_score': round(stat.avg_score, 2) if stat.avg_score else 0,
                        'max_score': stat.max_score,
                        'min_score': stat.min_score
                    }
                
                return result
            except SQLAlchemyError as e:
                logger.error(f"Error fetching chain statistics: {e}")
                raise DatabaseException(f"Failed to fetch chain statistics: {str(e)}")
    
    async def save_token_analysis(self, analysis_result: Dict) -> bool:
        """Speichert eine vollständige Token-Analyse"""
        async with self.get_async_session() as session:
            try:
                # Token-Daten speichern/aktualisieren
                token_data = analysis_result.get('token_data', {})
                if hasattr(token_data, '__dict__'):
                    token_dict = token_data.__dict__
                else:
                    token_dict = token_data
                
                # Token speichern oder aktualisieren
                token = await self.save_token(token_dict)
                
                # Scan-Ergebnis speichern
                scan_result = ScanResult(
                    token_id=token['id'],
                    score=analysis_result.get('token_score', 0),
                    metrics=analysis_result.get('metrics', {}),
                    analysis_date=datetime.utcnow()
                )
                
                session.add(scan_result)
                
                # Wallet-Analysen speichern
                wallet_analyses = analysis_result.get('wallet_analyses', [])
                for wallet_analysis in wallet_analyses:
                    if hasattr(wallet_analysis, '__dict__'):
                        wallet_dict = wallet_analysis.__dict__
                    else:
                        wallet_dict = wallet_analysis
                    
                    # Token-ID hinzufügen
                    wallet_dict['token_id'] = token['id']
                    
                    # Wallet-Analyse erstellen
                    wallet_analysis_obj = WalletAnalysis(**wallet_dict)
                    session.add(wallet_analysis_obj)
                
                await session.commit()
                logger.info(f"Token analysis saved for token {token.get('symbol', 'Unknown')}")
                return True
            except SQLAlchemyError as e:
                await session.rollback()
                logger.error(f"Error saving token analysis: {e}")
                raise DatabaseException(f"Failed to save token analysis: {str(e)}")
    
    async def cleanup_old_data(self, cutoff_date: datetime) -> int:
        """Bereinigt alte Daten aus der Datenbank"""
        async with self.get_async_session() as session:
            try:
                # Alte Scan-Ergebnisse löschen
                deleted_scan_results = await session.execute(
                    session.query(ScanResult).filter(ScanResult.analysis_date < cutoff_date)
                )
                scan_result_count = len(deleted_scan_results.scalars().all())
                
                if scan_result_count > 0:
                    await session.execute(
                        session.query(ScanResult).filter(ScanResult.analysis_date < cutoff_date).delete()
                    )
                
                # Alte Custom-Analysen löschen
                deleted_custom_analyses = await session.execute(
                    session.query(CustomAnalysis).filter(CustomAnalysis.analysis_date < cutoff_date)
                )
                custom_analysis_count = len(deleted_custom_analyses.scalars().all())
                
                if custom_analysis_count > 0:
                    await session.execute(
                        session.query(CustomAnalysis).filter(CustomAnalysis.analysis_date < cutoff_date).delete()
                    )
                
                await session.commit()
                logger.info(f"Cleaned up {scan_result_count} scan results and {custom_analysis_count} custom analyses")
                return scan_result_count + custom_analysis_count
            except SQLAlchemyError as e:
                await session.rollback()
                logger.error(f"Error cleaning up old data: {e}")
                raise DatabaseException(f"Failed to clean up old data: {str(e)}")
    
    async def save_scan_job(self, scan_job: ScanJob) -> Dict[str, Any]:
        """Speichert einen Scan-Job in der Datenbank"""
        async with self.get_async_session() as session:
            try:
                # Prüfen, ob der Scan-Job bereits existiert
                existing_job = session.query(ScanJob).filter(ScanJob.id == scan_job.id).first()
                
                if existing_job:
                    # Scan-Job aktualisieren
                    for key, value in scan_job.__dict__.items():
                        if hasattr(existing_job, key):
                            setattr(existing_job, key, value)
                    await session.flush()
                    result = existing_job
                else:
                    # Neuen Scan-Job erstellen
                    session.add(scan_job)
                    await session.flush()
                    result = scan_job
                
                await session.commit()
                return result.to_dict()
            except SQLAlchemyError as e:
                logger.error(f"Database error saving scan job: {e}")
                raise DatabaseException(f"Failed to save scan job: {str(e)}")
    
    async def get_scan_job(self, scan_id: str) -> Optional[Dict[str, Any]]:
        """Holt einen Scan-Job anhand seiner ID"""
        async with self.get_async_session() as session:
            try:
                scan_job = session.query(ScanJob).filter(ScanJob.id == scan_id).first()
                
                if not scan_job:
                    return None
                
                return scan_job.to_dict()
            except SQLAlchemyError as e:
                logger.error(f"Database error fetching scan job: {e}")
                raise DatabaseException(f"Failed to fetch scan job: {str(e)}")
    
    async def get_scan_jobs(self, limit: int = 50, status: Optional[ScanStatus] = None) -> List[Dict[str, Any]]:
        """Holt eine Liste von Scan-Jobs"""
        async with self.get_async_session() as session:
            try:
                query = session.query(ScanJob)
                
                if status:
                    query = query.filter(ScanJob.status == status)
                
                scan_jobs = query.order_by(ScanJob.start_time.desc()).limit(limit).all()
                
                return [job.to_dict() for job in scan_jobs]
            except SQLAlchemyError as e:
                logger.error(f"Database error fetching scan jobs: {e}")
                raise DatabaseException(f"Failed to fetch scan jobs: {str(e)}")
