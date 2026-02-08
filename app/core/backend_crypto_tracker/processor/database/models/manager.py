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
from sqlalchemy import create_engine, text, func, and_, or_, select, delete
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

# Import Exceptions
from app.core.backend_crypto_tracker.utils.exceptions import DatabaseException, InvalidAddressException
from app.core.backend_crypto_tracker.utils.logger import get_logger

from app.core.backend_crypto_tracker.config.database import (
    database_config,
    AsyncSessionLocal,
    SessionLocal,
    engine,
    async_engine
)

logger = get_logger(__name__)

class DatabaseManager:
    def __init__(self):
        self.database_config = database_config
        self.AsyncSessionLocal = AsyncSessionLocal
        self.SessionLocal = SessionLocal
        self.engine = engine
        self.async_engine = async_engine

    async def initialize(self):
        """Initializes the database connection and creates tables"""
        try:
            async with self.async_engine.begin() as conn:
                if hasattr(self.database_config, 'schema_name'):
                    await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.database_config.schema_name}"))
                    await conn.execute(text(f"SET search_path TO {self.database_config.schema_name}, public"))

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
        # Don't dispose shared engines - they are managed at application level
        logger.info("DatabaseManager closed.")

    # Token-Methoden
    async def get_tokens(self, limit: int = 50, min_score: float = 0,
                         chain: Optional[str] = None, search: Optional[str] = None,
                         max_market_cap: Optional[float] = None) -> List[Dict[str, Any]]:
        """Holt eine Liste der analysierten Tokens"""
        async with self.get_async_session() as session:
            try:
                stmt = select(Token)

                if min_score > 0:
                    stmt = stmt.where(Token.token_score >= min_score)

                if chain:
                    stmt = stmt.where(Token.chain == chain)

                if max_market_cap is not None:
                    stmt = stmt.where(Token.market_cap <= max_market_cap)

                if search:
                    stmt = stmt.where(
                        or_(
                            Token.name.ilike(f"%{search}%"),
                            Token.symbol.ilike(f"%{search}%")
                        )
                    )

                stmt = stmt.order_by(Token.token_score.desc()).limit(limit)
                result = await session.execute(stmt)
                tokens = result.scalars().all()

                return [token.to_dict() for token in tokens]
            except SQLAlchemyError as e:
                logger.error(f"Database error fetching tokens: {e}")
                raise DatabaseException(f"Failed to fetch tokens: {str(e)}")

    async def get_token_by_address(self, address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt ein Token anhand seiner Adresse"""
        async with self.get_async_session() as session:
            try:
                stmt = select(Token).where(
                    and_(Token.address == address, Token.chain == chain)
                )
                result = await session.execute(stmt)
                token = result.scalars().first()

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
                stmt = select(Token).where(
                    and_(Token.address == token_data['address'], Token.chain == token_data['chain'])
                )
                result = await session.execute(stmt)
                existing_token = result.scalars().first()

                if existing_token:
                    for key, value in token_data.items():
                        if hasattr(existing_token, key):
                            setattr(existing_token, key, value)
                    existing_token.updated_at = datetime.utcnow()
                    session.add(existing_token)
                    await session.flush()
                    token_result = existing_token
                else:
                    token = Token(**token_data)
                    session.add(token)
                    await session.flush()
                    token_result = token

                await session.commit()
                return token_result.to_dict()
            except SQLAlchemyError as e:
                logger.error(f"Database error saving token: {e}")
                raise DatabaseException(f"Failed to save token: {str(e)}")

    async def update_token_price(self, address: str, chain: str, price_data: Dict[str, Any]) -> bool:
        """Aktualisiert die Preisdaten eines Tokens"""
        async with self.get_async_session() as session:
            try:
                stmt = select(Token).where(
                    and_(Token.address == address, Token.chain == chain)
                )
                result = await session.execute(stmt)
                token = result.scalars().first()

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
                stmt = select(CustomAnalysis)

                if user_id:
                    stmt = stmt.where(CustomAnalysis.user_id == user_id)
                elif session_id:
                    stmt = stmt.where(CustomAnalysis.session_id == session_id)

                stmt = stmt.order_by(CustomAnalysis.analysis_date.desc()).limit(limit)
                result = await session.execute(stmt)
                analyses = result.scalars().all()

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
        """Holt Statistiken fuer verschiedene Chains"""
        async with self.get_async_session() as session:
            try:
                stmt = select(
                    CustomAnalysis.chain,
                    func.count(CustomAnalysis.id).label('total_analyses'),
                    func.avg(CustomAnalysis.total_score).label('avg_score'),
                    func.max(CustomAnalysis.total_score).label('max_score'),
                    func.min(CustomAnalysis.total_score).label('min_score')
                ).group_by(CustomAnalysis.chain)

                result = await session.execute(stmt)
                rows = result.all()

                stats = {}
                for row in rows:
                    stats[row.chain] = {
                        'total_analyses': row.total_analyses,
                        'average_score': round(row.avg_score, 2) if row.avg_score else 0,
                        'max_score': row.max_score,
                        'min_score': row.min_score
                    }

                return stats
            except SQLAlchemyError as e:
                logger.error(f"Error fetching chain statistics: {e}")
                raise DatabaseException(f"Failed to fetch chain statistics: {str(e)}")

    async def save_token_analysis(self, analysis_result: Dict) -> bool:
        """Speichert eine vollstaendige Token-Analyse"""
        async with self.get_async_session() as session:
            try:
                token_data = analysis_result.get('token_data', {})
                if hasattr(token_data, '__dict__'):
                    token_dict = token_data.__dict__
                else:
                    token_dict = token_data

                token = await self.save_token(token_dict)

                scan_result = ScanResult(
                    token_id=token['id'],
                    score=analysis_result.get('token_score', 0),
                    metrics=analysis_result.get('metrics', {}),
                    analysis_date=datetime.utcnow()
                )

                session.add(scan_result)

                wallet_analyses = analysis_result.get('wallet_analyses', [])
                for wallet_analysis in wallet_analyses:
                    if hasattr(wallet_analysis, '__dict__'):
                        wallet_dict = wallet_analysis.__dict__
                    else:
                        wallet_dict = wallet_analysis

                    wallet_dict['token_id'] = token['id']
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
                # Count scan results to delete
                count_stmt = select(func.count()).select_from(ScanResult).where(
                    ScanResult.analysis_date < cutoff_date
                )
                result = await session.execute(count_stmt)
                scan_result_count = result.scalar() or 0

                if scan_result_count > 0:
                    del_stmt = delete(ScanResult).where(ScanResult.analysis_date < cutoff_date)
                    await session.execute(del_stmt)

                # Count custom analyses to delete
                count_stmt = select(func.count()).select_from(CustomAnalysis).where(
                    CustomAnalysis.analysis_date < cutoff_date
                )
                result = await session.execute(count_stmt)
                custom_analysis_count = result.scalar() or 0

                if custom_analysis_count > 0:
                    del_stmt = delete(CustomAnalysis).where(CustomAnalysis.analysis_date < cutoff_date)
                    await session.execute(del_stmt)

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
                stmt = select(ScanJob).where(ScanJob.id == scan_job.id)
                result = await session.execute(stmt)
                existing_job = result.scalars().first()

                if existing_job:
                    for key, value in scan_job.__dict__.items():
                        if hasattr(existing_job, key) and not key.startswith('_'):
                            setattr(existing_job, key, value)
                    await session.flush()
                    job_result = existing_job
                else:
                    session.add(scan_job)
                    await session.flush()
                    job_result = scan_job

                await session.commit()
                return job_result.to_dict()
            except SQLAlchemyError as e:
                logger.error(f"Database error saving scan job: {e}")
                raise DatabaseException(f"Failed to save scan job: {str(e)}")

    async def get_scan_job(self, scan_id: str) -> Optional[Dict[str, Any]]:
        """Holt einen Scan-Job anhand seiner ID"""
        async with self.get_async_session() as session:
            try:
                stmt = select(ScanJob).where(ScanJob.id == scan_id)
                result = await session.execute(stmt)
                scan_job = result.scalars().first()

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
                stmt = select(ScanJob)

                if status:
                    stmt = stmt.where(ScanJob.status == status)

                stmt = stmt.order_by(ScanJob.start_time.desc()).limit(limit)
                result = await session.execute(stmt)
                scan_jobs = result.scalars().all()

                return [job.to_dict() for job in scan_jobs]
            except SQLAlchemyError as e:
                logger.error(f"Database error fetching scan jobs: {e}")
                raise DatabaseException(f"Failed to fetch scan jobs: {str(e)}")
