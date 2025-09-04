from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.backend_crypto_tracker.config.database import get_db
from app.core.backend_crypto_tracker.api.controllers.contract_controller import contract_controller
from app.core.backend_crypto_tracker.utils.logger import get_logger
from typing import Optional

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1/contracts", tags=["contracts"])

@router.get("/{address}/info", response_model=dict)
async def get_contract_info(
    address: str = Path(..., description="Smart Contract address"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    db: AsyncSession = Depends(get_db)
):
    """Get comprehensive smart contract information"""
    try:
        return await contract_controller.get_contract_info(address, chain, db)
    except Exception as e:
        logger.error(f"Error in get_contract_info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{address}/interactions", response_model=list)
async def get_contract_interactions(
    address: str = Path(..., description="Smart Contract address"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    time_period: str = Query("24h", description="Time period (1h, 24h, 7d, 30d)"),
    db: AsyncSession = Depends(get_db)
):
    """Analyze contract interactions and method calls"""
    try:
        return await contract_controller.get_contract_interactions(address, chain, time_period, db)
    except Exception as e:
        logger.error(f"Error in get_contract_interactions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{address}/security", response_model=dict)
async def get_contract_security_assessment(
    address: str = Path(..., description="Smart Contract address"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    db: AsyncSession = Depends(get_db)
):
    """Get comprehensive security assessment"""
    try:
        return await contract_controller.get_contract_security_assessment(address, chain)
    except Exception as e:
        logger.error(f"Error in get_contract_security_assessment: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{address}/time-series", response_model=dict)
async def get_contract_time_series(
    address: str = Path(..., description="Smart Contract address"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    time_period: str = Query("24h", description="Time period (1h, 24h, 7d, 30d)"),
    interval: str = Query("1h", description="Data interval (1m, 5m, 15m, 1h, 4h)"),
    db: AsyncSession = Depends(get_db)
):
    """Get time series data for contract activity"""
    try:
        return await contract_controller.get_contract_time_series(address, chain, time_period, interval)
    except Exception as e:
        logger.error(f"Error in get_contract_time_series: {e}")
        raise HTTPException(status_code=500, detail=str(e))
