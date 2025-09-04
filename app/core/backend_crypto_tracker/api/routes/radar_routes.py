from fastapi import APIRouter, Depends, HTTPException, Query, Path
from app.core.backend_crypto_tracker.api.controllers.radar_controller import radar_controller
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1/radar", tags=["radar"])

@router.get("/contract/{address}/data")
async def get_contract_radar_data(
    address: str = Path(..., description="Smart Contract address"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    time_period: str = Query("24h", description="Time period (1h, 24h, 7d, 30d)")
):
    """Get comprehensive radar data for a smart contract"""
    try:
        return await radar_controller.get_contract_radar_data(address, chain, time_period)
    except Exception as e:
        logger.error(f"Error in get_contract_radar_data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/wallet/{address}/details")
async def get_wallet_radar_details(
    address: str = Path(..., description="Wallet address"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    contract_address: str = Query(..., description="Smart Contract address"),
    time_period: str = Query("24h", description="Time period (1h, 24h, 7d, 30d)")
):
    """Get detailed radar information for a specific wallet"""
    try:
        return await radar_controller.get_wallet_radar_details(
            address, chain, contract_address, time_period
        )
    except Exception as e:
        logger.error(f"Error in get_wallet_radar_details: {e}")
        raise HTTPException(status_code=500, detail=str(e))
