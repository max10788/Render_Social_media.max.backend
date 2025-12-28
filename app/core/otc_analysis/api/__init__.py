"""
OTC Analysis API - Module-based Router
======================================

Aggregates all sub-routers from modular endpoint files.
"""

from fastapi import APIRouter

# Import sub-routers
from .desks import router as desks_router
from .wallets import router as wallets_router
from .statistics import router as statistics_router
from .network import router as network_router
from .flow import router as flow_router
from .monitoring import router as monitoring_router
from .admin import router as admin_router

# Create main router
router = APIRouter(prefix="/api/otc", tags=["OTC Analysis"])

# Include all sub-routers
router.include_router(desks_router)
router.include_router(wallets_router)
router.include_router(statistics_router)
router.include_router(network_router)
router.include_router(flow_router)
router.include_router(monitoring_router)
router.include_router(admin_router)

__all__ = ["router"]
