"""
OTC Address Management - Admin Endpoints
=========================================

Admin-only endpoints for managing OTC addresses manually.

Features:
- ✅ Add single OTC addresses
- ✅ Update existing addresses
- ✅ Delete addresses
- ✅ List all addresses with filtering
- ✅ Duplicate check
- ✅ Address validation
- ✅ Fixed: Tags serialization
- ✅ Fixed: Response validation

Version: 1.1
Date: 2025-01-08
"""

import logging
from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Depends, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from pydantic import BaseModel, Field, validator

from app.core.otc_analysis.models.wallet import OTCWallet
from app.core.otc_analysis.api.dependencies import get_db
from app.core.otc_analysis.api.validators import validate_ethereum_address

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/otc-addresses", tags=["Admin - OTC Addresses"])


# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class OTCAddressCreate(BaseModel):
    """Request model for creating OTC address"""
    address: str = Field(..., description="Ethereum address (0x...)")
    entity_type: str = Field(
        default="otc_desk",
        description="Entity type: otc_desk, market_maker, cex, prop_trading, whale"
    )
    entity_name: Optional[str] = Field(None, description="Name (e.g., 'Wintermute')")
    label: Optional[str] = Field(None, description="Display label")
    notes: Optional[str] = Field(None, description="Optional notes")
    tags: Optional[List[str]] = Field(default_factory=list, description="Custom tags")
    is_active: bool = Field(default=True, description="Is address active?")
    
    @validator('address')
    def validate_address(cls, v):
        """Validate Ethereum address format"""
        try:
            return validate_ethereum_address(v)
        except Exception as e:
            raise ValueError(f"Invalid Ethereum address: {str(e)}")
    
    @validator('entity_type')
    def validate_entity_type(cls, v):
        """Validate entity type"""
        valid_types = ['otc_desk', 'market_maker', 'cex', 'prop_trading', 'whale', 'unknown', `Cold_Wallet´, `Hot_wallet`]
        if v not in valid_types:
            raise ValueError(f"Invalid entity_type. Must be one of: {', '.join(valid_types)}")
        return v
    
    @validator('tags', pre=True, always=True)
    def validate_tags(cls, v):
        """Ensure tags is always a list"""
        if v is None:
            return []
        if isinstance(v, list):
            return v
        return []


class OTCAddressUpdate(BaseModel):
    """Request model for updating OTC address"""
    entity_type: Optional[str] = None
    entity_name: Optional[str] = None
    label: Optional[str] = None
    notes: Optional[str] = None
    tags: Optional[List[str]] = None
    is_active: Optional[bool] = None
    
    @validator('entity_type')
    def validate_entity_type(cls, v):
        """Validate entity type"""
        valid_types = ['otc_desk', 'market_maker', 'cex', 'prop_trading', 'whale', 'cold_wallet', 'hot_wallet', 'unknown']
        if v not in valid_types:
            raise ValueError(f"Invalid entity_type. Must be one of: {', '.join(valid_types)}")
        return v
    
    @validator('tags', pre=True)
    def validate_tags(cls, v):
        """Ensure tags is always a list if provided"""
        if v is None:
            return None
        if isinstance(v, list):
            return v
        return []


class OTCAddressResponse(BaseModel):
    """Response model for OTC address"""
    address: str
    entity_type: str
    entity_name: Optional[str] = None
    label: Optional[str] = None
    notes: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    is_active: bool
    confidence_score: float
    total_volume: float
    transaction_count: int
    created_at: datetime
    updated_at: datetime
    
    @validator('tags', pre=True, always=True)
    def validate_tags(cls, v):
        """Convert None to empty list"""
        if v is None:
            return []
        if isinstance(v, list):
            return v
        return []
    
    @validator('entity_name', 'label', 'notes', pre=True)
    def validate_optional_strings(cls, v):
        """Handle None values for optional string fields"""
        return v if v is not None else None
    
    class Config:
        from_attributes = True  # ✅ Pydantic v2
        orm_mode = True  # ✅ Pydantic v1 compatibility


# ============================================================================
# ADMIN AUTHENTICATION (PLACEHOLDER)
# ============================================================================

async def verify_admin(
    # TODO: Implement your auth logic here
    # Example: api_key: str = Header(...), user = Depends(get_current_user)
):
    """
    🔒 Admin authentication dependency.
    
    TODO: Ersetze dies mit deiner echten Auth-Logik:
    - API Key Check
    - JWT Token Verification
    - Role-based Access Control
    
    Beispiel:
        from fastapi import Header
        
        async def verify_admin(api_key: str = Header(...)):
            if api_key != os.getenv("ADMIN_API_KEY"):
                raise HTTPException(401, "Unauthorized")
            return True
    """
    # Placeholder - always allows access
    # ⚠️ IN PRODUCTION: Replace with real authentication!
    return True


# ============================================================================
# CRUD ENDPOINTS
# ============================================================================

@router.post(
    "/",
    response_model=OTCAddressResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add OTC Address",
    description="Add a single OTC address to the database (Admin only)"
)
async def create_otc_address(
    data: OTCAddressCreate,
    db: Session = Depends(get_db),
    _admin: bool = Depends(verify_admin)
):
    """
    ➕ Add new OTC address
    
    **Features:**
    - ✅ Automatic duplicate check
    - ✅ Address validation
    - ✅ Auto-generate label if not provided
    
    **Example:**
```json
    {
        "address": "0x1151314c646Ce4E0eFD76d1aF4760aE66a9Fe30F",
        "entity_type": "otc_desk",
        "entity_name": "Wintermute",
        "notes": "Main trading wallet",
        "tags": ["verified", "high_volume"]
    }
```
    """
    try:
        logger.info(f"➕ Admin: Adding OTC address {data.address[:10]}...")
        
        # ✅ Check for duplicates
        existing = db.query(OTCWallet).filter(
            OTCWallet.address == data.address
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Address {data.address} already exists in database"
            )
        
        # ✅ Ensure tags is a list, never None
        tags = data.tags if data.tags else []
        
        # Create wallet entry
        wallet = OTCWallet(
            address=data.address,
            entity_type=data.entity_type,
            entity_name=data.entity_name,
            label=data.label or data.entity_name or f"{data.address[:8]}...",
            notes=data.notes,
            tags=tags,  # ✅ Always a list
            is_active=data.is_active,
            # Initial values
            confidence_score=100.0,  # Manual entry = high confidence
            total_volume=0.0,
            transaction_count=0,
            first_seen=datetime.utcnow(),
            last_active=datetime.utcnow(),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        db.add(wallet)
        db.commit()
        db.refresh(wallet)
        
        # ✅ Ensure tags is not None after refresh
        if wallet.tags is None:
            wallet.tags = []
        
        logger.info(f"✅ Added {data.address[:10]}... ({data.entity_type})")
        
        return wallet
        
    except HTTPException:
        raise
    except IntegrityError as e:
        db.rollback()
        logger.error(f"❌ Database integrity error: {e}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Address already exists or database constraint violated"
        )
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error creating OTC address: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create address: {str(e)}"
        )


@router.get(
    "/",
    response_model=List[OTCAddressResponse],
    summary="List OTC Addresses",
    description="Get all OTC addresses with optional filtering"
)
async def list_otc_addresses(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    search: Optional[str] = Query(None, description="Search in name/label/address"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    offset: int = Query(0, ge=0, description="Skip N results"),
    db: Session = Depends(get_db),
    _admin: bool = Depends(verify_admin)
):
    """
    📋 List all OTC addresses
    
    **Filters:**
    - `entity_type`: Filter by type (otc_desk, market_maker, etc.)
    - `is_active`: Show only active/inactive
    - `search`: Search in name, label, or address
    - `limit`: Pagination limit (default: 100)
    - `offset`: Pagination offset (default: 0)
    """
    try:
        logger.info(f"📋 Admin: Listing OTC addresses (limit={limit}, offset={offset})")
        
        # Build query
        query = db.query(OTCWallet)
        
        # Apply filters
        if entity_type:
            query = query.filter(OTCWallet.entity_type == entity_type)
        
        if is_active is not None:
            query = query.filter(OTCWallet.is_active == is_active)
        
        if search:
            search_pattern = f"%{search}%"
            query = query.filter(
                (OTCWallet.address.ilike(search_pattern)) |
                (OTCWallet.entity_name.ilike(search_pattern)) |
                (OTCWallet.label.ilike(search_pattern))
            )
        
        # Get total count
        total = query.count()
        
        # Apply pagination and get results
        wallets = query.order_by(
            OTCWallet.updated_at.desc()
        ).offset(offset).limit(limit).all()
        
        # ✅ Ensure tags is not None for all wallets
        for wallet in wallets:
            if wallet.tags is None:
                wallet.tags = []
        
        logger.info(f"✅ Found {len(wallets)} addresses (total: {total})")
        
        return wallets
        
    except Exception as e:
        logger.error(f"❌ Error listing OTC addresses: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list addresses: {str(e)}"
        )


@router.get(
    "/{address}",
    response_model=OTCAddressResponse,
    summary="Get OTC Address",
    description="Get details of a specific OTC address"
)
async def get_otc_address(
    address: str,
    db: Session = Depends(get_db),
    _admin: bool = Depends(verify_admin)
):
    """🔍 Get single OTC address details"""
    try:
        address = validate_ethereum_address(address)
        
        wallet = db.query(OTCWallet).filter(
            OTCWallet.address == address
        ).first()
        
        if not wallet:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Address {address} not found"
            )
        
        # ✅ Ensure tags is not None
        if wallet.tags is None:
            wallet.tags = []
        
        return wallet
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting OTC address: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get address: {str(e)}"
        )


@router.put(
    "/{address}",
    response_model=OTCAddressResponse,
    summary="Update OTC Address",
    description="Update an existing OTC address"
)
async def update_otc_address(
    address: str,
    data: OTCAddressUpdate,
    db: Session = Depends(get_db),
    _admin: bool = Depends(verify_admin)
):
    """
    ✏️ Update OTC address
    
    **Example:**
```json
    {
        "entity_name": "Wintermute Trading",
        "notes": "Updated notes",
        "tags": ["verified", "high_volume", "market_maker"]
    }
```
    """
    try:
        address = validate_ethereum_address(address)
        logger.info(f"✏️ Admin: Updating {address[:10]}...")
        
        wallet = db.query(OTCWallet).filter(
            OTCWallet.address == address
        ).first()
        
        if not wallet:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Address {address} not found"
            )
        
        # Update only provided fields
        update_data = data.dict(exclude_unset=True)
        
        # ✅ Ensure tags is a list if provided
        if 'tags' in update_data:
            if update_data['tags'] is None:
                update_data['tags'] = []
        
        for field, value in update_data.items():
            setattr(wallet, field, value)
        
        wallet.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(wallet)
        
        # ✅ Ensure tags is not None after refresh
        if wallet.tags is None:
            wallet.tags = []
        
        logger.info(f"✅ Updated {address[:10]}...")
        
        return wallet
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error updating OTC address: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update address: {str(e)}"
        )


@router.delete(
    "/{address}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete OTC Address",
    description="Delete an OTC address from the database"
)
async def delete_otc_address(
    address: str,
    db: Session = Depends(get_db),
    _admin: bool = Depends(verify_admin)
):
    """
    🗑️ Delete OTC address
    
    ⚠️ **Warning:** This permanently deletes the address from the database.
    """
    try:
        address = validate_ethereum_address(address)
        logger.info(f"🗑️ Admin: Deleting {address[:10]}...")
        
        wallet = db.query(OTCWallet).filter(
            OTCWallet.address == address
        ).first()
        
        if not wallet:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Address {address} not found"
            )
        
        db.delete(wallet)
        db.commit()
        
        logger.info(f"✅ Deleted {address[:10]}...")
        
        return None
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error deleting OTC address: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete address: {str(e)}"
        )


@router.post(
    "/{address}/toggle-active",
    response_model=OTCAddressResponse,
    summary="Toggle Active Status",
    description="Toggle the is_active flag for an address"
)
async def toggle_active_status(
    address: str,
    db: Session = Depends(get_db),
    _admin: bool = Depends(verify_admin)
):
    """🔄 Toggle active/inactive status"""
    try:
        address = validate_ethereum_address(address)
        
        wallet = db.query(OTCWallet).filter(
            OTCWallet.address == address
        ).first()
        
        if not wallet:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Address {address} not found"
            )
        
        wallet.is_active = not wallet.is_active
        wallet.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(wallet)
        
        # ✅ Ensure tags is not None
        if wallet.tags is None:
            wallet.tags = []
        
        status_text = "active" if wallet.is_active else "inactive"
        logger.info(f"✅ Toggled {address[:10]}... to {status_text}")
        
        return wallet
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error toggling status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to toggle status: {str(e)}"
        )


# ============================================================================
# BULK OPERATIONS (Bonus)
# ============================================================================

@router.post(
    "/bulk/delete",
    status_code=status.HTTP_200_OK,
    summary="Bulk Delete",
    description="Delete multiple addresses at once"
)
async def bulk_delete_addresses(
    addresses: List[str],
    db: Session = Depends(get_db),
    _admin: bool = Depends(verify_admin)
):
    """
    🗑️ Bulk delete multiple addresses
    
    **Example:**
```json
    ["0x1151314c646Ce4E0eFD76d1aF4760aE66a9Fe30F", "0x..."]
```
    """
    try:
        logger.info(f"🗑️ Admin: Bulk deleting {len(addresses)} addresses...")
        
        # Validate all addresses
        validated_addresses = [validate_ethereum_address(addr) for addr in addresses]
        
        # Delete
        deleted_count = db.query(OTCWallet).filter(
            OTCWallet.address.in_(validated_addresses)
        ).delete(synchronize_session=False)
        
        db.commit()
        
        logger.info(f"✅ Bulk deleted {deleted_count} addresses")
        
        return {
            "success": True,
            "deleted_count": deleted_count,
            "requested_count": len(addresses)
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error in bulk delete: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to bulk delete: {str(e)}"
        )


# ============================================================================
# STATISTICS ENDPOINT (Bonus)
# ============================================================================

@router.get(
    "/stats",
    summary="Get Statistics",
    description="Get statistics about OTC addresses in the database"
)
async def get_address_statistics(
    db: Session = Depends(get_db),
    _admin: bool = Depends(verify_admin)
):
    """📊 Get database statistics"""
    try:
        total = db.query(OTCWallet).count()
        active = db.query(OTCWallet).filter(OTCWallet.is_active == True).count()
        
        # Count by entity type
        entity_counts = {}
        for entity_type in ['otc_desk', 'market_maker', 'cex', 'prop_trading', 'whale', 'unknown']:
            count = db.query(OTCWallet).filter(OTCWallet.entity_type == entity_type).count()
            if count > 0:
                entity_counts[entity_type] = count
        
        # Total volume
        from sqlalchemy import func
        total_volume = db.query(func.sum(OTCWallet.total_volume)).scalar() or 0.0
        
        return {
            "total_addresses": total,
            "active_addresses": active,
            "inactive_addresses": total - active,
            "by_entity_type": entity_counts,
            "total_volume_usd": total_volume
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting statistics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get statistics: {str(e)}"
        )
