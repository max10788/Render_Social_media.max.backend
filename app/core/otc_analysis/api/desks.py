"""
OTC Desk Endpoints
==================

Endpoints for OTC desk management:
- List desks (combined sources)
- Desk details
- Active discovery
- Transaction analysis
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime

from .dependencies import (
    get_db,
    get_otc_registry,
    get_labeling_service,
    get_transaction_extractor,
    get_price_oracle,
    get_wallet_profiler,
    get_otc_detector,
    node_provider,
    ensure_registry_wallets_in_db
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/desks", tags=["OTC Desks"])


# ============================================================================
# DESK ENDPOINTS
# ============================================================================

@router.get("")
async def get_otc_desks(
    include_discovered: bool = Query(True, description="Include auto-discovered desks"),
    include_db_validated: bool = Query(True, description="Include validated desks from database"),
    min_confidence: float = Query(0.7, ge=0.0, le=1.0, description="Minimum confidence score (0-1)"),
    source: Optional[str] = Query(None, description="Filter by source: registry, database, or all"),
    tags: Optional[str] = Query(None, description="Filter by tags (comma-separated, e.g. 'verified,otc_desk')"),  # ✅ NEW!
    db: Session = Depends(get_db),
    registry = Depends(get_otc_registry)
):
    """
    🎯 Get list of ALL OTC desks from MULTIPLE sources.
    
    GET /api/otc/desks?include_discovered=true&include_db_validated=true&min_confidence=0.7
    GET /api/otc/desks?tags=verified  ✅ NEW: Filter by tags!
    
    **Sources:**
    - **Registry**: Verified + Discovered desks (via Moralis)
    - **Database**: Validated wallets (high confidence from profiling)
    
    **Query Parameters:**
    - `include_discovered`: Include auto-discovered desks (default: true)
    - `include_db_validated`: Include DB validated desks (default: true)
    - `min_confidence`: Minimum confidence 0-1 (default: 0.7)
    - `source`: Filter by 'registry', 'database', or null for all
    - `tags`: Filter DB wallets by tags, comma-separated (e.g. 'verified,otc_desk')  ✅ NEW!
    
    **Tag Examples:**
    - `tags=verified` → Only wallets with 'verified' tag
    - `tags=verified,otc_desk` → Wallets with 'verified' OR 'otc_desk' tag
    - `tags=discovered,active` → Wallets with 'discovered' OR 'active' tag
    
    **Returns:**
    Combined list with source tracking!
    """
    logger.info(f"🏢 GET /desks: discovered={include_discovered}, db={include_db_validated}, min_conf={min_confidence}, tags={tags}")
    
    try:
        # ✅ AUTO-SYNC: Ensure registry wallets in DB
        await ensure_registry_wallets_in_db(db, max_to_fetch=3)
        
        # ✅ Parse tags parameter
        required_tags = None
        if tags:
            required_tags = [tag.strip() for tag in tags.split(',') if tag.strip()]
            logger.info(f"   🏷️  Tag filter: {required_tags}")
        
        # ✅ Get combined desk list (Registry + Database) with tag filter
        desks = registry.get_combined_desk_list(
            include_discovered=include_discovered,
            include_db_validated=include_db_validated,
            min_confidence=min_confidence,
            db_session=db,
            required_tags=required_tags  # ✅ NEW!
        )
        
        # ✅ Filter by source if requested
        if source:
            source_lower = source.lower()
            if source_lower == 'registry':
                desks = [d for d in desks if d.get('data_source') == 'registry']
                logger.info(f"   📋 Filtered to registry desks only")
            elif source_lower == 'database':
                desks = [d for d in desks if d.get('data_source') == 'database']
                logger.info(f"   💾 Filtered to database desks only")
        
        # ✅ Count by source and category
        registry_count = sum(1 for d in desks if d.get('data_source') == 'registry')
        database_count = sum(1 for d in desks if d.get('data_source') == 'database')
        
        verified_count = sum(1 for d in desks if d.get('desk_category') == 'verified')
        discovered_count = sum(1 for d in desks if d.get('desk_category') == 'discovered')
        db_validated_count = sum(1 for d in desks if d.get('desk_category') == 'db_validated')
        
        logger.info(f"✅ Loaded {len(desks)} OTC desks:")
        logger.info(f"   • Registry: {registry_count} (Verified: {verified_count}, Discovered: {discovered_count})")
        logger.info(f"   • Database: {database_count} (Validated: {db_validated_count})")
        
        return {
            "success": True,
            "data": {
                "desks": desks,
                "total_count": len(desks),
                "sources": {
                    "registry": registry_count,
                    "database": database_count
                },
                "categories": {
                    "verified": verified_count,
                    "discovered": discovered_count,
                    "db_validated": db_validated_count
                },
                "filters_applied": {
                    "include_discovered": include_discovered,
                    "include_db_validated": include_db_validated,
                    "min_confidence": min_confidence,
                    "source_filter": source,
                    "tag_filter": required_tags  # ✅ NEW!
                }
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch desks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ✅ NEW: DATABASE-ONLY ENDPOINT
# ============================================================================

@router.get("/database")
async def get_database_wallets(
    tags: Optional[str] = Query(None, description="Filter by tags (comma-separated)"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0, description="Minimum confidence (0-1)"),
    is_active: bool = Query(True, description="Only active wallets"),
    limit: int = Query(100, ge=1, le=500, description="Max results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db)
):
    """
    🎯 Get wallets ONLY from database (skip registry).
    
    GET /api/otc/desks/database?tags=verified,otc_desk&min_confidence=0.7
    """
    logger.info(f"💾 GET /desks/database: tags={tags}, min_conf={min_confidence}")
    
    try:
        from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
        from sqlalchemy import or_
        
        # Build base query
        query = db.query(OTCWallet).filter(
            OTCWallet.confidence_score >= min_confidence * 100,
            OTCWallet.is_active == is_active
        )
        
        # Filter by tags (works with both JSON and JSONB columns)
        if tags:
            required_tags = [tag.strip() for tag in tags.split(',') if tag.strip()]

            from sqlalchemy import cast, type_coerce, Text
            from sqlalchemy.dialects.postgresql import JSONB

            tag_filters = []
            for tag in required_tags:
                try:
                    tag_filters.append(
                        type_coerce(OTCWallet.tags, JSONB).contains([tag])
                    )
                except Exception:
                    tag_filters.append(
                        cast(OTCWallet.tags, Text).like(f'%"{tag}"%')
                    )

            if tag_filters:
                query = query.filter(or_(*tag_filters))
                logger.info(f"   Tag filter (OR logic): {required_tags}")
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        wallets = query.order_by(
            OTCWallet.created_at.desc()
        ).offset(offset).limit(limit).all()
        
        # Format response
        result = []
        for wallet in wallets:
            result.append({
                'address': wallet.address,
                'label': wallet.label,
                'entity_name': wallet.entity_name,
                'entity_type': wallet.entity_type,
                'confidence': wallet.confidence_score / 100,
                'is_active': wallet.is_active,
                'tags': wallet.tags or [],
                'total_volume': wallet.total_volume,
                'transaction_count': wallet.transaction_count,
                'first_seen': wallet.first_seen.isoformat() if wallet.first_seen else None,
                'last_active': wallet.last_active.isoformat() if wallet.last_active else None
            })
        
        # Calculate tag distribution
        tags_count = {}
        for wallet in wallets:
            for tag in (wallet.tags or []):
                tags_count[tag] = tags_count.get(tag, 0) + 1
        
        logger.info(f"✅ Found {len(result)} wallets (total: {total_count})")
        
        return {
            "success": True,
            "data": {
                "wallets": result,
                "count": len(result),
                "total_count": total_count,
                "tags_distribution": tags_count,
                "pagination": {
                    "offset": offset,
                    "limit": limit,
                    "has_more": (offset + len(result)) < total_count
                },
                "filters_applied": {
                    "tags": required_tags if tags else None,
                    "min_confidence": min_confidence,
                    "is_active": is_active
                }
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch database wallets: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/discover")
async def discover_otc_desks(
    hours_back: int = Query(1, ge=1, le=168, description="Hours to look back (1-168)"),
    volume_threshold: float = Query(100000, ge=1000, description="Min transaction value ($)"),
    max_new_desks: int = Query(20, ge=1, le=50, description="Max desks to discover"),
    db: Session = Depends(get_db),
    registry = Depends(get_otc_registry)
):
    """
    🚀 Discover OTC desks active in a specific time period.
    
    POST /api/otc/desks/discover?hours_back=1&volume_threshold=100000&max_new_desks=20
    
    **Time Period Options:**
    - `hours_back=1` → Last hour (real-time discovery)
    - `hours_back=6` → Last 6 hours
    - `hours_back=24` → Last day
    - `hours_back=168` → Last week
    
    **How It Works:**
    1. Scans verified desks for large transactions in time period
    2. Extracts counterparty addresses with high volume
    3. Validates via Moralis entity labels
    4. Returns newly discovered OTC desks
    
    **Query Parameters:**
    - `hours_back`: Hours to look back (1-168, default: 1)
    - `volume_threshold`: Min transaction value in USD (default: 100k)
    - `max_new_desks`: Max desks to discover (1-50, default: 20)
    """
    logger.info(f"🚀 POST /desks/discover: hours_back={hours_back}, threshold=${volume_threshold/1000:.0f}k")
    
    try:
        from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
        
        # Run time-based discovery
        result = registry.discover_desks_by_time_period(
            hours_back=hours_back,
            volume_threshold=volume_threshold,
            max_new_desks=max_new_desks
        )
        
        logger.info(f"✅ Discovery complete: {result['count']} new desks found")
        
        # ✅ Auto-save discovered desks to database if high confidence
        saved_count = 0
        if result['discovered_desks']:
            for desk in result['discovered_desks']:
                if desk.get('confidence', 0) >= 0.8:  # 80% threshold
                    try:
                        # Check if already exists
                        existing = db.query(OTCWallet).filter(
                            OTCWallet.address == desk['address']
                        ).first()
                        
                        if not existing:
                            wallet = OTCWallet(
                                address=desk['address'],
                                label=desk['name'],
                                entity_type='discovered',
                                entity_name=desk['name'],
                                confidence_score=desk['confidence'] * 100,
                                total_volume=desk.get('discovery_volume', 0),
                                transaction_count=desk.get('discovery_tx_count', 0),
                                first_seen=datetime.now(),
                                last_active=datetime.now(),
                                is_active=True,
                                tags=['discovered', 'active_discovery'],
                                created_at=datetime.now(),
                                updated_at=datetime.now()
                            )
                            db.add(wallet)
                            saved_count += 1
                    except Exception as e:
                        logger.error(f"❌ Error saving discovered desk: {e}")
                        continue
            
            if saved_count > 0:
                db.commit()
                logger.info(f"💾 Auto-saved {saved_count} high-confidence desks to DB")
        
        return {
            "success": True,
            "data": result,
            "metadata": {
                "saved_to_db": saved_count
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Discovery failed: {str(e)}", exc_info=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{desk_name}")
async def get_desk_details(
    desk_name: str,
    db: Session = Depends(get_db),
    registry = Depends(get_otc_registry)
):
    """
    Get detailed information about a specific OTC desk.
    
    GET /api/otc/desks/wintermute
    GET /api/otc/desks/jump_trading
    
    Searches BOTH registry and database!
    """
    logger.info(f"🏢 GET /desks/{desk_name}")
    
    try:
        from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
        
        # Try registry first
        desk_info = registry.get_desk_by_name(desk_name)
        
        if desk_info:
            desk_info['data_source'] = 'registry'
            logger.info(f"✅ Found in registry: {desk_info['name']}")
            return {
                "success": True,
                "data": desk_info
            }
        
        # Try database
        wallet = db.query(OTCWallet).filter(
            (OTCWallet.label.ilike(f"%{desk_name}%")) |
            (OTCWallet.entity_name.ilike(f"%{desk_name}%")) |
            (OTCWallet.address == desk_name)
        ).first()
        
        if wallet:
            logger.info(f"✅ Found in database: {wallet.label}")
            return {
                "success": True,
                "data": {
                    "name": wallet.label,
                    "display_name": wallet.entity_name or wallet.label,
                    "type": wallet.entity_type,
                    "desk_category": "db_validated",
                    "addresses": [wallet.address],
                    "confidence": wallet.confidence_score / 100,
                    "total_volume": wallet.total_volume,
                    "transaction_count": wallet.transaction_count,
                    "active": wallet.is_active,
                    "tags": wallet.tags or [],  # ✅ Include tags
                    "data_source": "database",
                    "last_updated": wallet.updated_at.isoformat() if wallet.updated_at else None,
                    "last_activity": wallet.last_active.isoformat() if wallet.last_active else None
                }
            }
        
        # Not found
        logger.warning(f"⚠️  Desk not found: {desk_name}")
        raise HTTPException(status_code=404, detail=f"OTC desk '{desk_name}' not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to fetch desk details: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# TRANSACTION ANALYSIS
# ============================================================================

@router.post("/analyze/transaction")
async def analyze_transaction(
    tx_hash: str = Query(..., description="Transaction hash to analyze"),
    labeling = Depends(get_labeling_service),
    tx_extractor = Depends(get_transaction_extractor),
    oracle = Depends(get_price_oracle),
    profiler = Depends(get_wallet_profiler),
    detector = Depends(get_otc_detector)
):
    """
    Analyze a specific transaction for OTC activity.
    
    POST /api/otc/desks/analyze/transaction?tx_hash=0x...
    """
    logger.info(f"🔍 Analyzing transaction: {tx_hash[:16]}...")
    
    try:
        logger.info(f"📡 Fetching transaction from blockchain...")
        tx_data = node_provider.get_transaction(tx_hash)
        
        if not tx_data:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        receipt = node_provider.get_transaction_receipt(tx_hash)
        
        from_address = tx_data['from']
        to_address = tx_data.get('to')
        
        if not to_address:
            logger.info(f"⚠️  Contract deployment, skipping OTC analysis")
            return {
                "success": True,
                "data": {
                    "is_suspected_otc": False,
                    "reason": "contract_deployment"
                }
            }
        
        transaction = {
            'tx_hash': tx_hash,
            'from_address': from_address,
            'to_address': to_address,
            'value': str(tx_data['value']),
            'value_decimal': node_provider.from_wei(tx_data['value']),
            'block_number': tx_data['blockNumber'],
            'timestamp': datetime.now(),
            'gas_used': receipt.get('gasUsed'),
            'is_contract_interaction': node_provider.is_contract(to_address)
        }
        
        logger.info(f"💰 Fetching ETH price...")
        eth_price = oracle.get_current_price(None)
        if eth_price:
            transaction['usd_value'] = transaction['value_decimal'] * eth_price
            logger.info(f"💵 Transaction value: ${transaction['usd_value']:,.2f}")
        
        logger.info(f"👤 Building wallet profile...")
        wallet_txs = tx_extractor.extract_wallet_transactions(from_address)
        wallet_profile = profiler.create_profile(from_address, wallet_txs)
        
        logger.info(f"🎯 Running OTC detection...")
        result = detector.detect_otc_transaction(
            transaction,
            wallet_profile,
            wallet_txs[:100]
        )
        
        logger.info(f"✅ Analysis complete - Confidence: {result['confidence_score']:.1f}")
        
        return {
            "success": True,
            "data": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Transaction analysis failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
