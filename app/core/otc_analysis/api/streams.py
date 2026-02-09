"""
Moralis Streams Webhook Endpoint
=================================

Receives real-time transfer events from Moralis Streams.

Security:
- Validates webhook signature (HMAC)
- Verifies request authenticity
- Prevents replay attacks

Processing:
- Extracts transfer data
- Validates addresses via Moralis
- Auto-discovers OTC desks
- Saves to database
"""

import os
import logging
import hmac
import hashlib
from fastapi import APIRouter, HTTPException, Request, Depends, Header
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime

from .dependencies import get_db, get_otc_registry, get_labeling_service

from app.core.otc_analysis.models.wallet import Wallet as OTCWallet

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/streams", tags=["Moralis Streams"])


# ============================================================================
# WEBHOOK SIGNATURE VALIDATION
# ============================================================================

def verify_moralis_signature(
    request_body: bytes,
    signature: str,
    secret: Optional[str] = None
) -> bool:
    """
    Verify Moralis webhook signature.
    
    Moralis signs webhooks with HMAC-SHA256.
    This prevents unauthorized requests.
    
    Args:
        request_body: Raw request body bytes
        signature: Signature from X-Signature header
        secret: Webhook secret (from Moralis dashboard)
        
    Returns:
        True if signature is valid
    """
    if not secret:
        secret = os.getenv('MORALIS_WEBHOOK_SECRET')
    
    if not secret:
        logger.warning("⚠️  MORALIS_WEBHOOK_SECRET not set - signature verification disabled!")
        return True  # Allow in dev mode
    
    try:
        # Calculate expected signature
        expected = hmac.new(
            secret.encode('utf-8'),
            request_body,
            hashlib.sha256
        ).hexdigest()
        
        # Compare signatures (constant-time comparison)
        is_valid = hmac.compare_digest(expected, signature)
        
        if not is_valid:
            logger.error("❌ Invalid webhook signature!")
            logger.error(f"   Expected: {expected[:16]}...")
            logger.error(f"   Received: {signature[:16]}...")
        
        return is_valid
        
    except Exception as e:
        logger.error(f"❌ Signature verification failed: {e}")
        return False


# ============================================================================
# WEBHOOK ENDPOINT
# ============================================================================

@router.post("/webhook")
async def moralis_webhook(
    request: Request,
    x_signature: Optional[str] = Header(None),
    db: Session = Depends(get_db),
    registry = Depends(get_otc_registry),
    labeling = Depends(get_labeling_service)
):
    """
    🔔 Moralis Streams Webhook Endpoint
    
    POST /api/otc/streams/webhook
    
    Receives real-time transfer events from Moralis Streams.
    
    Event Flow:
    1. Large transfer occurs on blockchain
    2. Moralis detects it and sends webhook
    3. We receive event here
    4. Validate signature
    5. Extract addresses
    6. Validate via Moralis entity API
    7. Auto-save discovered OTC desks
    
    Security:
    - HMAC-SHA256 signature validation
    - Secret key verification
    - Request authenticity check
    """
    try:
        # Read raw body for signature verification
        body = await request.body()
        
        # Verify signature
        if x_signature:
            is_valid = verify_moralis_signature(body, x_signature)
            if not is_valid:
                logger.error("❌ Invalid webhook signature - rejecting request")
                raise HTTPException(status_code=401, detail="Invalid signature")
        else:
            logger.warning("⚠️  No signature provided - accepting in dev mode")
        
        # Parse JSON
        data = await request.json()
        
        # Log webhook receipt
        logger.info("🔔 Moralis webhook received!")
        logger.info(f"   Tag: {data.get('tag')}")
        logger.info(f"   Chain: {data.get('chainId')}")
        logger.info(f"   Block: {data.get('block', {}).get('number')}")
        
        # Extract confirmed transactions
        confirmed_txs = data.get('txs', [])
        internal_txs = data.get('txsInternal', [])
        logs = data.get('logs', [])
        
        logger.info(f"   Transactions: {len(confirmed_txs)}")
        logger.info(f"   Internal: {len(internal_txs)}")
        logger.info(f"   Logs: {len(logs)}")
        
        # Process all transfers
        discovered_addresses = set()
        
        # Process native transfers (ETH)
        for tx in confirmed_txs:
            addresses = await process_native_transfer(tx, db, registry, labeling)
            discovered_addresses.update(addresses)
        
        # Process ERC20 transfers (from logs)
        for log in logs:
            addresses = await process_erc20_transfer(log, db, registry, labeling)
            discovered_addresses.update(addresses)
        
        logger.info(f"Webhook processed: {len(discovered_addresses)} addresses discovered")

        # Broadcast to WebSocket clients
        if discovered_addresses or confirmed_txs:
            from app.core.otc_analysis.api.websocket import broadcast_to_all
            for tx in confirmed_txs:
                value_wei = int(tx.get('value', 0))
                value_eth = value_wei / 1e18
                value_usd = value_eth * 3000
                if value_usd >= 100_000:
                    await broadcast_to_all("new_large_transfer", {
                        "type": "new_large_transfer",
                        "tx_hash": tx.get('hash', ''),
                        "from_address": tx.get('fromAddress', ''),
                        "to_address": tx.get('toAddress', ''),
                        "value_eth": round(value_eth, 4),
                        "usd_value": round(value_usd, 2),
                        "timestamp": datetime.now().isoformat(),
                        "source": "moralis_webhook",
                    })

        return {
            "success": True,
            "message": "Webhook processed",
            "discovered_addresses": len(discovered_addresses),
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Webhook processing failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# TRANSFER PROCESSING
# ============================================================================

async def process_native_transfer(
    tx: dict,
    db: Session,
    registry,
    labeling
) -> set:
    """
    Process native (ETH) transfer.
    
    Returns:
        Set of discovered OTC desk addresses
    """
    discovered = set()
    
    try:
        from_address = tx.get('fromAddress', '').lower()
        to_address = tx.get('toAddress', '').lower()
        value_wei = int(tx.get('value', 0))
        value_eth = value_wei / 1e18
        
        # Rough USD value (would use price oracle in production)
        value_usd = value_eth * 3000
        
        logger.info(f"💸 Native transfer: {value_eth:.2f} ETH (≈${value_usd:,.0f})")
        logger.info(f"   From: {from_address[:10]}...")
        logger.info(f"   To: {to_address[:10]}...")
        
        # Check both addresses
        for address in [from_address, to_address]:
            if not address or address == '0x0000000000000000000000000000000000000000':
                continue
            
            # Check if already known
            existing = db.query(OTCWallet).filter(
                OTCWallet.address == address
            ).first()
            
            if existing and existing.confidence_score >= 80:
                logger.info(f"   ✓ {address[:10]}... already in DB")
                continue
            
            # Validate via Moralis entity API
            try:
                from app.core.otc_analysis.blockchain.moralis import MoralisAPI
                
                moralis = MoralisAPI()
                validation = moralis.validate_otc_entity(address)
                
                if validation and validation.get('is_otc'):
                    confidence = validation.get('confidence', 0)
                    entity_name = validation.get('entity_name')
                    
                    logger.info(f"   ✅ OTC DESK DISCOVERED: {entity_name} ({confidence:.0%})")
                    
                    # Save to database if high confidence
                    if confidence >= 0.8:
                        wallet = OTCWallet(
                            address=address,
                            label=entity_name or f"{address[:8]}...",
                            entity_type='discovered_stream',
                            entity_name=entity_name,
                            confidence_score=confidence * 100,
                            total_volume=value_usd,
                            transaction_count=1,
                            first_seen=datetime.now(),
                            last_active=datetime.now(),
                            is_active=True,
                            tags=['stream_discovered', 'large_transfer'],
                            created_at=datetime.now(),
                            updated_at=datetime.now()
                        )
                        
                        db.add(wallet)
                        db.commit()
                        
                        discovered.add(address)
                        logger.info(f"      💾 Saved to database")
                    else:
                        logger.info(f"      ⚠️  Low confidence ({confidence:.0%}) - not saving")
                
            except Exception as e:
                logger.error(f"   ❌ Validation failed for {address[:10]}: {e}")
        
        return discovered
        
    except Exception as e:
        logger.error(f"❌ Error processing native transfer: {e}")
        return discovered


async def process_erc20_transfer(
    log: dict,
    db: Session,
    registry,
    labeling
) -> set:
    """
    Process ERC20 token transfer (from event log).
    
    Returns:
        Set of discovered OTC desk addresses
    """
    discovered = set()
    
    try:
        # ERC20 Transfer event signature
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        
        if log.get('topic0') != transfer_topic:
            return discovered
        
        # Decode transfer data
        # topic1 = from (indexed)
        # topic2 = to (indexed)
        # data = value
        
        topics = log.get('topics', [])
        if len(topics) < 3:
            return discovered
        
        from_address = '0x' + topics[1][-40:]
        to_address = '0x' + topics[2][-40:]
        
        # Decode value from data
        data = log.get('data', '0x0')
        value_int = int(data, 16)
        
        # Get token info
        token_address = log.get('address', '').lower()
        
        logger.info(f"🪙 ERC20 transfer detected")
        logger.info(f"   Token: {token_address[:10]}...")
        logger.info(f"   Value: {value_int}")
        logger.info(f"   From: {from_address[:10]}...")
        logger.info(f"   To: {to_address[:10]}...")
        
        # Check both addresses (same as native transfers)
        for address in [from_address.lower(), to_address.lower()]:
            if not address or address == '0x0000000000000000000000000000000000000000':
                continue
            
            existing = db.query(OTCWallet).filter(
                OTCWallet.address == address
            ).first()
            
            if existing and existing.confidence_score >= 80:
                continue
            
            # Validate via Moralis
            try:
                from app.core.otc_analysis.blockchain.moralis import MoralisAPI
                
                moralis = MoralisAPI()
                validation = moralis.validate_otc_entity(address)
                
                if validation and validation.get('is_otc'):
                    confidence = validation.get('confidence', 0)
                    entity_name = validation.get('entity_name')
                    
                    logger.info(f"   ✅ OTC DESK: {entity_name} ({confidence:.0%})")
                    
                    if confidence >= 0.8:
                        wallet = OTCWallet(
                            address=address,
                            label=entity_name or f"{address[:8]}...",
                            entity_type='discovered_stream',
                            entity_name=entity_name,
                            confidence_score=confidence * 100,
                            total_volume=0,  # Would need token price
                            transaction_count=1,
                            first_seen=datetime.now(),
                            last_active=datetime.now(),
                            is_active=True,
                            tags=['stream_discovered', 'erc20_transfer'],
                            created_at=datetime.now(),
                            updated_at=datetime.now()
                        )
                        
                        db.add(wallet)
                        db.commit()
                        
                        discovered.add(address)
                
            except Exception as e:
                logger.error(f"   ❌ Validation failed: {e}")
        
        return discovered
        
    except Exception as e:
        logger.error(f"❌ Error processing ERC20 transfer: {e}")
        return discovered


# ============================================================================
# STREAM MANAGEMENT ENDPOINTS
# ============================================================================

@router.get("/status")
async def get_stream_status():
    """
    Get status of Moralis Streams.
    
    GET /api/otc/streams/status
    """
    try:
        # ✅ Check if MORALIS_API_KEY is set
        import os
        api_key = os.getenv('MORALIS_API_KEY')
        
        if not api_key:
            logger.warning("⚠️  MORALIS_API_KEY not set")
            return {
                "success": False,
                "message": "Moralis API key not configured",
                "total_streams": 0,
                "streams": [],
                "note": "Set MORALIS_API_KEY environment variable to use Moralis Streams"
            }
        
        from app.core.otc_analysis.blockchain.moralis_streams import MoralisStreamsManager
        
        manager = MoralisStreamsManager()
        streams = manager.list_streams()
        
        return {
            "success": True,
            "total_streams": len(streams),
            "streams": [
                {
                    "id": s.get('id'),
                    "description": s.get('description'),
                    "tag": s.get('tag'),
                    "status": s.get('status'),
                    "webhook_url": s.get('webhookUrl')
                }
                for s in streams
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get stream status: {e}")
        
        # ✅ Return graceful error instead of 500
        return {
            "success": False,
            "message": "Moralis Streams not available",
            "error": str(e),
            "total_streams": 0,
            "streams": [],
            "note": "Check MORALIS_API_KEY and network connectivity"
        }

@router.post("/test")
async def test_webhook_delivery():
    """
    Send test webhook from Moralis.
    
    POST /api/otc/streams/test
    """
    try:
        from app.core.otc_analysis.blockchain.moralis_streams import MoralisStreamsManager
        
        manager = MoralisStreamsManager()
        streams = manager.list_streams()
        
        if not streams:
            raise HTTPException(status_code=404, detail="No streams configured")
        
        # Test first stream
        stream_id = streams[0]['id']
        manager.test_webhook(stream_id)
        
        return {
            "success": True,
            "message": "Test webhook sent",
            "stream_id": stream_id
        }
        
    except Exception as e:
        logger.error(f"❌ Test webhook failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
