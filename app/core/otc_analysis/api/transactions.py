"""
Transaction Endpoints
=====================

On-demand transaction loading between wallets.

This module handles:
- Get transactions between two specific addresses
- Efficient filtering without slow bulk processing
- Used by SankeyFlow for TX hash details on click

Version: 1.0
Date: 2025-01-07
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List, Dict
from datetime import datetime, timedelta

from .dependencies import get_transaction_extractor
from app.core.otc_analysis.api.validators import validate_ethereum_address

logger = logging.getLogger(__name__)

# Transaction router - no prefix, URLs added via /api/otc in main.py
transaction_router = APIRouter(prefix="", tags=["Transactions"])


# ============================================================================
# TRANSACTION ENDPOINTS
# ============================================================================

@transaction_router.get("/transactions")
async def get_transactions_between_wallets(
    from_address: str = Query(..., description="Source wallet address"),
    to_address: str = Query(..., description="Target wallet address"),
    limit: int = Query(5, ge=1, le=20, description="Max transactions to return"),
    start_date: Optional[str] = Query(None, description="Filter TXs after this date"),
    end_date: Optional[str] = Query(None, description="Filter TXs before this date"),
    tx_extractor = Depends(get_transaction_extractor)
):
    """
    Get transactions between two specific wallet addresses.
    
    GET /api/otc/transactions?from_address=0xabc...&to_address=0xdef...&limit=5
    
    This endpoint is optimized for on-demand loading:
    - Called when user clicks on a Sankey Flow link
    - Fetches only TXs between the two specified addresses
    - Much faster than bulk analysis
    
    Parameters:
        from_address: Source wallet Ethereum address (required)
        to_address: Target wallet Ethereum address (required)
        limit: Maximum number of transactions to return (1-20, default: 5)
        start_date: Optional ISO date to filter transactions after
        end_date: Optional ISO date to filter transactions before
    
    Returns:
        {
            "transactions": [
                {
                    "hash": "0xabc123...",
                    "from": "0xabc...",
                    "to": "0xdef...",
                    "value": 1.5,
                    "value_usd": 3000.0,
                    "timestamp": "2024-12-25T10:30:00Z",
                    "token": "ETH"
                }
            ],
            "metadata": {
                "total_found": 12,
                "returned": 5,
                "from_address": "0xabc...",
                "to_address": "0xdef...",
                "date_range": {...}
            }
        }
    
    Used by:
        - SankeyFlow component for displaying TX hashes on link click
        - NetworkGraph for edge detail panels
    """
    try:
        # Validate addresses
        from_addr = validate_ethereum_address(from_address)
        to_addr = validate_ethereum_address(to_address)
        
        logger.info(f"📡 GET /transactions: {from_addr[:10]}... → {to_addr[:10]}... (limit={limit})")
        
        # Parse dates if provided
        start_datetime = None
        end_datetime = None
        
        if start_date:
            try:
                start_datetime = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid start_date format: {start_date}")
        
        if end_date:
            try:
                end_datetime = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid end_date format: {end_date}")
        
        # ====================================================================
        # STEP 1: Get transactions from source address
        # ====================================================================
        
        logger.info(f"   Fetching transactions from source: {from_addr[:10]}...")
        
        try:
            source_txs = tx_extractor.extract_wallet_transactions(
                from_addr,
                include_internal=True,
                include_tokens=True
            )
            logger.info(f"   ✅ Found {len(source_txs)} total transactions from source")
        except Exception as fetch_error:
            logger.error(f"   ❌ Failed to fetch transactions: {fetch_error}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to fetch transactions: {str(fetch_error)}"
            )
        
        # ====================================================================
        # STEP 2: Filter for transactions TO the target address
        # ====================================================================
        
        matching_txs = []
        
        for tx in source_txs:
            # Check if this TX is to our target address
            tx_to = tx.get('to', '').lower()
            
            if tx_to != to_addr.lower():
                continue
            
            # Parse timestamp
            tx_time = tx.get('timestamp')
            if isinstance(tx_time, str):
                try:
                    tx_time = datetime.fromisoformat(tx_time.replace('Z', '+00:00'))
                except ValueError:
                    tx_time = None
            elif isinstance(tx_time, int):
                tx_time = datetime.fromtimestamp(tx_time)
            
            # Apply date filters if provided
            if start_datetime and tx_time and tx_time < start_datetime:
                continue
            if end_datetime and tx_time and tx_time > end_datetime:
                continue
            
            # Get USD value
            value_usd = tx.get('value_usd', 0) or tx.get('valueUSD', 0)
            
            if not value_usd and tx.get('value'):
                try:
                    eth_value = float(tx.get('value', 0)) / 1e18
                    value_usd = eth_value * 2000  # Rough estimate
                except (ValueError, TypeError):
                    value_usd = 0
            
            # Get token symbol
            token_symbol = tx.get('tokenSymbol') or 'ETH'
            
            # Format transaction
            matching_txs.append({
                "hash": tx.get('hash') or tx.get('tx_hash') or tx.get('transactionHash'),
                "from": from_addr,
                "to": to_addr,
                "value": float(tx.get('value', 0)) / 1e18 if tx.get('value') else 0,
                "value_usd": value_usd,
                "timestamp": tx_time.isoformat() if tx_time else None,
                "token": token_symbol,
                "block_number": tx.get('blockNumber'),
                "gas_used": tx.get('gasUsed')
            })
            
            # Stop if we've reached the limit
            if len(matching_txs) >= limit * 2:  # Fetch extra for sorting
                break
        
        # ====================================================================
        # STEP 3: Sort by timestamp (newest first) and limit
        # ====================================================================
        
        # Sort by timestamp (newest first)
        matching_txs.sort(
            key=lambda x: x['timestamp'] if x['timestamp'] else '', 
            reverse=True
        )
        
        # Apply limit
        total_found = len(matching_txs)
        matching_txs = matching_txs[:limit]
        
        logger.info(f"   ✅ Found {total_found} matching transactions, returning {len(matching_txs)}")
        
        # ====================================================================
        # STEP 4: Return result
        # ====================================================================
        
        return {
            "transactions": matching_txs,
            "metadata": {
                "total_found": total_found,
                "returned": len(matching_txs),
                "from_address": from_addr,
                "to_address": to_addr,
                "limit": limit,
                "date_range": {
                    "start": start_datetime.isoformat() if start_datetime else None,
                    "end": end_datetime.isoformat() if end_datetime else None
                }
            }
        }
        
    except ValueError as e:
        # Handle validation errors
        logger.error(f"❌ Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"❌ Unexpected error in /transactions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# EXPORTS
# ============================================================================

# Export router for main.py
router = transaction_router
__all__ = ["transaction_router", "router"]
