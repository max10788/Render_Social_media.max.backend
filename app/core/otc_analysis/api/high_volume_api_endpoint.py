"""
High Volume Discovery API Endpoint
===================================

Add this to discovery_routes.py (at the end, before any closing statements)
"""

# Add this import at the top of discovery_routes.py:
# from app.core.otc_analysis.api.dependencies import discover_high_volume_from_transactions

@router.post("/discover/high-volume")
async def discover_high_volume_wallets(
    source_address: str = Query(..., description="Source wallet address to analyze"),
    num_transactions: int = Query(5, ge=1, le=20, description="Number of recent transactions to analyze"),
    min_volume_threshold: float = Query(
        1_000_000, 
        ge=100_000, 
        le=1_000_000_000,
        description="Minimum USD volume threshold (default: $1M)"
    ),
    filter_known_entities: bool = Query(
        True, 
        description="Filter out known exchanges/protocols via Moralis labels"
    ),
    db: Session = Depends(get_db)
) -> Dict:
    """
    🔍 Discover high-volume wallets from recent transactions.
    
    ✅ VOLUME-FOCUSED DISCOVERY (not OTC-specific):
    
    **Process:**
    1. Extract counterparties from last N transactions (with Moralis labels)
    2. Filter known exchanges/protocols (optional)
    3. Analyze volume patterns using Moralis ERC20 transfers
    4. Score based on:
       - Total USD volume
       - Average transaction size
       - Transaction frequency
       - Token diversity
       - Large transfer count
    5. Save wallets meeting threshold with 'high_volume_wallet' entity type
    
    **Classifications:**
    - `mega_whale`: $100M+ volume, $1M+ avg transaction
    - `whale`: $10M+ volume, $500K+ avg transaction
    - `high_volume_trader`: $5M+ volume, 100+ transactions
    - `institutional`: $1M+ avg transaction, selective trading
    - `active_trader`: 200+ transactions, $2M+ volume
    - `moderate_volume`: Meets threshold ($1M+)
    
    **Scoring Breakdown (0-100):**
    - Total Volume: 0-30 points
    - Avg Transaction Size: 0-25 points
    - Transaction Frequency: 0-20 points
    - Token Diversity: 0-15 points
    - Large Transfers ($100K+): 0-10 points
    
    **Minimum Requirements:**
    - Volume Score: ≥40/100
    - Total Volume: ≥ min_volume_threshold
    
    **Example Response:**
    ```json
    {
      "success": true,
      "source_address": "0x...",
      "transactions_analyzed": 5,
      "min_volume_threshold": 1000000,
      "discovered_count": 2,
      "wallets": [
        {
          "address": "0x...",
          "volume_score": 75,
          "total_volume": 25000000,
          "classification": "whale",
          "tags": ["whale", "very_high_volume", "large_transactions"],
          "moralis_label": "Wallet Name",
          "volume_breakdown": {...}
        }
      ]
    }
    ```
    """
    logger.info(
        f"🔍 High Volume Discovery: {source_address[:10]}... "
        f"last {num_transactions} TXs (threshold: ${min_volume_threshold:,.0f})"
    )
    
    try:
        # Import here to avoid circular imports
        from app.core.otc_analysis.api.dependencies import discover_high_volume_from_transactions
        
        # Discover high-volume wallets
        discovered = await discover_high_volume_from_transactions(
            db=db,
            source_address=source_address,
            num_transactions=num_transactions,
            min_volume_threshold=min_volume_threshold,
            filter_known_entities=filter_known_entities
        )
        
        # Build detailed response
        wallets_response = []
        
        for wallet in discovered:
            wallet_data = {
                "address": wallet["address"],
                "volume_score": wallet["volume_score"],
                "total_volume": wallet["total_volume"],
                "tx_count": wallet["tx_count"],
                "avg_transaction": wallet["avg_transaction"],
                "classification": wallet["classification"],
                "tags": wallet["tags"],
                "volume_breakdown": wallet["volume_breakdown"]
            }
            
            # Include Moralis labels if available
            if wallet.get("moralis_label"):
                wallet_data["moralis_label"] = wallet["moralis_label"]
            if wallet.get("moralis_entity"):
                wallet_data["moralis_entity"] = wallet["moralis_entity"]
            
            # Include counterparty data
            if wallet.get("counterparty_data"):
                cp_data = wallet["counterparty_data"]
                wallet_data["counterparty_info"] = {
                    "interactions_with_source": cp_data.get("tx_count", 0),
                    "volume_with_source": cp_data.get("total_volume", 0),
                    "first_interaction": cp_data.get("first_seen"),
                    "last_interaction": cp_data.get("last_seen")
                }
            
            wallets_response.append(wallet_data)
        
        # Calculate summary stats
        if wallets_response:
            total_volume = sum(w["total_volume"] for w in wallets_response)
            avg_score = sum(w["volume_score"] for w in wallets_response) / len(wallets_response)
            
            # Count classifications
            classifications = {}
            for w in wallets_response:
                classification = w["classification"]
                classifications[classification] = classifications.get(classification, 0) + 1
            
            summary = {
                "total_volume_discovered": total_volume,
                "average_score": round(avg_score, 1),
                "classifications": classifications
            }
        else:
            summary = None
        
        return {
            "success": True,
            "source_address": source_address,
            "transactions_analyzed": num_transactions,
            "min_volume_threshold": min_volume_threshold,
            "filter_enabled": filter_known_entities,
            "discovered_count": len(discovered),
            "wallets": wallets_response,
            "summary": summary,
            "message": (
                f"Analyzed last {num_transactions} transactions, "
                f"found {len(discovered)} high-volume wallets "
                f"(threshold: ${min_volume_threshold:,.0f}, "
                f"filter: {'enabled' if filter_known_entities else 'disabled'})"
            )
        }
        
    except Exception as e:
        logger.error(f"❌ High volume discovery error: {e}", exc_info=True)
        return {
            "success": False,
            "source_address": source_address,
            "error": str(e),
            "error_type": type(e).__name__
        }


@router.get("/discover/high-volume/stats")
async def get_high_volume_stats(
    min_volume: float = Query(1_000_000, ge=0),
    db: Session = Depends(get_db)
) -> Dict:
    """
    📊 Get statistics about discovered high-volume wallets.
    
    Returns counts and metrics for wallets in the database with
    entity_type='high_volume_wallet'.
    """
    from app.core.otc_analysis.models.wallet import Wallet as OTCWallet
    from sqlalchemy import func
    
    try:
        # Query high-volume wallets
        query = db.query(OTCWallet).filter(
            OTCWallet.entity_type == 'high_volume_wallet',
            OTCWallet.total_volume >= min_volume
        )
        
        wallets = query.all()
        
        if not wallets:
            return {
                "success": True,
                "count": 0,
                "message": "No high-volume wallets found"
            }
        
        # Calculate stats
        total_volume = sum(w.total_volume or 0 for w in wallets)
        avg_volume = total_volume / len(wallets)
        avg_score = sum(w.confidence_score or 0 for w in wallets) / len(wallets)
        
        # Count by classification (from tags)
        classifications = {}
        for wallet in wallets:
            tags = wallet.tags or []
            # Find classification tag
            for tag in tags:
                if tag in ['mega_whale', 'whale', 'high_volume_trader', 
                          'institutional', 'active_trader', 'moderate_volume']:
                    classifications[tag] = classifications.get(tag, 0) + 1
                    break
        
        # Top wallets by volume
        top_wallets = sorted(wallets, key=lambda w: w.total_volume or 0, reverse=True)[:10]
        
        return {
            "success": True,
            "count": len(wallets),
            "min_volume_filter": min_volume,
            "statistics": {
                "total_volume": total_volume,
                "average_volume": avg_volume,
                "average_score": round(avg_score, 1),
                "classifications": classifications
            },
            "top_10_by_volume": [
                {
                    "address": w.address,
                    "label": w.label,
                    "volume": w.total_volume,
                    "score": w.confidence_score,
                    "tags": w.tags[:5] if w.tags else []
                }
                for w in top_wallets
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting high-volume stats: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }
