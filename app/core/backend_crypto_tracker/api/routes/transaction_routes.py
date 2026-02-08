# app/core/backend_crypto_tracker/api/routes/transaction_routes.py
from fastapi import APIRouter, Depends, HTTPException, Query, Path, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime
from app.core.backend_crypto_tracker.config.database import get_db
from app.core.backend_crypto_tracker.api.controllers.transaction_controller import transaction_controller
from app.core.backend_crypto_tracker.processor.database.models.transaction import Transaction
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/transactions", tags=["transactions"])

# IMPORTANT: Specific routes must come BEFORE generic path parameter routes
# Otherwise FastAPI will match "/recent" as tx_hash="recent"

@router.get("/recent", response_model=List[Dict[str, Any]])
async def get_recent_transactions(
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    limit: int = Query(50, ge=1, le=200, description="Maximale Anzahl der Transaktionen"),
    min_value: Optional[float] = Query(None, description="Minimaler Transaktionswert"),
    max_value: Optional[float] = Query(None, description="Maximaler Transaktionswert"),
    db: Session = Depends(get_db)
):
    """Holt kürzliche Transaktionen mit Filteroptionen"""
    try:
        query = db.query(Transaction).filter(Transaction.chain == chain)

        if min_value is not None:
            query = query.filter(Transaction.value >= min_value)

        if max_value is not None:
            query = query.filter(Transaction.value <= max_value)

        transactions = query.order_by(Transaction.timestamp.desc()).limit(limit).all()

        return [tx.to_dict() for tx in transactions]
    except Exception as e:
        logger.error(f"Unexpected error getting recent transactions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/statistics", response_model=Dict[str, Any])
async def get_transaction_statistics(
    chain: Optional[str] = Query(None, description="Blockchain für Statistiken"),
    time_range: int = Query(24, ge=1, le=168, description="Zeitbereich in Stunden"),
    db: Session = Depends(get_db)
):
    """Ruft Transaktionsstatistiken"""
    try:
        from datetime import timedelta

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=time_range)

        query = db.query(Transaction)

        if chain:
            query = query.filter(Transaction.chain == chain)

        query = query.filter(Transaction.timestamp >= start_time)
        query = query.filter(Transaction.timestamp <= end_time)

        # Gesamtzahl der Transaktionen
        total_transactions = query.count()

        # Durchschnittlicher Transaktionswert
        avg_value = query.with_entities(
            db.func.avg(Transaction.value)
        ).scalar() or 0

        # Durchschnittliche Gas-Gebühren
        avg_fee = query.with_entities(
            db.func.avg(Transaction.fee)
        ).scalar() or 0

        # Erfolgsrate
        success_count = query.filter(Transaction.status == 'success').count()
        success_rate = (success_count / total_transactions * 100) if total_transactions > 0 else 0

        # Token-Transaktionen
        token_tx_count = query.filter(Transaction.token_address.isnot(None)).count()

        # Nach Blockchain gruppieren
        chain_stats = {}
        if not chain:
            chains = db.query(Transaction.chain, db.func.count(Transaction.id)).group_by(Transaction.chain).all()
            chain_stats = {chain_name: count for chain_name, count in chains}

        return {
            "time_range_hours": time_range,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "total_transactions": total_transactions,
            "average_value": float(avg_value),
            "average_fee": float(avg_fee),
            "success_rate": success_rate,
            "token_transactions": token_tx_count,
            "chain_distribution": chain_stats,
            "last_updated": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Unexpected error getting transaction statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/search", response_model=List[Dict[str, Any]])
async def search_transactions(
    query: str = Query(..., min_length=3, description="Suchbegriff"),
    chain: Optional[str] = Query(None, description="Blockchain (ethereum, bsc, solana, sui)"),
    limit: int = Query(50, ge=1, le=200, description="Maximale Anzahl der Ergebnisse"),
    search_type: str = Query("address", description="Suchtyp: address, hash, method"),
    db: Session = Depends(get_db)
):
    """Sucht nach Transaktionen"""
    try:
        search_query = f"%{query.lower()}%"

        if search_type == "address":
            # Suche in Adressfeldern
            db_query = db.query(Transaction).filter(
                Transaction.from_address.ilike(search_query) |
                Transaction.to_address.ilike(search_query)
            )
        elif search_type == "hash":
            # Suche im Transaktions-Hash
            db_query = db.query(Transaction).filter(
                Transaction.tx_hash.ilike(search_query)
            )
        elif search_type == "method":
            # Suche in Methoden
            db_query = db.query(Transaction).filter(
                Transaction.method.ilike(search_query)
            )
        else:
            raise HTTPException(status_code=400, detail="Invalid search type")

        if chain:
            db_query = db_query.filter(Transaction.chain == chain)

        transactions = db_query.limit(limit).all()

        return [tx.to_dict() for tx in transactions]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error searching transactions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/analyze", response_model=Dict[str, Any])
async def analyze_transaction(
    tx_hash: str,
    chain: str,
    include_internal: bool = True,
    include_logs: bool = True,
    db: Session = Depends(get_db)
):
    """Analysiert eine Transaktion umfassend"""
    try:
        result = await transaction_controller.analyze_transaction(
            tx_hash, chain, include_internal, include_logs, db
        )
        return result
    except HTTPException:
        raise
    except APIException as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error analyzing transaction {tx_hash}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{tx_hash}/detail", response_model=Dict[str, Any])
async def get_transaction_detail(
    tx_hash: str = Path(..., description="Transaktions-Hash"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    include_internal: bool = Query(True, description="Interne Transaktionen einschließen"),
    include_logs: bool = Query(True, description="Transaktions-Logs einschließen"),
    db: Session = Depends(get_db)
):
    """Holt detaillierte Transaktionsinformationen"""
    try:
        result = await transaction_controller.get_transaction(tx_hash, chain, db)
        
        # Zusätzliche Details abrufen
        if include_internal or include_logs:
            detailed_result = await transaction_controller.analyze_transaction(
                tx_hash, chain, include_internal, include_logs, db
            )
            return detailed_result
        
        return result
    except HTTPException:
        raise
    except APIException as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error getting transaction detail {tx_hash}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/address/{address}", response_model=List[Dict[str, Any]])
async def get_wallet_transactions(
    address: str = Path(..., description="Wallet-Adresse"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximale Anzahl der Transaktionen"),
    start_block: Optional[int] = Query(None, description="Start-Blocknummer"),
    end_block: Optional[int] = Query(None, description="End-Blocknummer"),
    include_token_transfers: bool = Query(True, description="Token-Transfers markieren"),
    db: Session = Depends(get_db)
):
    """Holt Transaktionen für eine Wallet-Adresse"""
    try:
        result = await transaction_controller.get_wallet_transactions(
            address, chain, limit, start_block, end_block, db
        )
        
        # Token-Transfers markieren, falls gewünscht
        if include_token_transfers:
            for tx in result:
                if tx.get('token_address'):
                    tx['is_token_transfer'] = True
        
        return result
    except HTTPException:
        raise
    except APIException as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error getting wallet transactions for {address}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/token/{token_address}", response_model=List[Dict[str, Any]])
async def get_token_transactions(
    token_address: str = Path(..., description="Token-Adresse"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximale Anzahl der Transaktionen"),
    start_time: Optional[datetime] = Query(None, description="Startzeitpunkt (ISO Format)"),
    end_time: Optional[datetime] = Query(None, description="Endzeitpunkt (ISO Format)"),
    db: Session = Depends(get_db)
):
    """Holt Transaktionen für einen Token"""
    try:
        result = await transaction_controller.get_token_transactions(
            token_address, chain, limit, start_time, end_time, db
        )
        return result
    except HTTPException:
        raise
    except APIException as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error getting token transactions for {token_address}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/graph/{address}", response_model=Dict[str, Any])
async def get_transaction_graph(
    address: str = Path(..., description="Startadresse für den Graphen"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    depth: int = Query(2, ge=1, le=5, description="Tiefe des Graphen"),
    limit: int = Query(50, ge=10, le=200, description="Maximale Anzahl der Knoten"),
    include_token_flows: bool = Query(True, description="Token-Flüsse einschließen"),
    db: Session = Depends(get_db)
):
    """Erstellt einen Transaktionsgraphen für eine Adresse"""
    try:
        result = await transaction_controller.get_transaction_graph(
            address, chain, depth, limit, include_token_flows, db
        )
        return result
    except HTTPException:
        raise
    except APIException as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating transaction graph for {address}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Generic route must come LAST to avoid matching specific routes
@router.get("/{tx_hash}", response_model=Dict[str, Any])
async def get_transaction(
    tx_hash: str = Path(..., description="Transaktions-Hash"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    db: Session = Depends(get_db)
):
    """Holt eine Transaktion anhand ihres Hashes"""
    try:
        result = await transaction_controller.get_transaction(tx_hash, chain, db)
        return result
    except HTTPException:
        raise
    except APIException as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error getting transaction {tx_hash}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

