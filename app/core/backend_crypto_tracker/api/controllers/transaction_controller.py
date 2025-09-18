# app/core/backend_crypto_tracker/api/controllers/transaction_controller.py
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from app.core.backend_crypto_tracker.config.database import get_db
from app.core.backend_crypto_tracker.processor.database.models.transaction import Transaction
from app.core.backend_crypto_tracker.processor.database.models.address import Address
from app.core.backend_crypto_tracker.processor.database.models.token import Token
from app.core.backend_crypto_tracker.blockchain import EthereumProvider, SolanaProvider, SuiProvider
from app.core.backend_crypto_tracker.utils.exceptions import APIException, InvalidAddressException
from app.core.backend_crypto_tracker.utils.logger import get_logger
from pydantic import BaseModel, Field
import asyncio

logger = get_logger(__name__)

# Pydantic-Modelle für API-Antworten
class TransactionResponse(BaseModel):
    id: int
    tx_hash: str
    chain: str
    block_number: Optional[int] = None
    block_hash: Optional[str] = None
    timestamp: datetime
    from_address: str
    to_address: Optional[str] = None
    value: float
    gas_used: Optional[int] = None
    gas_price: Optional[float] = None
    fee: Optional[float] = None
    token_address: Optional[str] = None
    token_amount: Optional[float] = None
    status: str
    method: Optional[str] = None
    
    class Config:
        orm_mode = True

class TransactionDetailResponse(TransactionResponse):
    input_data: Optional[str] = None
    logs: Optional[List[Dict[str, Any]]] = None
    confirmations: Optional[int] = None
    internal_transactions: Optional[List[Dict[str, Any]]] = None

class TransactionAnalysisRequest(BaseModel):
    tx_hash: str = Field(..., description="Transaktions-Hash")
    chain: str = Field(..., description="Blockchain")
    include_internal: bool = Field(True, description="Interne Transaktionen einschließen")
    include_logs: bool = Field(True, description="Transaktions-Logs einschließen")

class TransactionAnalysisResponse(BaseModel):
    transaction: Dict[str, Any]
    risk_assessment: Dict[str, Any]
    token_transfers: List[Dict[str, Any]]
    contract_interactions: List[Dict[str, Any]]
    network_impact: Dict[str, Any]

class WalletTransactionsRequest(BaseModel):
    address: str = Field(..., description="Wallet-Adresse")
    chain: str = Field(..., description="Blockchain")
    limit: int = Field(100, ge=1, le=1000, description="Maximale Anzahl der Transaktionen")
    start_block: Optional[int] = Field(None, description="Start-Blocknummer")
    end_block: Optional[int] = Field(None, description="End-Blocknummer")
    include_token_transfers: bool = Field(True, description="Token-Transfers einschließen")

class TokenTransactionsRequest(BaseModel):
    token_address: str = Field(..., description="Token-Adresse")
    chain: str = Field(..., description="Blockchain")
    limit: int = Field(100, ge=1, le=1000, description="Maximale Anzahl der Transaktionen")
    start_time: Optional[datetime] = Field(None, description="Startzeitpunkt")
    end_time: Optional[datetime] = Field(None, description="Endzeitpunkt")

class TransactionGraphRequest(BaseModel):
    address: str = Field(..., description="Startadresse")
    chain: str = Field(..., description="Blockchain")
    depth: int = Field(2, ge=1, le=5, description="Tiefe des Graphen")
    limit: int = Field(50, ge=10, le=200, description="Maximale Anzahl der Knoten")
    include_token_flows: bool = Field(True, description="Token-Flüsse einschließen")

router = APIRouter(prefix="/transactions", tags=["transactions"])

class TransactionController:
    """Controller für Transaktions-bezogene Operationen"""
    
    def __init__(self):
        # Alte Initialisierung (ersetzen):
        # self.etherscan_api = None
        # self.solana_api = None
        # self.sui_api = None
        
        # Neue Initialisierung (hinzufügen):
        self.ethereum_provider = None
        self.bsc_provider = None
        self.solana_provider = None
        self.sui_provider = None
    
    # Alte Methode (ersetzen):
    # async def _get_api_for_chain(self, chain: str):
    #     """Holt die passende API für die Blockchain"""
    #     if chain in ['ethereum', 'bsc']:
    #         if not self.etherscan_api:
    #             self.etherscan_api = EtherscanAPI()
    #         return self.etherscan_api
    #     elif chain == 'solana':
    #         if not self.solana_api:
    #             self.solana_api = SolanaAPIService()
    #         return self.solana_api
    #     elif chain == 'sui':
    #         if not self.sui_api:
    #             self.sui_api = SuiAPIService()
    #         return self.sui_api
    #     else:
    #         raise ValueError(f"Unsupported chain: {chain}")

    # Neue Methode (hinzufügen):
    async def _get_api_for_chain(self, chain: str):
        """Holt die passende API für die Blockchain"""
        if chain == 'ethereum':
            if not self.ethereum_provider:
                self.ethereum_provider = EthereumProvider()
            return self.ethereum_provider
        elif chain == 'bsc':
            if not self.bsc_provider:
                self.bsc_provider = EthereumProvider()  # BSC verwendet auch EthereumProvider
            return self.bsc_provider
        elif chain == 'solana':
            if not self.solana_provider:
                self.solana_provider = SolanaProvider()
            return self.solana_provider
        elif chain == 'sui':
            if not self.sui_provider:
                self.sui_provider = SuiProvider()
            return self.sui_provider
        else:
            raise ValueError(f"Unsupported chain: {chain}")
    
    async def get_transaction(self, tx_hash: str, chain: str, db: Session) -> Dict[str, Any]:
        """Holt eine Transaktion anhand des Hashes"""
        try:
            # Zuerst in der Datenbank suchen
            transaction = db.query(Transaction).filter(
                Transaction.tx_hash == tx_hash,
                Transaction.chain == chain
            ).first()
            
            if transaction:
                return transaction.to_dict()
            
            # Wenn nicht in der DB, von der API holen
            api = await self._get_api_for_chain(chain)
            
            if chain in ['ethereum', 'bsc']:
                tx_data = await api.get_transaction_by_hash(tx_hash)
            elif chain == 'solana':
                tx_data = await api.get_transaction(tx_hash)
            elif chain == 'sui':
                tx_data = await api.get_transaction(tx_hash)
            else:
                raise ValueError(f"Unsupported chain: {chain}")
            
            if tx_data:
                # In der Datenbank speichern
                new_transaction = self._create_transaction_from_data(tx_data, chain)
                db.add(new_transaction)
                db.commit()
                db.refresh(new_transaction)
                return new_transaction.to_dict()
            
            raise HTTPException(status_code=404, detail="Transaction not found")
            
        except Exception as e:
            logger.error(f"Error getting transaction {tx_hash}: {e}")
            raise APIException(f"Failed to get transaction: {str(e)}")
    
    async def get_wallet_transactions(self, address: str, chain: str, 
                                  limit: int = 100, 
                                  start_block: Optional[int] = None,
                                  end_block: Optional[int] = None,
                                  db: Session = None) -> List[Dict[str, Any]]:
        """Holt Transaktionen für eine Wallet-Adresse"""
        try:
            # Zuerst in der Datenbank suchen
            if db:
                query = db.query(Transaction).filter(
                    Transaction.from_address == address,
                    Transaction.chain == chain
                )
                
                if start_block is not None:
                    query = query.filter(Transaction.block_number >= start_block)
                
                if end_block is not None:
                    query = query.filter(Transaction.block_number <= end_block)
                
                transactions = query.order_by(Transaction.block_number.desc()).limit(limit).all()
                
                if transactions:
                    return [tx.to_dict() for tx in transactions]
            
            # Wenn nicht in der DB oder nicht genug, von der API holen
            api = await self._get_api_for_chain(chain)
            
            if chain in ['ethereum', 'bsc']:
                transactions = await api.get_transactions_by_address(
                    address, start_block or 0, end_block or 99999999, limit
                )
            elif chain == 'solana':
                # Solana hat andere Parameter
                transactions = await api.get_account_info(address)
                transactions = transactions.get('transactions', [])[:limit]
            elif chain == 'sui':
                transactions = await api.get_account_info(address)
                transactions = transactions.get('transactions', [])[:limit]
            else:
                raise ValueError(f"Unsupported chain: {chain}")
            
            # Transaktionen in der Datenbank speichern
            if db and transactions:
                for tx_data in transactions:
                    existing_tx = db.query(Transaction).filter(
                        Transaction.tx_hash == tx_data.get('tx_hash', ''),
                        Transaction.chain == chain
                    ).first()
                    
                    if not existing_tx:
                        new_transaction = self._create_transaction_from_data(tx_data, chain)
                        db.add(new_transaction)
                
                db.commit()
            
            return transactions[:limit]
            
        except Exception as e:
            logger.error(f"Error getting transactions for {address}: {e}")
            raise APIException(f"Failed to get transactions: {str(e)}")
    
    async def get_token_transactions(self, token_address: str, chain: str,
                                   limit: int = 100,
                                   start_time: Optional[datetime] = None,
                                   end_time: Optional[datetime] = None,
                                   db: Session = None) -> List[Dict[str, Any]]:
        """Holt Transaktionen für einen Token"""
        try:
            # Token-Informationen holen
            if db:
                token = db.query(Token).filter(
                    Token.address == token_address,
                    Token.chain == chain
                ).first()
                
                if token:
                    # Transaktionen mit Token-Transfers suchen
                    query = db.query(Transaction).filter(
                        Transaction.token_address == token_address,
                        Transaction.chain == chain
                    )
                    
                    if start_time:
                        query = query.filter(Transaction.timestamp >= start_time)
                    
                    if end_time:
                        query = query.filter(Transaction.timestamp <= end_time)
                    
                    transactions = query.order_by(Transaction.timestamp.desc()).limit(limit).all()
                    
                    if transactions:
                        return [tx.to_dict() for tx in transactions]
            
            # Von der API holen
            api = await self._get_api_for_chain(chain)
            
            if chain in ['ethereum', 'bsc']:
                # Token-Transfers über Etherscan/BscScan
                transfers = await api.get_token_transfers(token_address, limit)
            elif chain == 'solana':
                # Token-Transfers über Solana API
                transfers = await api.get_token_transfers(token_address, limit)
            elif chain == 'sui':
                # Token-Transfers über Sui API
                transfers = await api.get_token_transfers(token_address, limit)
            else:
                raise ValueError(f"Unsupported chain: {chain}")
            
            # Transaktionen in der Datenbank speichern
            if db and transfers:
                for transfer in transfers:
                    existing_tx = db.query(Transaction).filter(
                        Transaction.tx_hash == transfer.get('tx_hash', ''),
                        Transaction.chain == chain
                    ).first()
                    
                    if not existing_tx:
                        new_transaction = self._create_transaction_from_data(transfer, chain)
                        db.add(new_transaction)
                
                db.commit()
            
            return transfers[:limit]
            
        except Exception as e:
            logger.error(f"Error getting token transactions for {token_address}: {e}")
            raise APIException(f"Failed to get token transactions: {str(e)}")
    
    async def analyze_transaction(self, tx_hash: str, chain: str, 
                                 include_internal: bool = True,
                                 include_logs: bool = True,
                                 db: Session = None) -> Dict[str, Any]:
        """Analysiert eine Transaktion umfassend"""
        try:
            # Transaktionsdaten holen
            transaction_data = await self.get_transaction(tx_hash, chain, db)
            
            # API für Analyse holen
            api = await self._get_api_for_chain(chain)
            
            # Risikobewertung
            risk_assessment = await self._assess_transaction_risk(transaction_data, chain)
            
            # Token-Transfers extrahieren
            token_transfers = []
            if include_internal and chain in ['ethereum', 'bsc']:
                try:
                    token_transfers = await api.get_internal_transactions(tx_hash)
                except Exception as e:
                    logger.warning(f"Could not get internal transactions: {e}")
            
            # Contract-Interaktionen
            contract_interactions = []
            if include_logs:
                try:
                    if chain in ['ethereum', 'bsc']:
                        logs = await api.get_transaction_receipt(tx_hash)
                        contract_interactions = self._extract_contract_interactions(logs)
                    elif chain == 'solana':
                        logs = await api.get_transaction(tx_hash)
                        contract_interactions = self._extract_solana_contract_interactions(logs)
                except Exception as e:
                    logger.warning(f"Could not get transaction logs: {e}")
            
            # Netzwerk-Auswirkung
            network_impact = await self._calculate_network_impact(transaction_data, chain)
            
            return {
                'transaction': transaction_data,
                'risk_assessment': risk_assessment,
                'token_transfers': token_transfers,
                'contract_interactions': contract_interactions,
                'network_impact': network_impact
            }
            
        except Exception as e:
            logger.error(f"Error analyzing transaction {tx_hash}: {e}")
            raise APIException(f"Failed to analyze transaction: {str(e)}")
    
    async def get_transaction_graph(self, address: str, chain: str,
                                   depth: int = 2,
                                   limit: int = 50,
                                   include_token_flows: bool = True,
                                   db: Session = None) -> Dict[str, Any]:
        """Erstellt einen Transaktionsgraphen für eine Adresse"""
        try:
            # Initiale Transaktionen holen
            initial_transactions = await self.get_wallet_transactions(
                address, chain, limit=limit, db=db
            )
            
            # Graph-Knoten und Kanten erstellen
            nodes = set()
            edges = []
            token_flows = []
            
            # Startknoten hinzufügen
            nodes.add(address.lower())
            
            # Transaktionen verarbeiten
            for tx in initial_transactions:
                from_addr = tx.get('from_address', '').lower()
                to_addr = tx.get('to_address', '').lower()
                
                if from_addr:
                    nodes.add(from_addr)
                    edges.append({
                        'from': from_addr,
                        'to': to_addr if to_addr else 'unknown',
                        'tx_hash': tx.get('tx_hash', ''),
                        'value': tx.get('value', 0),
                        'timestamp': tx.get('timestamp')
                    })
                
                if to_addr:
                    nodes.add(to_addr)
                
                # Token-Flüsse
                if include_token_flows and tx.get('token_address'):
                    token_flows.append({
                        'tx_hash': tx.get('tx_hash', ''),
                        'token_address': tx.get('token_address'),
                        'from_address': from_addr,
                        'to_address': to_addr,
                        'amount': tx.get('token_amount', 0)
                    })
            
            # Rekursiv weitere Knoten hinzufügen (bis zur gewünschten Tiefe)
            if depth > 1:
                additional_nodes = list(nodes - {address.lower()})
                
                for node_addr in additional_nodes[:10]:  # Begrenzung zur Vermeidung von Explosionen
                    try:
                        node_transactions = await self.get_wallet_transactions(
                            node_addr, chain, limit=10, db=db
                        )
                        
                        for tx in node_transactions:
                            from_addr = tx.get('from_address', '').lower()
                            to_addr = tx.get('to_address', '').lower()
                            
                            if from_addr in nodes or len(nodes) < limit:
                                nodes.add(from_addr)
                                edges.append({
                                    'from': from_addr,
                                    'to': to_addr if to_addr else 'unknown',
                                    'tx_hash': tx.get('tx_hash', ''),
                                    'value': tx.get('value', 0),
                                    'timestamp': tx.get('timestamp')
                                })
                            
                            if to_addr in nodes or len(nodes) < limit:
                                nodes.add(to_addr)
                    
                    except Exception as e:
                        logger.warning(f"Could not get transactions for {node_addr}: {e}")
            
            # Für die Ausgabe formatieren
            nodes_list = [
                {
                    'id': addr,
                    'label': addr[:8] + '...' if len(addr) > 10 else addr,
                    'type': 'address'
                }
                for addr in list(nodes)[:limit]
            ]
            
            return {
                'nodes': nodes_list,
                'edges': edges[:limit*2],  # Mehr Kanten für bessere Visualisierung
                'token_flows': token_flows[:limit],
                'stats': {
                    'total_nodes': len(nodes_list),
                    'total_edges': len(edges),
                    'depth': depth,
                    'token_flows_count': len(token_flows)
                }
            }
            
        except Exception as e:
            logger.error(f"Error creating transaction graph for {address}: {e}")
            raise APIException(f"Failed to create transaction graph: {str(e)}")
    
    def _create_transaction_from_data(self, tx_data: Dict[str, Any], chain: str) -> Transaction:
        """Erstellt ein Transaction-Objekt aus API-Daten"""
        return Transaction(
            tx_hash=tx_data.get('tx_hash', ''),
            chain=chain,
            block_number=tx_data.get('block_number'),
            block_hash=tx_data.get('block_hash'),
            timestamp=tx_data.get('timestamp', datetime.utcnow()),
            from_address=tx_data.get('from_address', ''),
            to_address=tx_data.get('to_address'),
            value=tx_data.get('value', 0),
            gas_used=tx_data.get('gas_used'),
            gas_price=tx_data.get('gas_price'),
            fee=tx_data.get('fee'),
            token_address=tx_data.get('token_address'),
            token_amount=tx_data.get('token_amount'),
            status=tx_data.get('status', 'unknown'),
            method=tx_data.get('method'),
            input_data=tx_data.get('input_data'),
            logs=tx_data.get('logs')
        )
    
    async def _assess_transaction_risk(self, transaction_data: Dict[str, Any], chain: str) -> Dict[str, Any]:
        """Bewertet das Risiko einer Transaktion"""
        risk_score = 0
        risk_factors = []
        
        # Transaktionswert
        value = transaction_data.get('value', 0)
        if value > 1000:  # Hoher Wert in ETH
            risk_score += 20
            risk_factors.append('high_value_transaction')
        
        # Gas-Preis
        gas_price = transaction_data.get('gas_price', 0)
        if gas_price > 200 * 10**9:  # > 200 Gwei
            risk_score += 15
            risk_factors.append('high_gas_price')
        
        # Token-Transfers
        if transaction_data.get('token_address'):
            risk_score += 10
            risk_factors.append('token_transfer')
        
        # Interne Transaktionen
        if transaction_data.get('internal_transactions'):
            risk_score += 15
            risk_factors.append('internal_transactions')
        
        # Chain-spezifische Risiken
        if chain == 'bsc':
            risk_score += 10
            risk_factors.append('bsc_high_risk_chain')
        
        # Status
        if transaction_data.get('status') != 'success':
            risk_score += 30
            risk_factors.append('failed_transaction')
        
        # Risikolevel bestimmen
        if risk_score >= 70:
            risk_level = 'high'
        elif risk_score >= 40:
            risk_level = 'medium'
        else:
            risk_level = 'low'
        
        return {
            'risk_score': min(100, risk_score),
            'risk_level': risk_level,
            'risk_factors': risk_factors,
            'assessment_date': datetime.utcnow().isoformat()
        }
    
    def _extract_contract_interactions(self, logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extrahiert Contract-Interaktionen aus Logs"""
        interactions = []
        
        for log in logs:
            if log.get('address') and log.get('topics'):
                interactions.append({
                    'contract_address': log['address'],
                    'topics': log['topics'][:3],  # Erste 3 Topics
                    'event_signature': log['topics'][0][:10] if log['topics'] else '',
                    'log_index': log.get('logIndex')
                })
        
        return interactions
    
    def _extract_solana_contract_interactions(self, tx_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extrahiert Contract-Interaktionen aus Solana-Transaktionen"""
        interactions = []
        
        # Vereinfachte Extraktion für Solana
        if 'accountKeys' in tx_data:
            for account in tx_data['accountKeys']:
                if account.get('writable') and account.get('signer'):
                    interactions.append({
                        'account': account.get('pubkey'),
                        'writable': account.get('writable'),
                        'signer': account.get('signer')
                    })
        
        return interactions
    
    async def _calculate_network_impact(self, transaction_data: Dict[str, Any], chain: str) -> Dict[str, Any]:
        """Berechnet die Netzwerk-Auswirkung einer Transaktion"""
        impact_score = 0
        
        # Transaktionswert
        value = transaction_data.get('value', 0)
        if value > 10:  # > 10 ETH
            impact_score += 30
        elif value > 1:  # > 1 ETH
            impact_score += 15
        
        # Gas-Verbrauch
        gas_used = transaction_data.get('gas_used', 0)
        if gas_used > 500000:  # Hoher Gas-Verbrauch
            impact_score += 20
        
        # Anzahl der Token-Transfers
        if transaction_data.get('token_amount', 0) > 0:
            impact_score += 10
        
        # Interne Transaktionen
        if transaction_data.get('internal_transactions'):
            impact_score += 15
        
        # Chain-spezifische Auswirkungen
        if chain == 'ethereum':
            impact_score *= 1.2  # Ethereum hat höhere Netzwerk-Auswirkung
        elif chain == 'solana':
            impact_score *= 0.8  # Solana hat geringere Netzwerk-Auswirkung
        
        return {
            'impact_score': min(100, impact_score),
            'value_impact': value,
            'gas_impact': gas_used,
            'chain_multiplier': 1.2 if chain == 'ethereum' else 0.8 if chain == 'solana' else 1.0
        }

# Controller-Instanz für die Routes
transaction_controller = TransactionController()
