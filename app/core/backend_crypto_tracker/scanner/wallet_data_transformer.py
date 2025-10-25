"""
Wallet Data Transformer - Konvertiert DB-Wallet-Daten ins Frontend-Format
"""
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class WalletDataTransformer:
    """Transformiert Wallet-Daten für das Frontend"""
    
    @staticmethod
    def transform_classified_wallet(
        wallet_data: Dict[str, Any],
        token_address: str,
        chain: str,
        classification_result: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Transformiert eine klassifizierte Wallet ins Frontend-Format.
        
        Args:
            wallet_data: Rohe Wallet-Daten aus der DB
            token_address: Token-Adresse
            chain: Blockchain
            classification_result: Klassifizierungsergebnis (wallet_type, confidence, etc.)
        
        Returns:
            Dict im Frontend-Format
        """
        try:
            # Extract basic info
            wallet_address = wallet_data.get('address') or wallet_data.get('wallet_address', 'Unknown')
            
            # Classification info
            wallet_type = "unclassified"
            confidence_score = 0.0
            
            if classification_result:
                wallet_type = classification_result.get('wallet_type', 'unclassified').lower()
                confidence_score = float(classification_result.get('confidence_score', 0.0))
            
            # Token balance data
            balance = float(wallet_data.get('balance', 0.0))
            percentage_of_supply = float(wallet_data.get('percentage_of_supply', 0.0))
            
            # Transaction data
            tx_count = int(wallet_data.get('transaction_count', 0) or 
                          wallet_data.get('tx_count', 0) or 0)
            
            # Timestamps
            first_tx = wallet_data.get('first_transaction') or wallet_data.get('first_tx_timestamp')
            last_tx = wallet_data.get('last_transaction') or wallet_data.get('last_tx_timestamp')
            
            # Format timestamps to ISO format
            first_transaction = WalletDataTransformer._format_timestamp(first_tx)
            last_transaction = WalletDataTransformer._format_timestamp(last_tx)
            
            # Risk analysis
            risk_score = int(wallet_data.get('risk_score', 0))
            risk_flags = wallet_data.get('risk_flags', [])
            if isinstance(risk_flags, str):
                risk_flags = [risk_flags] if risk_flags else []
            
            # Metadata
            created_at = WalletDataTransformer._format_timestamp(
                wallet_data.get('created_at') or datetime.utcnow()
            )
            updated_at = WalletDataTransformer._format_timestamp(
                wallet_data.get('updated_at') or datetime.utcnow()
            )
            
            # Build frontend wallet object
            frontend_wallet = {
                # Identifikation
                "wallet_address": wallet_address,
                "chain": chain.capitalize(),
                "wallet_type": wallet_type,
                "confidence_score": round(confidence_score, 4),
                
                # Token-Daten
                "token_address": token_address,
                "balance": round(balance, 2),
                "percentage_of_supply": round(percentage_of_supply, 4),
                
                # Transaktionen
                "transaction_count": tx_count,
                "first_transaction": first_transaction,
                "last_transaction": last_transaction,
                
                # Risikoanalyse
                "risk_score": risk_score,
                "risk_flags": risk_flags,
                
                # Metadaten
                "created_at": created_at,
                "updated_at": updated_at
            }
            
            # Add optional detailed metrics if available
            if classification_result and 'metrics' in classification_result:
                frontend_wallet['detailed_metrics'] = classification_result['metrics']
            
            return frontend_wallet
            
        except Exception as e:
            logger.error(f"Error transforming wallet data: {str(e)}", exc_info=True)
            return WalletDataTransformer._create_fallback_wallet(
                wallet_address=wallet_data.get('address', 'Unknown'),
                token_address=token_address,
                chain=chain
            )
    
    @staticmethod
    def transform_unclassified_wallet(
        wallet_data: Dict[str, Any],
        token_address: str,
        chain: str
    ) -> Dict[str, Any]:
        """
        Transformiert eine NICHT klassifizierte Wallet (nur Trade Stats).
        
        Diese Wallets haben nur Basic Info ohne vollständige Klassifizierung.
        """
        try:
            wallet_address = wallet_data.get('address') or wallet_data.get('wallet_address', 'Unknown')
            
            # Basic transaction info
            tx_count = int(wallet_data.get('transaction_count', 0) or 
                          wallet_data.get('tx_count', 0) or 0)
            
            balance = float(wallet_data.get('balance', 0.0))
            
            # Timestamps
            first_tx = wallet_data.get('first_transaction') or wallet_data.get('first_tx_timestamp')
            last_tx = wallet_data.get('last_transaction') or wallet_data.get('last_tx_timestamp')
            
            first_transaction = WalletDataTransformer._format_timestamp(first_tx)
            last_transaction = WalletDataTransformer._format_timestamp(last_tx)
            
            # Unclassified wallet structure (simpler)
            return {
                # Identifikation
                "wallet_address": wallet_address,
                "chain": chain.capitalize(),
                "wallet_type": "unclassified",
                "confidence_score": 0.0,
                "classified": False,  # Flag für Frontend
                
                # Token-Daten (basic)
                "token_address": token_address,
                "balance": round(balance, 2),
                "percentage_of_supply": round(float(wallet_data.get('percentage_of_supply', 0.0)), 4),
                
                # Transaktionen (basic)
                "transaction_count": tx_count,
                "first_transaction": first_transaction,
                "last_transaction": last_transaction,
                
                # Keine Risk Analysis für unclassified
                "risk_score": 0,
                "risk_flags": [],
                
                # Metadaten
                "created_at": WalletDataTransformer._format_timestamp(datetime.utcnow()),
                "updated_at": WalletDataTransformer._format_timestamp(datetime.utcnow())
            }
            
        except Exception as e:
            logger.error(f"Error transforming unclassified wallet: {str(e)}", exc_info=True)
            return WalletDataTransformer._create_fallback_wallet(
                wallet_address=wallet_data.get('address', 'Unknown'),
                token_address=token_address,
                chain=chain,
                classified=False
            )
    
    @staticmethod
    def _format_timestamp(timestamp: Any) -> str:
        """Formatiert Timestamp ins ISO-Format"""
        if timestamp is None:
            return datetime.utcnow().isoformat() + "Z"
        
        if isinstance(timestamp, str):
            # Already formatted
            if timestamp.endswith('Z') or '+' in timestamp:
                return timestamp
            # Try to parse and reformat
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return dt.isoformat() + "Z"
            except:
                return timestamp
        
        if isinstance(timestamp, datetime):
            return timestamp.isoformat() + "Z"
        
        # Fallback
        return datetime.utcnow().isoformat() + "Z"
    
    @staticmethod
    def _create_fallback_wallet(
        wallet_address: str,
        token_address: str,
        chain: str,
        classified: bool = True
    ) -> Dict[str, Any]:
        """Erstellt ein Fallback-Wallet-Objekt bei Fehlern"""
        now = datetime.utcnow().isoformat() + "Z"
        
        return {
            "wallet_address": wallet_address,
            "chain": chain.capitalize(),
            "wallet_type": "unknown" if classified else "unclassified",
            "confidence_score": 0.0,
            "classified": classified,
            "token_address": token_address,
            "balance": 0.0,
            "percentage_of_supply": 0.0,
            "transaction_count": 0,
            "first_transaction": now,
            "last_transaction": now,
            "risk_score": 0,
            "risk_flags": ["Data unavailable"],
            "created_at": now,
            "updated_at": now
        }
    
    @staticmethod
    def transform_wallet_list(
        wallets: List[Dict[str, Any]],
        token_address: str,
        chain: str,
        classification_results: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transformiert eine Liste von Wallets.
        
        Returns:
            {
                "classified": [...],  # Top 50 with full classification
                "unclassified": [...]  # Rest with basic stats only
            }
        """
        classified = []
        unclassified = []
        
        classification_map = {}
        if classification_results:
            # Create lookup map: wallet_address -> classification
            for wallet_addr, classification in classification_results.items():
                classification_map[wallet_addr.lower()] = classification
        
        for wallet in wallets:
            wallet_addr = (wallet.get('address') or wallet.get('wallet_address', '')).lower()
            
            # Check if this wallet has classification
            if wallet_addr in classification_map:
                # Classified wallet
                transformed = WalletDataTransformer.transform_classified_wallet(
                    wallet_data=wallet,
                    token_address=token_address,
                    chain=chain,
                    classification_result=classification_map[wallet_addr]
                )
                classified.append(transformed)
            else:
                # Unclassified wallet (basic stats only)
                transformed = WalletDataTransformer.transform_unclassified_wallet(
                    wallet_data=wallet,
                    token_address=token_address,
                    chain=chain
                )
                unclassified.append(transformed)
        
        logger.info(f"Transformed {len(classified)} classified and {len(unclassified)} unclassified wallets")
        
        return {
            "classified": classified,
            "unclassified": unclassified
        }

