from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_active_wallets(provider, contract_address: str, hours: int = 6) -> List[Dict[str, Any]]:
    """
    Holt alle Wallets, die in den letzten X Stunden mit einem Smart Contract interagiert haben.
    
    Args:
        contract_address: Die Adresse des Smart Contracts
        hours: Zeitraum in Stunden (Standard: 6)
        
    Returns:
        Liste der aktiven Wallets mit ihren Transaktionen
    """
    try:
        logger.info(f"Suche aktive Wallets für Contract {contract_address} der letzten {hours} Stunden")
        
        # Hole Transaktionen des Contracts
        transactions = await execute_get_contract_transactions(provider, contract_address, hours)
        
        if not transactions:
            logger.warning(f"Keine Transaktionen für {contract_address} in den letzten {hours} Stunden gefunden")
            return []
        
        # Extrahiere einzigartige Wallet-Adressen
        active_wallets = set()
        for tx in transactions:
            if tx.get('from'):
                active_wallets.add(tx['from'])
            if tx.get('to'):
                active_wallets.add(tx['to'])
        
        logger.info(f"Gefundene aktive Wallet-Adressen: {len(active_wallets)}")
        
        # Für jede Wallet, hole detaillierte Informationen
        wallet_details = []
        for wallet_address in active_wallets:
            try:
                # Hole Token-Bestand der Wallet
                token_balance = await execute_get_token_balance(provider, contract_address, wallet_address)
                
                # Hole Transaktionshistorie der Wallet
                wallet_txs = await execute_get_address_transactions(
                    provider,
                    wallet_address, 
                    start_block=0, 
                    end_block=99999999,
                    sort='desc'
                )
                
                # Berechne Statistiken
                total_txs = len(wallet_txs) if wallet_txs else 0
                first_tx_time = wallet_txs[-1]['timestamp'] if wallet_txs else None
                last_tx_time = wallet_txs[0]['timestamp'] if wallet_txs else None
                
                wallet_details.append({
                    'address': wallet_address,
                    'token_balance': token_balance,
                    'total_transactions': total_txs,
                    'first_transaction': first_tx_time,
                    'last_transaction': last_tx_time,
                    'is_contract': wallet_address.lower() == contract_address.lower()
                })
                
            except Exception as e:
                logger.warning(f"Fehler bei der Analyse von Wallet {wallet_address}: {e}")
                continue
        
        # Sortiere nach Token-Bestand (absteigend)
        wallet_details.sort(key=lambda x: x['token_balance'], reverse=True)
        
        logger.info(f"Analyse von {len(wallet_details)} aktiven Wallets abgeschlossen")
        return wallet_details
        
    except Exception as e:
        logger.error(f"Fehler bei der Abfrage aktiver Wallets: {e}")
        return []
