from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_top_token_holders(provider, token_address: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Holt die Top-Holder eines Tokens basierend auf ihren Token-Beständen.
    
    Args:
        token_address: Die Adresse des Tokens
        limit: Maximale Anzahl an Holdern, die abgerufen werden sollen
        
    Returns:
        Liste der Top-Holder mit ihren Beständen und Prozentsätzen
    """
    try:
        logger.info(f"Hole Top-Holder für Token {token_address}")
        
        # Methode 1: Direkte Abfrage über Etherscan Token Holder API
        if provider.api_key:
            holders = await _get_holders_from_etherscan_api(provider, token_address, limit)
            if holders:
                logger.info(f"{len(holders)} Holder von Etherscan API erhalten")
                return holders
        
        # Methode 2: Analyse von Token-Transfers
        logger.info("Fallback: Analyse von Token-Transfers")
        holders = await _get_holders_from_token_transfers(provider, token_address, limit)
        
        if holders:
            logger.info(f"{len(holders)} Holder aus Token-Transfers analysiert")
            return holders
        
        # Methode 3: Abfrage der aktiven Wallets als letzter Fallback
        logger.info("Letzter Fallback: Abfrage aktiver Wallets")
        active_wallets = await execute_get_active_wallets(provider, token_address, hours=24)
        
        # Konvertiere in das erwartete Format
        holders = []
        total_supply = sum(w['token_balance'] for w in active_wallets if w['token_balance'] > 0)
        
        for wallet in active_wallets:
            if wallet['token_balance'] > 0:
                percentage = (wallet['token_balance'] / total_supply * 100) if total_supply > 0 else 0
                holders.append({
                    'address': wallet['address'],
                    'balance': wallet['token_balance'],
                    'percentage': percentage
                })
        
        # Sortiere nach Balance
        holders.sort(key=lambda x: x['balance'], reverse=True)
        holders = holders[:limit]
        
        logger.info(f"{len(holders)} Holder aus aktiven Wallets ermittelt")
        return holders
        
    except Exception as e:
        logger.error(f"Fehler bei der Abfrage der Top-Holder: {e}")
        return []


async def _get_holders_from_etherscan_api(provider, token_address: str, limit: int) -> List[Dict[str, Any]]:
    """Holt Holder direkt von der Etherscan Token Holder API"""
    try:
        params = {
            'module': 'token',
            'action': 'tokenholderlist',
            'contractaddress': token_address,
            'page': '1',
            'offset': str(limit),
            'sort': 'desc',
            'apikey': provider.api_key
        }
        
        data = await provider._make_request(provider.base_url, params)
        
        if data and data.get('status') == '1' and data.get('message') == 'OK':
            result = []
            total_supply = 0
            
            # Berechne die Gesamtmenge für Prozentsätze
            for holder in data.get('result', []):
                try:
                    quantity = float(holder.get('TokenHolderQuantity', 0))
                    total_supply += quantity
                except (ValueError, TypeError):
                    continue
            
            # Erstelle die Holder-Liste
            for holder in data.get('result', []):
                try:
                    address = holder.get('TokenHolderAddress')
                    quantity = float(holder.get('TokenHolderQuantity', 0))
                    percentage = (quantity / total_supply * 100) if total_supply > 0 else 0
                    
                    result.append({
                        'address': address,
                        'balance': quantity,
                        'percentage': percentage
                    })
                except (ValueError, TypeError):
                    continue
            
            return result[:limit]
            
    except Exception as e:
        logger.error(f"Fehler bei der Etherscan API Abfrage: {e}")
    
    return []


async def _get_holders_from_token_transfers(provider, token_address: str, limit: int) -> List[Dict[str, Any]]:
    """Analysiert Token-Transfers, um die Top-Holder zu ermitteln"""
    try:
        params = {
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': token_address,
            'sort': 'desc',
            'apikey': provider.api_key
        }
        
        data = await provider._make_request(provider.base_url, params)
        
        if data and data.get('status') == '1' and data.get('result'):
            balances = {}
            decimals = 18  # Standardwert, wird später aktualisiert
            
            # Versuche, die Decimals aus der ersten Transaktion zu ermitteln
            for tx in data['result']:
                try:
                    decimals = int(tx.get('tokenDecimal', 18))
                    break
                except (ValueError, TypeError):
                    continue
            
            # Analysiere alle Transaktionen
            for tx in data['result']:
                try:
                    from_address = tx.get('from')
                    to_address = tx.get('to')
                    value = int(tx.get('value', 0))
                    
                    # Konvertiere in tatsächlichen Token-Wert
                    token_value = value / (10 ** decimals)
                    
                    # Aktualisiere den Saldo des Absenders
                    if from_address not in balances:
                        balances[from_address] = 0
                    balances[from_address] -= token_value
                    
                    # Aktualisiere den Saldo des Empfängers
                    if to_address not in balances:
                        balances[to_address] = 0
                    balances[to_address] += token_value
                except (ValueError, TypeError, ZeroDivisionError):
                    continue
            
            # Filtere Null-Beträge und negative Salden
            balances = {addr: bal for addr, bal in balances.items() if bal > 0}
            
            # Sortiere nach Balance (absteigend)
            sorted_balances = sorted(balances.items(), key=lambda x: x[1], reverse=True)
            
            # Berechne den Gesamtbestand für Prozentangaben
            total_supply = sum(bal for _, bal in sorted_balances)
            
            # Erstelle die Ergebnisliste
            holders = []
            for address, balance in sorted_balances[:limit]:
                percentage = (balance / total_supply * 100) if total_supply > 0 else 0
                holders.append({
                    'address': address,
                    'balance': balance,
                    'percentage': percentage
                })
            
            return holders
            
    except Exception as e:
        logger.error(f"Fehler bei der Analyse der Token-Transfers: {e}")
    
    return []
