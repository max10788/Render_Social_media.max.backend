from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_holders(provider, coin_type: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
    """Holt Token-Holder für einen bestimmten Coin-Typ auf Sui"""
    try:
        # Zuerst versuchen, alle Objekte vom Typ Coin mit diesem Coin-Typ zu finden
        # Dies ist eine komplexe Abfrage, die alle Coin-Objekte abruft
        holders = []
        
        # Versuche, alle Coin-Objekte für diesen Coin-Typ zu finden
        # Dies ist eine vereinfachte Implementierung - in der Praxis müsste man alle Coins suchen
        # und dann deren Besitzer ermitteln
        
        # Für Sui müssen wir die Coin-Objekte suchen und deren Besitzer ermitteln
        # Dies erfordert eine komplexe Abfrage über alle Coin-Objekte
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getAllObjects',
            'params': [
                {
                    'filter': {
                        'StructType': coin_type
                    },
                    'options': {
                        'showType': True,
                        'showOwner': True,
                        'showContent': True
                    }
                }
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            for obj in data['result'].get('data', []):
                owner = obj.get('owner')
                if owner and owner.get('AddressOwner'):
                    address = owner['AddressOwner']
                    content = obj.get('content', {})
                    if content.get('dataType') == 'moveObject':
                        balance = content.get('fields', {}).get('balance', 0)
                        holders.append({
                            'address': address,
                            'balance': balance,
                            'last_updated': datetime.now()
                        })
        
        # Sortiere nach Balance (absteigend)
        holders.sort(key=lambda x: x['balance'], reverse=True)
        
        return holders[:limit]
    except Exception as e:
        logger.error(f"Error fetching Sui token holders: {e}")
        return []


async def execute_get_coin_holders(provider, coin_type: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
    """Alternative Methode zum Abrufen von Coin-Holdern"""
    try:
        # Verwende die sui_getCoins Methode, falls verfügbar
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getCoins',
            'params': [
                '0x2::sui::SUI',  # Beispiel für SUI Coin - anpassen für spezifischen Coin-Typ
                None  # owner (optional)
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            holders = []
            for coin in data['result'].get('data', []):
                holders.append({
                    'coin_type': coin.get('coinType'),
                    'coin_object_id': coin.get('coinObjectId'),
                    'address': coin.get('owner'),
                    'balance': coin.get('balance'),
                    'last_updated': datetime.now()
                })
            
            return holders[:limit]
    except Exception as e:
        logger.error(f"Error fetching Sui coin holders: {e}")
        return []


async def execute_get_top_token_holders(provider, coin_type: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
    """Holt die Top-Holder für einen bestimmten Coin-Typ"""
    try:
        # Verwende die gleiche Logik wie execute_get_token_holders, aber mit Sortierung
        holders = await execute_get_token_holders(provider, coin_type, limit * 2)  # Mehr holen für bessere Auswahl
        
        if holders:
            # Sortiere nach Balance (absteigend)
            sorted_holders = sorted(holders, key=lambda x: x['balance'], reverse=True)
            
            # Berechne Gesamtbestand für Prozentsätze
            total_supply = sum(holder['balance'] for holder in sorted_holders)
            
            # Füge Prozentsätze hinzu und limitiere die Anzahl
            for holder in sorted_holders:
                holder['percentage'] = (holder['balance'] / total_supply * 100) if total_supply > 0 else 0
            
            return sorted_holders[:limit]
    except Exception as e:
        logger.error(f"Error fetching Sui top token holders: {e}")
        return []
