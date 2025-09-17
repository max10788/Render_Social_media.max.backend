"""
Sui blockchain API provider implementation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from ..base_provider import BaseAPIProvider
from ...data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class SuiProvider(BaseAPIProvider):
    """Sui Blockchain API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Sui", "https://fullnode.mainnet.sui.io", api_key, "SUI_API_KEY")
    
    async def get_object_info(self, object_id: str) -> Optional[Dict[str, Any]]:
        """Holt Informationen zu einem Sui-Objekt"""
        try:
            url = f"{self.base_url}"
            params = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': 'sui_getObject',
                'params': [
                    object_id,
                    {'showType': True, 'showOwner': True, 'showPreviousTransaction': True}
                ]
            }
            
            data = await self._make_post_request(url, params)
            
            if data and data.get('result'):
                obj = data['result']
                return {
                    'object_id': obj.get('objectId'),
                    'type': obj.get('type'),
                    'owner': obj.get('owner'),
                    'previous_transaction': obj.get('previousTransaction'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Sui object info: {e}")
        
        return None
    
    async def get_account_balance(self, address: str) -> Optional[Dict[str, Any]]:
        """Holt den Kontostand einer Sui-Adresse"""
        try:
            url = f"{self.base_url}"
            params = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': 'sui_getBalance',
                'params': [address]
            }
            
            data = await self._make_post_request(url, params)
            
            if data and data.get('result'):
                balance_info = data['result']
                return {
                    'address': address,
                    'balance': int(balance_info.get('totalBalance', 0)) / 10**9,  # MIST zu SUI
                    'balance_mist': int(balance_info.get('totalBalance', 0)),
                    'coin_object_count': balance_info.get('coinObjectCount', 0),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Sui account balance: {e}")
        
        return None
    
    async def get_account_objects(self, address: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        """Holt alle Objekte einer Sui-Adresse"""
        try:
            url = f"{self.base_url}"
            params = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': 'sui_getObjectsOwnedByAddress',
                'params': [address]
            }
            
            data = await self._make_post_request(url, params)
            
            if data and data.get('result'):
                objects = data['result']
                object_list = []
                
                for obj_id in objects[:limit]:  # Limitieren
                    obj_info = await self.get_object_info(obj_id)
                    if obj_info:
                        object_list.append(obj_info)
                
                return object_list
        except Exception as e:
            logger.error(f"Error fetching Sui account objects: {e}")
        
        return None
    
    async def get_transaction_details(self, tx_digest: str) -> Optional[Dict[str, Any]]:
        """Holt Details zu einer spezifischen Transaktion"""
        try:
            url = f"{self.base_url}"
            params = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': 'sui_getTransaction',
                'params': [tx_digest]
            }
            
            data = await self._make_post_request(url, params)
            
            if data and data.get('result'):
                tx = data['result']
                return {
                    'transaction_digest': tx.get('digest'),
                    'timestamp': datetime.fromtimestamp(int(tx.get('timestampMs', 0)) / 1000) if tx.get('timestampMs') else None,
                    'status': tx.get('status', {}).get('status'),
                    'effects': tx.get('effects', {}),
                    'events': tx.get('events', []),
                    'object_changes': tx.get('objectChanges', []),
                    'balance_changes': tx.get('balanceChanges', []),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Sui transaction details: {e}")
        
        return None
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Sui-spezifische Token-Preisabfrage"""
        try:
            # Sui hat keine native Token-Preis-API, daher leere Implementierung
            # In der Praxis würde man hier eine externe API verwenden
            return None
        except Exception as e:
            logger.error(f"Error fetching Sui token price: {e}")
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_second": 5, "requests_per_minute": 300}
