"""
Solana blockchain API provider implementation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from ..base_provider import BaseAPIProvider
from ...data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class SolanaProvider(BaseAPIProvider):
    """Solana Blockchain API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Solana", "https://api.mainnet-beta.solana.com", api_key, "SOLANA_API_KEY")
    
    async def get_account_balance(self, pubkey: str) -> Optional[Dict[str, Any]]:
        """Holt den Kontostand einer Solana-Adresse"""
        try:
            url = self.base_url
            params = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': 'getAccountInfo',
                'params': [
                    pubkey,
                    {'encoding': 'base64'}
                ]
            }
            
            data = await self._make_post_request(url, params)
            
            if data and data.get('result'):
                account_info = data['result']['value']
                if account_info:
                    balance_lamports = account_info.get('lamports', 0)
                    return {
                        'pubkey': pubkey,
                        'balance': balance_lamports / 10**9,  # Lamports zu SOL
                        'balance_lamports': balance_lamports,
                        'owner': account_info.get('owner'),
                        'executable': account_info.get('executable', False),
                        'rent_epoch': account_info.get('rentEpoch'),
                        'last_updated': datetime.now()
                    }
        except Exception as e:
            logger.error(f"Error fetching Solana account balance: {e}")
        
        return None
    
    async def get_account_transactions(self, pubkey: str, limit: int = 25) -> Optional[List[Dict[str, Any]]]:
        """Holt Transaktionen für eine Solana-Adresse"""
        try:
            url = self.base_url
            params = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': 'getSignaturesForAddress',
                'params': [
                    pubkey,
                    {'limit': limit}
                ]
            }
            
            data = await self._make_post_request(url, params)
            
            if data and data.get('result'):
                signatures = data['result']
                transactions = []
                
                for sig_info in signatures:
                    tx_details = await self.get_transaction_details(sig_info['signature'])
                    if tx_details:
                        transactions.append(tx_details)
                
                return transactions
        except Exception as e:
            logger.error(f"Error fetching Solana account transactions: {e}")
        
        return None
    
    async def get_transaction_details(self, signature: str) -> Optional[Dict[str, Any]]:
        """Holt Details zu einer spezifischen Transaktion"""
        try:
            url = self.base_url
            params = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': 'getTransaction',
                'params': [
                    signature,
                    {'encoding': 'jsonParsed'}
                ]
            }
            
            data = await self._make_post_request(url, params)
            
            if data and data.get('result'):
                tx = data['result']
                return {
                    'signature': tx.get('signature'),
                    'slot': tx.get('slot'),
                    'block_time': datetime.fromtimestamp(tx.get('blockTime')) if tx.get('blockTime') else None,
                    'fee': tx.get('meta', {}).get('fee', 0) / 10**9,
                    'status': tx.get('meta', {}).get('err') is None,
                    'account_keys': tx.get('transaction', {}).get('message', {}).get('accountKeys', []),
                    'instructions': tx.get('transaction', {}).get('message', {}).get('instructions', []),
                    'pre_balances': [bal / 10**9 for bal in tx.get('meta', {}).get('preBalances', [])],
                    'post_balances': [bal / 10**9 for bal in tx.get('meta', {}).get('postBalances', [])],
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Solana transaction details: {e}")
        
        return None
    
    async def get_token_accounts_by_owner(self, owner: str) -> Optional[List[Dict[str, Any]]]:
        """Holt alle Token-Konten eines Besitzers"""
        try:
            url = self.base_url
            params = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': 'getTokenAccountsByOwner',
                'params': [
                    owner,
                    {'programId': 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'},
                    {'encoding': 'jsonParsed'}
                ]
            }
            
            data = await self._make_post_request(url, params)
            
            if data and data.get('result'):
                accounts = data['result']['value']
                token_accounts = []
                
                for account in accounts:
                    account_info = account['account']['data']['parsed']['info']
                    token_accounts.append({
                        'pubkey': account['pubkey'],
                        'mint': account_info['mint'],
                        'owner': account_info['owner'],
                        'token_amount': account_info['tokenAmount'],
                        'last_updated': datetime.now()
                    })
                
                return token_accounts
        except Exception as e:
            logger.error(f"Error fetching Solana token accounts: {e}")
        
        return None
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Solana-spezifische Token-Preisabfrage"""
        try:
            # Solana hat keine native Token-Preis-API, daher leere Implementierung
            # In der Praxis würde man hier eine externe API wie Jupiter oder Raydium verwenden
            return None
        except Exception as e:
            logger.error(f"Error fetching Solana token price: {e}")
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_second": 10, "requests_per_minute": 600}
