from datetime import datetime
from typing import Any, Dict, Optional
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_transaction_details(provider, signature: str) -> Optional[Dict[str, Any]]:
    """
    ✅ Holt Details zu einer Solana-Transaktion über RPC
    
    Args:
        provider: Solana RPC Client (SolanaClient)
        signature: Transaktions-Signatur
    
    Returns:
        Dictionary mit Transaktionsdetails oder None
    """
    try:
        if not provider or not signature:
            logger.error(f"Invalid provider or signature")
            return None
        
        logger.debug(f"Fetching Solana transaction: {signature}")
        
        # ✅ Nutze die offizielle solana-py Methode
        response = await provider.get_transaction(signature, encoding='json')
        
        if not response or not response.get('result'):
            logger.debug(f"No transaction found for {signature}")
            return None
        
        tx = response['result']
        
        # ✅ Strukturiere Transaction mit Standard-Feldern
        transaction_obj = {
            'signature': signature,
            'hash': signature,
            'tx_hash': signature,
            'slot': tx.get('slot'),
            'block_time': int(tx.get('blockTime', 0)) if tx.get('blockTime') else None,
            'timestamp': int(tx.get('blockTime', 0)) if tx.get('blockTime') else None,
            'transaction': tx.get('transaction'),
            'meta': tx.get('meta'),
            'err': tx.get('transaction', {}).get('message') if tx.get('transaction') else None,
            'last_updated': datetime.now().isoformat(),
            'inputs': [],
            'outputs': []
        }
        
        # ✅ Extrahiere Inputs und Outputs wenn verfügbar
        if 'meta' in tx and tx['meta']:
            meta = tx['meta']
            
            # Pre-token-balances als Inputs interpretieren
            if 'preTokenBalances' in meta and meta['preTokenBalances']:
                for idx, balance in enumerate(meta['preTokenBalances']):
                    transaction_obj['inputs'].append({
                        'index': idx,
                        'account': balance.get('owner', ''),
                        'amount': float(balance.get('uiTokenAmount', {}).get('amount', 0)) if balance.get('uiTokenAmount') else 0,
                        'token_mint': balance.get('mint')
                    })
            
            # Post-token-balances als Outputs interpretieren
            if 'postTokenBalances' in meta and meta['postTokenBalances']:
                for idx, balance in enumerate(meta['postTokenBalances']):
                    transaction_obj['outputs'].append({
                        'index': idx,
                        'account': balance.get('owner', ''),
                        'amount': float(balance.get('uiTokenAmount', {}).get('amount', 0)) if balance.get('uiTokenAmount') else 0,
                        'token_mint': balance.get('mint')
                    })
        
        # ✅ Fallback: Wenn keine Token-Balances, nutze Account-Keys
        if not transaction_obj['inputs'] or not transaction_obj['outputs']:
            if 'transaction' in tx and 'message' in tx['transaction']:
                msg = tx['transaction']['message']
                
                # Nutze Account-Keys als Input
                if 'accountKeys' in msg:
                    account_keys = msg.get('accountKeys', [])
                    for idx, key in enumerate(account_keys):
                        if isinstance(key, dict):
                            transaction_obj['inputs'].append({
                                'index': idx,
                                'account': key.get('pubkey', ''),
                                'is_signer': key.get('signer', False),
                                'is_writable': key.get('writable', False)
                            })
                        else:
                            transaction_obj['inputs'].append({
                                'index': idx,
                                'account': str(key),
                                'is_signer': False,
                                'is_writable': False
                            })
                    
                    # Output = erste Writable Account
                    for idx, key in enumerate(account_keys):
                        is_writable = False
                        pubkey = str(key)
                        
                        if isinstance(key, dict):
                            is_writable = key.get('writable', False)
                            pubkey = key.get('pubkey', '')
                        
                        if is_writable:
                            transaction_obj['outputs'].append({
                                'index': idx,
                                'account': pubkey,
                                'is_writable': True
                            })
                            break
        
        # ✅ Stelle sicher dass mindestens eine Output existiert
        if not transaction_obj['outputs'] and transaction_obj['inputs']:
            transaction_obj['outputs'] = [
                {
                    'index': 0,
                    'account': transaction_obj['inputs'][0].get('account', ''),
                    'amount': 0
                }
            ]
        elif not transaction_obj['outputs']:
            transaction_obj['outputs'] = [
                {
                    'index': 0,
                    'account': signature[:20],
                    'amount': 0
                }
            ]
        
        logger.debug(f"Transaction {signature}: {len(transaction_obj['inputs'])} inputs, {len(transaction_obj['outputs'])} outputs")
        return transaction_obj
            
    except Exception as e:
        logger.error(f"Error fetching Solana transaction {signature}: {str(e)}", exc_info=True)
        return None
