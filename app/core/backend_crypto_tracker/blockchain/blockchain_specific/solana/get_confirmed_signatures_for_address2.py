from typing import Any, Dict, Optional, List
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_confirmed_signatures_for_address2(
    provider, 
    address: str, 
    limit: int = 25
) -> Optional[List[Dict[str, Any]]]:
    """
    ✅ Holt die letzten bestätigten Signaturen für eine Solana-Adresse
    
    Args:
        provider: Solana RPC Client (SolanaClient)
        address: Solana Wallet-Adresse (string)
        limit: Maximale Anzahl von Signaturen (default 25)
    
    Returns:
        Liste von Signaturen oder None bei Fehler
    """
    try:
        if not provider:
            raise Exception("Provider ist None")
        
        if not address:
            raise Exception("Address ist leer")
        
        logger.info(f"Rufe Solana Signaturen für {address} ab (limit={limit})")
        
        # ✅ Konvertiere String zu PublicKey mit try/except
        try:
            from solders.pubkey import Pubkey

            pubkey = Pubkey.from_string(address)
            logger.debug(f"PublicKey konvertiert: {pubkey}")
        except Exception as e:
            logger.error(f"Invalid PublicKey: {address}, Error: {str(e)}")
            return None
        
        # ✅ Nutze die offizielle solana-py Methode mit PublicKey
        import inspect as inspect_module
        result = provider.get_signatures_for_address(pubkey, limit=limit)
        if inspect_module.isawaitable(result):
            response = await result
        else:
            response = result

        # Handle both old dict-style and new typed responses
        if hasattr(response, 'value'):
            # Newer solana-py returns typed objects
            signatures = response.value
        elif isinstance(response, dict) and response.get('result'):
            signatures = response['result']
        else:
            signatures = None

        if signatures:
            
            # Konvertiere zu Standard-Format (handle both dict and typed objects)
            formatted_sigs = []
            for sig_obj in signatures:
                if isinstance(sig_obj, dict):
                    formatted_sigs.append({
                        'signature': sig_obj.get('signature', ''),
                        'slot': sig_obj.get('slot'),
                        'err': sig_obj.get('err'),
                        'blockTime': sig_obj.get('blockTime'),
                        'confirmationStatus': sig_obj.get('confirmationStatus')
                    })
                else:
                    # Typed object from newer solana-py
                    formatted_sigs.append({
                        'signature': str(sig_obj.signature),
                        'slot': sig_obj.slot,
                        'err': sig_obj.err,
                        'blockTime': getattr(sig_obj, 'block_time', None),
                        'confirmationStatus': getattr(sig_obj, 'confirmation_status', None)
                    })
            
            logger.info(f"Gefunden {len(formatted_sigs)} Signaturen für {address}")
            return formatted_sigs
        else:
            logger.info(f"Keine Signaturen für Adresse {address} gefunden")
            return []
            
    except Exception as e:
        logger.error(f"Error fetching Solana confirmed signatures: {str(e)}", exc_info=True)
        return None
