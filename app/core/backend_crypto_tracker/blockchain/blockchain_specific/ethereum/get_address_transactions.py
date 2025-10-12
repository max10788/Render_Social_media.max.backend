from typing import Any, Dict, Optional, List
from app.core.backend_crypto_tracker.utils.logger import get_logger

try:
    from solders.publickey import PublicKey
except ImportError:
    from solana.publickey import PublicKey

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
        
        # ✅ Konvertiere String zu PublicKey
        try:
            pubkey = PublicKey(address)
            logger.debug(f"PublicKey konvertiert: {pubkey}")
        except Exception as e:
            logger.error(f"Invalid PublicKey: {address}, Error: {str(e)}")
            return None
        
        # ✅ Nutze die offizielle solana-py Methode mit PublicKey
        response = await provider.get_signatures_for_address(pubkey, limit=limit)
        
        if response and response.get('result'):
            signatures = response['result']
            
            # Konvertiere zu Standard-Format
            formatted_sigs = []
            for sig_obj in signatures:
                formatted_sigs.append({
                    'signature': sig_obj.get('signature', ''),
                    'slot': sig_obj.get('slot'),
                    'err': sig_obj.get('err'),
                    'blockTime': sig_obj.get('blockTime'),
                    'confirmationStatus': sig_obj.get('confirmationStatus')
                })
            
            logger.info(f"✅ Gefunden {len(formatted_sigs)} Signaturen für {address}")
            return formatted_sigs
        else:
            logger.info(f"ℹ️  Keine Signaturen für Adresse {address} gefunden")
            return []
            
    except Exception as e:
        logger.error(f"Error fetching Solana confirmed signatures: {str(e)}", exc_info=True)
        return None
