"""
Simple Last-5-TX Discovery
Analysiert die Counterparties der letzten 5 Transaktionen
"""

from typing import List, Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SimpleLastTxAnalyzer:
    """
    Simpelster Discovery-Ansatz:
    - Nimm letzte 5 Transaktionen
    - Finde Counterparty-Adressen
    - Analysiere sie
    """
    
    def __init__(self, db, transaction_extractor, wallet_profiler, price_oracle):
        self.db = db
        self.transaction_extractor = transaction_extractor
        self.wallet_profiler = wallet_profiler
        self.price_oracle = price_oracle

    def discover_from_last_transactions(
        self,
        otc_address: str,
        num_transactions: int = 5
    ) -> List[Dict]:
        """Analysiere Counterparties der letzten N Transaktionen."""
        
        otc_normalized = otc_address.lower().strip()
        
        logger.info(f"🔍 Analyzing last {num_transactions} transactions of {otc_normalized[:10]}...")
        
        try:
            transactions = self.transaction_extractor.extract_wallet_transactions(
                otc_address,
                include_internal=True,
                include_tokens=True
            )
            
            if not transactions:
                logger.warning(f"⚠️ No transactions found for {otc_normalized[:10]}")
                return []
            
            recent_txs = sorted(
                transactions,
                key=lambda x: x.get('timestamp', datetime.min),
                reverse=True
            )[:num_transactions]
            
            logger.info(f"📊 Found {len(recent_txs)} recent transactions")
            
            counterparties = []
            
            for i, tx in enumerate(recent_txs, 1):
                # ✅ RICHTIGE FELDNAMEN VERWENDEN!
                from_addr = str(tx.get('from_address', '')).lower().strip()
                to_addr = str(tx.get('to_address', '')).lower().strip()
                
                # Finde Counterparty
                if from_addr == otc_normalized:
                    counterparty = to_addr
                    direction = "sent_to"
                elif to_addr == otc_normalized:
                    counterparty = from_addr
                    direction = "received_from"
                else:
                    logger.warning(f"⚠️ TX {i}: Neither from nor to matches OTC address")
                    continue
                
                if not counterparty:
                    continue
                
                # Speichere TX Info
                tx_info = {
                    'tx_hash': tx.get('tx_hash', ''),  # ✅ tx_hash statt hash
                    'timestamp': tx.get('timestamp'),
                    'from': from_addr,
                    'to': to_addr,
                    'counterparty': counterparty,
                    'direction': direction,
                    'value_eth': tx.get('value_decimal', 0),  # ✅ value_decimal statt value
                    'value_usd': tx.get('usd_value', 0),
                    'token_symbol': tx.get('token_symbol', 'ETH'),  # ✅ token_symbol
                    'token_address': tx.get('token_address', None)  # ✅ token_address
                }
                
                counterparties.append(tx_info)
                
                logger.info(
                    f"   TX {i}: {direction.upper()} {counterparty[:10]}... "
                    f"({tx_info['token_symbol']} ${tx_info['value_usd']:,.2f})"
                )
            
            # Dedupliziere Counterparties
            unique_counterparties = {}
            for cp in counterparties:
                addr = cp['counterparty']
                if addr not in unique_counterparties:
                    unique_counterparties[addr] = {
                        'address': addr,
                        'transactions': [],
                        'total_value_usd': 0,
                        'first_seen': cp['timestamp'],
                        'last_seen': cp['timestamp']
                    }
                
                unique_counterparties[addr]['transactions'].append(cp)
                unique_counterparties[addr]['total_value_usd'] += cp['value_usd']
                
                if cp['timestamp'] < unique_counterparties[addr]['first_seen']:
                    unique_counterparties[addr]['first_seen'] = cp['timestamp']
                if cp['timestamp'] > unique_counterparties[addr]['last_seen']:
                    unique_counterparties[addr]['last_seen'] = cp['timestamp']
            
            logger.info(f"✅ Found {len(unique_counterparties)} unique counterparties")
            
            return list(unique_counterparties.values())
            
        except Exception as e:
            logger.error(f"❌ Error discovering from last transactions: {e}", exc_info=True)
            return []
    
    def analyze_counterparty(self, counterparty_address: str) -> Optional[Dict]:
        """
        Führe volle OTC-Analyse auf Counterparty durch.
        
        Args:
            counterparty_address: Adresse zum Analysieren
            
        Returns:
            Analyse-Ergebnis oder None
        """
        logger.info(f"🔬 Analyzing counterparty {counterparty_address[:10]}...")
        
        try:
            # 1. Hole Transaktionen der Counterparty
            transactions = self.transaction_extractor.extract_wallet_transactions(
                counterparty_address,
                include_internal=True,
                include_tokens=True
            )
            
            if not transactions:
                logger.warning(f"⚠️ No transactions for {counterparty_address[:10]}")
                return None
            
            # 2. Enrich mit USD
            transactions = self.transaction_extractor.enrich_with_usd_value(
                transactions,
                self.price_oracle,
                max_transactions=30
            )
            
            # 3. Erstelle Profil
            profile = self.wallet_profiler.create_profile(
                counterparty_address,
                transactions,
                labels={}  # Keine vordefinierten Labels
            )
            
            # 4. Berechne OTC Probability
            otc_probability = self.wallet_profiler.calculate_otc_probability(profile)
            confidence = otc_probability * 100
            
            result = {
                'address': counterparty_address,
                'confidence': confidence,
                'total_volume': profile.get('total_volume_usd', 0),
                'transaction_count': len(transactions),
                'avg_transaction': profile.get('avg_transaction_usd', 0),
                'first_seen': profile.get('first_transaction'),
                'last_seen': profile.get('last_transaction'),
                'profile': profile
            }
            
            logger.info(
                f"✅ Analysis complete: {confidence:.1f}% OTC probability, "
                f"${result['total_volume']:,.0f} volume"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error analyzing {counterparty_address[:10]}: {e}", exc_info=True)
            return None
