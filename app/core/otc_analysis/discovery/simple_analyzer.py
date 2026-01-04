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
                logger.warning(f"⚠️ No transactions found")
                return []
            
            recent_txs = sorted(
                transactions,
                key=lambda x: x.get('timestamp', datetime.min),
                reverse=True
            )[:num_transactions]
            
            logger.info(f"📊 Extracted {len(recent_txs)} recent transactions")
            
            counterparties = []
            
            for i, tx in enumerate(recent_txs, 1):
                from_addr = str(tx.get('from_address', '')).lower().strip()
                to_addr = str(tx.get('to_address', '')).lower().strip()
                
                logger.info(f"   🔎 TX {i}:")
                logger.info(f"      From: {from_addr[:42] if from_addr else 'EMPTY'}")
                logger.info(f"      To:   {to_addr[:42] if to_addr else 'EMPTY'}")
                logger.info(f"      OTC:  {otc_normalized[:42]}")
                
                # Finde Counterparty
                counterparty = None
                direction = None
                
                if from_addr == otc_normalized and to_addr:
                    counterparty = to_addr
                    direction = "sent_to"
                    logger.info(f"      ✅ MATCH: OTC sent to {to_addr[:10]}...")
                elif to_addr == otc_normalized and from_addr:
                    counterparty = from_addr
                    direction = "received_from"
                    logger.info(f"      ✅ MATCH: OTC received from {from_addr[:10]}...")
                else:
                    logger.warning(f"      ❌ NO MATCH (neither from nor to equals OTC)")
                    continue
                
                if not counterparty:
                    logger.warning(f"      ⚠️ Empty counterparty")
                    continue
                
                # TX Info
                tx_info = {
                    'tx_hash': tx.get('tx_hash', ''),
                    'timestamp': tx.get('timestamp'),
                    'from': from_addr,
                    'to': to_addr,
                    'counterparty': counterparty,
                    'direction': direction,
                    'value_eth': tx.get('value_decimal', 0),
                    'value_usd': tx.get('usd_value', 0),
                    'token_symbol': tx.get('token_symbol', 'ETH'),
                    'token_address': tx.get('token_address', None),
                    'block_number': tx.get('block_number', 0)  # ✅ ADD
                }
                
                counterparties.append(tx_info)
                
                logger.info(
                    f"      📝 Saved: {direction.upper()} {counterparty[:10]}... "
                    f"({tx_info['token_symbol']})"
                )
            
            logger.info(f"📋 Total counterparties extracted: {len(counterparties)}")
            
            # Dedupliziere
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
            
            logger.info(f"✅ Found {len(unique_counterparties)} UNIQUE counterparties:")
            for addr in unique_counterparties.keys():
                logger.info(f"   • {addr[:10]}... ({len(unique_counterparties[addr]['transactions'])} TXs)")
            
            return list(unique_counterparties.values())
            
        except Exception as e:
            logger.error(f"❌ Error: {e}", exc_info=True)
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
                labels={}
            )
            
            # 4. Berechne OTC Probability
            otc_probability = self.wallet_profiler.calculate_otc_probability(profile)
            confidence = otc_probability * 100
            
            # ✅ NEW: Store transactions in result
            result = {
                'address': counterparty_address,
                'confidence': confidence,
                'total_volume': profile.get('total_volume_usd', 0),
                'transaction_count': len(transactions),
                'avg_transaction': profile.get('avg_transaction_usd', 0),
                'first_seen': profile.get('first_transaction'),
                'last_seen': profile.get('last_transaction'),
                'profile': profile,
                'transactions': transactions[:100]  # ✅ ADD: Top 100 transactions
            }
            
            logger.info(
                f"✅ Analysis complete: {confidence:.1f}% OTC probability, "
                f"${result['total_volume']:,.0f} volume"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error analyzing {counterparty_address[:10]}: {e}", exc_info=True)
            return None
    
    # ✅ NEW METHOD: Save transactions to database
    def save_transactions_to_db(self, transactions: List[Dict], session) -> int:
        """
        Save discovered transactions to database.
        
        Args:
            transactions: List of transaction dicts
            session: SQLAlchemy session
            
        Returns:
            Number of transactions saved
        """
        from app.core.otc_analysis.models.transaction import OTCTransaction
        
        saved_count = 0
        
        try:
            for tx in transactions:
                # Check if transaction already exists
                existing = session.query(OTCTransaction).filter_by(
                    tx_hash=tx.get('tx_hash')
                ).first()
                
                if existing:
                    continue
                
                # Create new transaction
                transaction = OTCTransaction(
                    tx_hash=tx.get('tx_hash', ''),
                    from_address=tx.get('from_address', '').lower(),
                    to_address=tx.get('to_address', '').lower(),
                    token_symbol=tx.get('token_symbol', 'ETH'),
                    token_address=tx.get('token_address'),
                    amount=float(tx.get('amount', 0) or 0),
                    usd_value=float(tx.get('usd_value', 0) or 0),
                    timestamp=tx.get('timestamp'),
                    block_number=int(tx.get('block_number', 0) or 0)
                )
                
                session.add(transaction)
                saved_count += 1
            
            session.commit()
            logger.info(f"💾 Saved {saved_count} transactions to database")
            
            return saved_count
            
        except Exception as e:
            logger.error(f"❌ Error saving transactions: {e}", exc_info=True)
            session.rollback()
            return 0
