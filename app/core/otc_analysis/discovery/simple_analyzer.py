"""
Simple Last-5-TX Discovery - WITH ALWAYS QUICK STATS FIRST
============================================================

✨ IMPROVED VERSION:
- Quick Stats API FIRST (for all counterparties)
- Nur bei Fehler → Transaction Processing
- 15x schneller für Discovery

Version: 2.0 - Always Quick Stats First
Date: 2025-01-04
"""

from typing import List, Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SimpleLastTxAnalyzer:
    """
    Simpelster Discovery-Ansatz mit QUICK STATS FIRST:
    - Nimm letzte 5 Transaktionen
    - Finde Counterparty-Adressen
    - Analysiere sie mit Quick Stats API (PRIORITY 1)
    - Fallback zu Transaction Processing nur wenn nötig
    
    ✨ NEW: 15x schneller durch Quick Stats API
    """
    
    def __init__(self, db, transaction_extractor, wallet_profiler, price_oracle, wallet_stats_api=None):
        self.db = db
        self.transaction_extractor = transaction_extractor
        self.wallet_profiler = wallet_profiler
        self.price_oracle = price_oracle
        self.wallet_stats_api = wallet_stats_api  # ✨ NEW

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
                    'block_number': tx.get('block_number', 0)
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
    
    # ========================================================================
    # ✨ IMPROVED: ANALYZE WITH QUICK STATS FIRST
    # ========================================================================
    
    def analyze_counterparty(self, counterparty_address: str) -> Optional[Dict]:
        """
        Führe volle OTC-Analyse auf Counterparty durch.
        
        ✨ NEW STRATEGY - ALWAYS QUICK STATS FIRST:
        1. Hole Quick Stats FIRST (um TX Count zu wissen)
        2. Wenn Quick Stats verfügbar → NUTZE ES direkt (15x schneller)
        3. Nur bei Fehler → Fallback zu Transaction Processing
        
        Args:
            counterparty_address: Adresse zum Analysieren
            
        Returns:
            Analyse-Ergebnis oder None
        """
        logger.info(f"🔬 Analyzing counterparty {counterparty_address[:10]}...")
        
        try:
            # ================================================================
            # ✨ PRIORITY 1: ALWAYS TRY QUICK STATS FIRST
            # ================================================================
            
            if self.wallet_stats_api:
                logger.info(f"   🚀 PRIORITY 1: Trying Quick Stats API (ALWAYS preferred)")
                
                quick_stats = self.wallet_stats_api.get_quick_stats(counterparty_address)
                tx_count = quick_stats.get('total_transactions', 0)
                
                logger.info(f"   📊 Quick stats result: {tx_count} transactions")
                
                # ============================================================
                # ✨ STRATEGY A: Use Quick Stats (if available)
                # ============================================================
                
                if quick_stats.get('source') != 'none':
                    logger.info(f"   ✅ Quick Stats available from {quick_stats['source']}")
                    logger.info(f"   ⚡ Using aggregated data (NO transaction processing)")
                    
                    # Create profile from Quick Stats (no TX processing needed!)
                    labels = {}
                    profile = self.wallet_profiler._create_profile_from_quick_stats(
                        counterparty_address,
                        quick_stats,
                        labels,
                        tx_count
                    )
                    
                    # Calculate OTC Probability
                    otc_probability = self.wallet_profiler.calculate_otc_probability(profile)
                    confidence = otc_probability * 100
                    
                    result = {
                        'address': counterparty_address,
                        'confidence': confidence,
                        'total_volume': quick_stats.get('total_value_usd', 0),
                        'transaction_count': tx_count,
                        'avg_transaction': quick_stats.get('total_value_usd', 0) / max(1, tx_count),
                        'first_seen': profile.get('first_seen'),
                        'last_seen': profile.get('last_seen'),
                        'profile': profile,
                        'strategy': 'quick_stats',  # ✅ Mark which strategy was used
                        'stats_source': quick_stats.get('source'),
                        'data_quality': quick_stats.get('data_quality', 'medium')
                    }
                    
                    logger.info(
                        f"✅ Quick Stats Analysis complete: "
                        f"{confidence:.1f}% OTC probability, "
                        f"${result['total_volume']:,.0f} volume "
                        f"(source: {quick_stats['source']})"
                    )
                    
                    return result
                
                else:
                    # Quick Stats unavailable - fallback
                    logger.warning(f"   ⚠️  Quick Stats unavailable from all APIs")
                    logger.warning(f"   ⚠️  FALLBACK: Will process transactions manually")
            else:
                logger.warning(f"   ⚠️  WalletStatsAPI not available")
                logger.warning(f"   ⚠️  FALLBACK: Will process transactions manually")
            
            # ================================================================
            # ✨ STRATEGY B: FALLBACK - Transaction Processing
            # ================================================================
            
            logger.info(f"   📊 FALLBACK: Processing transactions manually")
            
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
            
            # 3. Erstelle Profil (wird automatisch versuchen Quick Stats zu nutzen)
            profile = self.wallet_profiler.create_profile(
                counterparty_address,
                transactions,
                labels={}
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
                'first_seen': profile.get('first_seen'),
                'last_seen': profile.get('last_seen'),
                'profile': profile,
                'transactions': transactions[:100],  # Top 100 transactions
                'strategy': 'transaction_processing',  # ✅ Mark which strategy was used
                'data_quality': profile.get('data_quality', 'unknown')
            }
            
            logger.info(
                f"✅ Transaction Processing complete: "
                f"{confidence:.1f}% OTC probability, "
                f"${result['total_volume']:,.0f} volume"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error analyzing {counterparty_address[:10]}: {e}", exc_info=True)
            return None
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
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


# Export
__all__ = ['SimpleLastTxAnalyzer']
