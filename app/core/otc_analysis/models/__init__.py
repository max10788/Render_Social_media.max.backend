#!/usr/bin/env python3
"""
OTC Analysis Database Initialization Script

Erstellt alle Tabellen und fügt Sample-Daten hinzu.

Usage:
    python scripts/init_otc_db.py
    
    # Oder mit Python-Modul:
    python -m scripts.init_otc_db
"""

import sys
import os
from datetime import datetime, timedelta

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.core.backend_crypto_tracker.config.database import init_db, check_connection, SessionLocal, database_config
from app.core.otc_analysis.models.wallet import Wallet
from app.core.otc_analysis.models.watchlist import WatchlistItem
from app.core.otc_analysis.models.alert import Alert

def add_sample_wallets():
    """Fügt Sample Wallet-Daten hinzu"""
    print("\n📦 Füge Sample Wallet-Daten hinzu...")
    
    db = SessionLocal()
    
    try:
        # Check if data already exists
        existing = db.query(Wallet).first()
        if existing:
            print("⚠️  Sample Wallets existieren bereits, überspringe...")
            return
        
        # Create sample wallets
        wallets = [
            Wallet(
                address='0x1234567890abcdef1234567890abcdef12345678',
                entity_type='otc_desk',
                entity_name='Wintermute Trading',
                total_volume_usd=50000000,
                avg_transaction_usd=1200000,
                median_transaction_usd=950000,
                confidence_score=95,
                otc_probability=0.95,
                total_transactions=234,
                transaction_frequency=2.5,
                unique_counterparties=45,
                counterparty_entropy=3.2,
                has_defi_interactions=False,
                has_dex_swaps=False,
                betweenness_centrality=0.85,
                degree_centrality=0.75,
                clustering_coefficient=0.25,
                is_known_otc_desk=True,
                active_hours=[9, 10, 11, 12, 13, 14, 15, 16, 17],
                active_days=[0, 1, 2, 3, 4],  # Mon-Fri
                weekend_activity_ratio=0.1,
                labels=['OTC', 'Market Maker'],
                first_seen=datetime(2023, 1, 15),
                last_seen=datetime.utcnow(),
                chain_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            ),
            Wallet(
                address='0xabcdef1234567890abcdef1234567890abcdef12',
                entity_type='exchange',
                entity_name='Binance Hot Wallet',
                total_volume_usd=200000000,
                avg_transaction_usd=800000,
                median_transaction_usd=650000,
                confidence_score=88,
                otc_probability=0.65,
                total_transactions=1234,
                transaction_frequency=12.5,
                unique_counterparties=890,
                counterparty_entropy=5.8,
                has_defi_interactions=True,
                has_dex_swaps=False,
                betweenness_centrality=0.92,
                degree_centrality=0.88,
                clustering_coefficient=0.45,
                is_known_otc_desk=False,
                active_hours=list(range(24)),  # 24/7
                active_days=[0, 1, 2, 3, 4, 5, 6],
                weekend_activity_ratio=0.45,
                labels=['Exchange', 'CEX'],
                first_seen=datetime(2022, 6, 1),
                last_seen=datetime.utcnow(),
                chain_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            ),
            Wallet(
                address='0x9876543210fedcba9876543210fedcba98765432',
                entity_type='otc_desk',
                entity_name='Jump Trading',
                total_volume_usd=35000000,
                avg_transaction_usd=950000,
                median_transaction_usd=800000,
                confidence_score=92,
                otc_probability=0.92,
                total_transactions=156,
                transaction_frequency=1.8,
                unique_counterparties=32,
                counterparty_entropy=2.9,
                has_defi_interactions=False,
                has_dex_swaps=False,
                betweenness_centrality=0.78,
                degree_centrality=0.68,
                clustering_coefficient=0.22,
                is_known_otc_desk=True,
                active_hours=[8, 9, 10, 11, 12, 13, 14, 15, 16],
                active_days=[0, 1, 2, 3, 4],
                weekend_activity_ratio=0.05,
                labels=['OTC', 'HFT'],
                first_seen=datetime(2023, 3, 10),
                last_seen=datetime.utcnow() - timedelta(days=2),
                chain_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            ),
            Wallet(
                address='0xfedcba0987654321fedcba0987654321fedcba09',
                entity_type='institutional',
                entity_name='Three Arrows Capital',
                total_volume_usd=80000000,
                avg_transaction_usd=2100000,
                median_transaction_usd=1800000,
                confidence_score=85,
                otc_probability=0.78,
                total_transactions=567,
                transaction_frequency=3.2,
                unique_counterparties=128,
                counterparty_entropy=4.1,
                has_defi_interactions=True,
                has_dex_swaps=True,
                betweenness_centrality=0.65,
                degree_centrality=0.72,
                clustering_coefficient=0.38,
                is_known_otc_desk=False,
                active_hours=[6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
                active_days=[0, 1, 2, 3, 4, 5, 6],
                weekend_activity_ratio=0.25,
                labels=['Institutional', 'Fund'],
                first_seen=datetime(2021, 8, 15),
                last_seen=datetime.utcnow() - timedelta(days=180),
                chain_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            ),
            # Mehr diverse Wallets
            Wallet(
                address='0x1111111111111111111111111111111111111111',
                entity_type='otc_desk',
                entity_name='Cumberland DRW',
                total_volume_usd=42000000,
                avg_transaction_usd=1100000,
                median_transaction_usd=900000,
                confidence_score=93,
                otc_probability=0.93,
                total_transactions=189,
                transaction_frequency=2.1,
                unique_counterparties=38,
                counterparty_entropy=3.1,
                has_defi_interactions=False,
                has_dex_swaps=False,
                betweenness_centrality=0.82,
                degree_centrality=0.71,
                clustering_coefficient=0.24,
                is_known_otc_desk=True,
                active_hours=[8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
                active_days=[0, 1, 2, 3, 4],
                weekend_activity_ratio=0.08,
                labels=['OTC', 'Liquidity Provider'],
                first_seen=datetime(2022, 11, 20),
                last_seen=datetime.utcnow() - timedelta(hours=6),
                chain_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
        ]
        
        # Add to database
        for wallet in wallets:
            db.add(wallet)
        
        db.commit()
        
        print(f"✅ {len(wallets)} Sample Wallets hinzugefügt")
        print("\n📊 Sample Wallets:")
        for w in wallets:
            print(f"   • {w.entity_name}: ${w.total_volume_usd:,.0f} ({w.entity_type})")
        
    except Exception as e:
        print(f"❌ Fehler beim Hinzufügen von Sample-Daten: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def add_sample_alerts():
    """Fügt Sample Alert-Daten hinzu"""
    print("\n🔔 Füge Sample Alert-Daten hinzu...")
    
    db = SessionLocal()
    
    try:
        # Check if data already exists
        existing = db.query(Alert).first()
        if existing:
            print("⚠️  Sample Alerts existieren bereits, überspringe...")
            return
        
        alerts = [
            Alert(
                alert_type='new_large_transfer',
                severity='high',
                tx_hash='0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef',
                from_address='0x1234567890abcdef1234567890abcdef12345678',
                to_address='0xabcdef1234567890abcdef1234567890abcdef12',
                usd_value=5000000,
                confidence_score=95,
                alert_metadata={'token': 'USDT', 'from_label': 'Wintermute Trading', 'to_label': 'Binance'},
                created_at=datetime.utcnow() - timedelta(hours=2)
            ),
            Alert(
                alert_type='desk_interaction',
                severity='medium',
                tx_hash='0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
                from_address='0x9876543210fedcba9876543210fedcba98765432',
                to_address='0x1111111111111111111111111111111111111111',
                usd_value=2500000,
                confidence_score=92,
                alert_metadata={'token': 'USDC', 'from_label': 'Jump Trading', 'to_label': 'Cumberland DRW'},
                created_at=datetime.utcnow() - timedelta(hours=8)
            )
        ]
        
        for alert in alerts:
            db.add(alert)
        
        db.commit()
        
        print(f"✅ {len(alerts)} Sample Alerts hinzugefügt")
        
    except Exception as e:
        print(f"❌ Fehler beim Hinzufügen von Sample Alerts: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def main():
    print("=" * 80)
    print("🚀 OTC Analysis Database Initialization")
    print("=" * 80)
    
    # Check database connection
    print("\n🔍 Prüfe Datenbankverbindung...")
    if not check_connection():
        print("❌ Datenbankverbindung fehlgeschlagen!")
        print(f"📋 DATABASE_URL: {database_config.database_url[:50]}...")
        sys.exit(1)
    
    print(f"✅ Verbunden mit: {database_config.db_host}:{database_config.db_port}/{database_config.db_name}")
    print(f"📦 Schema: {database_config.schema_name}")
    
    # Initialize database
    try:
        init_db()
    except Exception as e:
        print(f"❌ Fehler bei Datenbank-Initialisierung: {e}")
        sys.exit(1)
    
    # Add sample data
    try:
        add_sample_wallets()
        add_sample_alerts()
    except Exception as e:
        print(f"❌ Fehler beim Hinzufügen von Sample-Daten: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 80)
    print("✅ Datenbank-Initialisierung abgeschlossen!")
    print("=" * 80)
    
    print("\n📊 Nächste Schritte:")
    print("   1. Backend starten: uvicorn main:app --reload")
    print("   2. API testen: curl http://localhost:8000/api/otc/health")
    print("   3. Statistics: curl 'http://localhost:8000/api/otc/statistics?from_date=2023-01-01&to_date=2025-12-31'")

if __name__ == "__main__":
    main()
