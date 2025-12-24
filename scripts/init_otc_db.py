"""
Initialize OTC Analysis Database with Sample Data
Path: scripts/init_otc_db.py

This script creates sample OTC wallets in the database.
Can be run manually or automatically on backend startup.
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.otc_analysis.models.wallet import OTCWallet, Base


def get_database_url():
    """Get database URL from environment"""
    # Try different environment variable names
    db_url = (
        os.getenv('DATABASE_URL') or 
        os.getenv('DB_URL') or
        os.getenv('POSTGRES_URL')
    )
    
    if not db_url:
        raise ValueError(
            "DATABASE_URL not found in environment. "
            "Please set DATABASE_URL environment variable."
        )
    
    # Fix Render's postgres:// to postgresql://
    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)
    
    return db_url


def create_sample_wallets(session):
    """
    Create realistic sample OTC wallets
    
    Returns:
        List of created wallet labels
    """
    
    sample_wallets = [
        {
            "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
            "label": "Wintermute Trading",
            "entity_type": "market_maker",
            "confidence_score": 0.95,
            "total_volume": 250_000_000.0,  # $250M
            "transaction_count": 1_245,
            "avg_transaction_size": 200_000.0,
            "unique_counterparties": 87,
            "first_seen": datetime.now() - timedelta(days=365),
            "last_active": datetime.now() - timedelta(hours=3),
            "is_active": True,
            "risk_score": 0.15,
            "tags": ["market_maker", "high_volume", "verified"],
            "notes": "Large institutional market maker with consistent high volume"
        },
        {
            "address": "0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549",
            "label": "Binance 14",
            "entity_type": "cex",
            "confidence_score": 0.98,
            "total_volume": 89_000_000.0,  # $89M
            "transaction_count": 3_421,
            "avg_transaction_size": 26_000.0,
            "unique_counterparties": 234,
            "first_seen": datetime.now() - timedelta(days=500),
            "last_active": datetime.now() - timedelta(minutes=45),
            "is_active": True,
            "risk_score": 0.05,
            "tags": ["cex", "binance", "verified"],
            "notes": "Binance hot wallet - high frequency trading"
        },
        {
            "address": "0x6cc5F688a315f3dC28A7781717a9A798a59fDA7b",
            "label": "Jump Trading",
            "entity_type": "prop_trading",
            "confidence_score": 0.92,
            "total_volume": 45_000_000.0,  # $45M
            "transaction_count": 567,
            "avg_transaction_size": 79_400.0,
            "unique_counterparties": 34,
            "first_seen": datetime.now() - timedelta(days=280),
            "last_active": datetime.now() - timedelta(hours=12),
            "is_active": True,
            "risk_score": 0.20,
            "tags": ["prop_trading", "high_frequency", "algorithmic"],
            "notes": "Proprietary trading firm with algorithmic strategies"
        },
        {
            "address": "0x075e72a5eDf65F0A5f44699c7654C1a76941Ddc8",
            "label": "Cumberland DRW",
            "entity_type": "otc_desk",
            "confidence_score": 0.88,
            "total_volume": 18_000_000.0,  # $18M
            "transaction_count": 234,
            "avg_transaction_size": 76_900.0,
            "unique_counterparties": 45,
            "first_seen": datetime.now() - timedelta(days=420),
            "last_active": datetime.now() - timedelta(days=2),
            "is_active": True,
            "risk_score": 0.18,
            "tags": ["otc_desk", "institutional", "regulated"],
            "notes": "Institutional OTC desk serving large clients"
        },
        {
            "address": "0x9696f59E4d72E237BE84fFD425DCaD154Bf96976",
            "label": "Kraken 7",
            "entity_type": "cex",
            "confidence_score": 0.97,
            "total_volume": 5_000_000.0,  # $5M
            "transaction_count": 892,
            "avg_transaction_size": 5_600.0,
            "unique_counterparties": 156,
            "first_seen": datetime.now() - timedelta(days=600),
            "last_active": datetime.now() - timedelta(hours=8),
            "is_active": True,
            "risk_score": 0.08,
            "tags": ["cex", "kraken", "verified"],
            "notes": "Kraken exchange wallet - retail and institutional"
        }
    ]

    created = []
    skipped = []
    
    for wallet_data in sample_wallets:
        try:
            # Check if wallet already exists
            existing = session.query(OTCWallet).filter(
                OTCWallet.address == wallet_data['address']
            ).first()

            if existing:
                skipped.append(wallet_data['label'])
                continue

            wallet = OTCWallet(**wallet_data)
            session.add(wallet)
            created.append(wallet_data['label'])
            
        except Exception as e:
            print(f"⚠️  Error creating {wallet_data['label']}: {e}")
            continue

    if created:
        session.commit()
    
    return created, skipped


def init_database(verbose=True):
    """
    Main initialization function
    
    Args:
        verbose: If True, print detailed output
        
    Returns:
        dict with initialization results
    """
    try:
        if verbose:
            print("=" * 60)
            print("🚀 OTC Analysis Database Initialization")
            print("=" * 60)
        
        # Get database URL
        db_url = get_database_url()
        
        if verbose:
            # Mask password in output
            masked_url = db_url
            if '@' in db_url and ':' in db_url:
                parts = db_url.split('@')
                creds = parts[0].split('//')[-1]
                if ':' in creds:
                    user = creds.split(':')[0]
                    masked_url = db_url.replace(creds, f"{user}:***")
            print(f"\n📊 Database: {masked_url}\n")
        
        # Create engine
        engine = create_engine(db_url)
        
        # Create tables if they don't exist
        if verbose:
            print("📋 Creating tables...")
        Base.metadata.create_all(engine)
        if verbose:
            print("✅ Tables created/verified\n")
        
        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Check if data already exists
        existing_count = session.query(OTCWallet).count()
        
        if existing_count >= 5:
            if verbose:
                print(f"ℹ️  Database already has {existing_count} wallets")
                print("✅ Initialization not needed")
            session.close()
            return {
                "success": True,
                "created": 0,
                "existing": existing_count,
                "message": "Database already initialized"
            }
        
        # Create sample wallets
        if verbose:
            print("💼 Creating sample wallets...")
        created, skipped = create_sample_wallets(session)
        
        # Query final stats
        total_wallets = session.query(OTCWallet).count()
        all_wallets = session.query(OTCWallet).all()
        total_volume = sum(w.total_volume or 0 for w in all_wallets)
        
        session.close()
        
        # Print summary
        if verbose:
            print("\n" + "=" * 60)
            if created:
                print(f"✅ Successfully created {len(created)} sample wallets:")
                for label in created:
                    print(f"   - {label}")
            if skipped:
                print(f"\nℹ️  Skipped {len(skipped)} existing wallets:")
                for label in skipped:
                    print(f"   - {label}")
            
            print(f"\n📈 Database Statistics:")
            print(f"   Total Wallets: {total_wallets}")
            print(f"   Total Volume: ${total_volume:,.0f}")
            print("=" * 60)
            print("\n🎉 Initialization complete!")
        
        return {
            "success": True,
            "created": len(created),
            "skipped": len(skipped),
            "total_wallets": total_wallets,
            "total_volume": total_volume,
            "message": f"Created {len(created)} wallets"
        }
        
    except Exception as e:
        if verbose:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "message": f"Initialization failed: {e}"
        }


def main():
    """CLI entry point"""
    result = init_database(verbose=True)
    
    if result["success"]:
        print("\n💡 Next steps:")
        print("   1. Deploy your backend to Render")
        print("   2. The database will be auto-initialized on first startup")
        print("   3. Check the dashboard - you should see data!")
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
