"""
Automatic Database Setup Script
================================
Erstellt automatisch die transactions Tabelle wenn sie nicht existiert.

Usage:
    1. Beim App-Start automatisch aufrufen
    2. Manuell über Admin-Endpoint
"""

import logging
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import ProgrammingError
import os

logger = logging.getLogger(__name__)


def get_database_url():
    """Holt die Database URL aus verschiedenen Quellen."""
    # Probiere verschiedene Environment-Variablen
    db_url = (
        os.getenv('DATABASE_URL') or
        os.getenv('DB_URL') or
        os.getenv('SQLALCHEMY_DATABASE_URI')
    )
    
    if not db_url:
        logger.warning("No DATABASE_URL found, using default localhost")
        db_url = "postgresql://postgres:postgres@localhost:5432/otc_analysis"
    
    return db_url


def table_exists(engine, table_name: str) -> bool:
    """Prüft ob eine Tabelle existiert."""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def create_transactions_table(engine) -> dict:
    """
    Erstellt die transactions Tabelle mit allen Indexes.
    
    Returns:
        dict: Status-Information über die Migration
    """
    result = {
        "success": False,
        "table_existed": False,
        "table_created": False,
        "indexes_created": 0,
        "errors": []
    }
    
    try:
        # Check if table exists
        result["table_existed"] = table_exists(engine, "transactions")
        
        if result["table_existed"]:
            logger.info("✅ Table 'transactions' already exists - skipping creation")
            result["success"] = True
            return result
        
        logger.info("📦 Creating table 'transactions'...")
        
        # Create table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS transactions (
            -- Primary identifiers
            tx_hash VARCHAR(66) PRIMARY KEY,
            block_number BIGINT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            
            -- Transaction details
            from_address VARCHAR(42) NOT NULL,
            to_address VARCHAR(42) NOT NULL,
            token_address VARCHAR(42),
            
            -- Value information
            value VARCHAR(78) NOT NULL,
            value_decimal DOUBLE PRECISION NOT NULL,
            usd_value DOUBLE PRECISION,
            
            -- Gas information
            gas_used BIGINT,
            gas_price BIGINT,
            
            -- Classification
            is_contract_interaction BOOLEAN DEFAULT FALSE,
            method_id VARCHAR(10),
            
            -- OTC-specific fields
            otc_score DOUBLE PRECISION DEFAULT 0.0,
            is_suspected_otc BOOLEAN DEFAULT FALSE,
            
            -- Metadata
            chain_id INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """
        
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
            result["table_created"] = True
            logger.info("✅ Table created successfully!")
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_block_number ON transactions(block_number)",
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_from_address ON transactions(from_address)",
            "CREATE INDEX IF NOT EXISTS idx_to_address ON transactions(to_address)",
            "CREATE INDEX IF NOT EXISTS idx_token_address ON transactions(token_address)",
            "CREATE INDEX IF NOT EXISTS idx_usd_value ON transactions(usd_value)",
            "CREATE INDEX IF NOT EXISTS idx_otc_score ON transactions(otc_score)",
            "CREATE INDEX IF NOT EXISTS idx_is_suspected_otc ON transactions(is_suspected_otc)",
            "CREATE INDEX IF NOT EXISTS idx_from_timestamp ON transactions(from_address, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_to_timestamp ON transactions(to_address, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_usd_value_desc ON transactions(usd_value DESC)",
            "CREATE INDEX IF NOT EXISTS idx_otc_suspected ON transactions(is_suspected_otc, otc_score)",
        ]
        
        with engine.connect() as conn:
            for idx_sql in indexes:
                try:
                    conn.execute(text(idx_sql))
                    result["indexes_created"] += 1
                except ProgrammingError as e:
                    logger.warning(f"Index creation warning: {e}")
                    result["errors"].append(str(e))
            
            conn.commit()
        
        logger.info(f"✅ Created {result['indexes_created']} indexes")
        
        # Insert test transaction
        logger.info("🧪 Inserting test transaction...")
        test_sql = """
        INSERT INTO transactions (
            tx_hash, block_number, timestamp, from_address, to_address, 
            value, value_decimal, usd_value, otc_score, is_suspected_otc
        ) VALUES (
            '0xtest_startup_migration',
            18000000,
            NOW() - INTERVAL '1 hour',
            '0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13',
            '0xdbf5e9c5206d0db70a90108bf936da60221dc080',
            '1000000000000000000',
            1.0,
            3500.00,
            0.85,
            TRUE
        ) ON CONFLICT (tx_hash) DO NOTHING;
        """
        
        with engine.connect() as conn:
            conn.execute(text(test_sql))
            conn.commit()
        
        logger.info("✅ Test transaction inserted")
        
        result["success"] = True
        logger.info("=" * 60)
        logger.info("✅ DATABASE MIGRATION COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}", exc_info=True)
        result["errors"].append(str(e))
        result["success"] = False
    
    return result


def setup_database_on_startup():
    """
    Hauptfunktion für automatische Migration beim App-Start.
    Wird von FastAPI startup event aufgerufen.
    """
    logger.info("🚀 Starting database setup...")
    
    try:
        db_url = get_database_url()
        logger.info(f"📍 Database URL: {db_url.split('@')[1] if '@' in db_url else 'localhost'}")
        
        engine = create_engine(db_url)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection successful")
        
        # Create table if needed
        result = create_transactions_table(engine)
        
        if result["success"]:
            logger.info("✅ Database setup complete")
        else:
            logger.error(f"❌ Database setup failed: {result['errors']}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Database setup error: {e}", exc_info=True)
        return {
            "success": False,
            "errors": [str(e)]
        }


if __name__ == '__main__':
    # Für manuelles Testen
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    setup_database_on_startup()
