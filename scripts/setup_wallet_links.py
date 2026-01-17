"""
Auto-Migration: wallet_links Table
===================================

Creates wallet_links table on app startup if it doesn't exist.
Safe to run multiple times (idempotent).
"""

import logging
from sqlalchemy import text, inspect
from app.core.backend_crypto_tracker.config.database import get_db

logger = logging.getLogger(__name__)

def setup_wallet_links_table():
    """
    ✅ Create wallet_links table and indexes if they don't exist.
    
    Returns:
        dict: Migration result
    """
    try:
        logger.info("🔧 Checking wallet_links table...")
        
        # Get database session
        db = next(get_db())
        
        # Check if table exists
        inspector = inspect(db.bind)
        tables = inspector.get_table_names()
        
        if 'wallet_links' in tables:
            logger.info("✅ wallet_links table already exists - skipping migration")
            return {
                "success": True,
                "table_created": False,
                "message": "Table already exists"
            }
        
        logger.info("📦 Creating wallet_links table...")
        
        # SQL to create table
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS wallet_links (
                -- PRIMARY IDENTIFIERS
                id SERIAL PRIMARY KEY,
                from_address VARCHAR(42) NOT NULL,
                to_address VARCHAR(42) NOT NULL,
                
                -- WALLET METADATA
                from_wallet_type VARCHAR(50),
                to_wallet_type VARCHAR(50),
                from_wallet_label VARCHAR(255),
                to_wallet_label VARCHAR(255),
                
                -- AGGREGATED TRANSACTION DATA
                transaction_count INTEGER DEFAULT 0,
                total_volume_usd DOUBLE PRECISION DEFAULT 0.0,
                avg_transaction_usd DOUBLE PRECISION DEFAULT 0.0,
                min_transaction_usd DOUBLE PRECISION,
                max_transaction_usd DOUBLE PRECISION,
                
                -- TIME WINDOW
                first_transaction TIMESTAMP,
                last_transaction TIMESTAMP,
                analysis_start TIMESTAMP NOT NULL,
                analysis_end TIMESTAMP NOT NULL,
                
                -- LINK CLASSIFICATION & SCORING
                link_strength DOUBLE PRECISION DEFAULT 0.0,
                is_suspected_otc BOOLEAN DEFAULT FALSE,
                otc_confidence DOUBLE PRECISION DEFAULT 0.0,
                volume_score DOUBLE PRECISION DEFAULT 0.0,
                frequency_score DOUBLE PRECISION DEFAULT 0.0,
                recency_score DOUBLE PRECISION DEFAULT 0.0,
                consistency_score DOUBLE PRECISION DEFAULT 0.0,
                
                -- PATTERN DETECTION
                detected_patterns JSONB DEFAULT '[]'::jsonb,
                flow_type VARCHAR(50),
                is_bidirectional BOOLEAN DEFAULT FALSE,
                
                -- DATA SOURCE & QUALITY
                data_source VARCHAR(50) DEFAULT 'transactions',
                data_quality VARCHAR(20) DEFAULT 'high',
                sample_tx_hashes JSONB DEFAULT '[]'::jsonb,
                
                -- NETWORK ANALYSIS
                from_cluster_id VARCHAR(66),
                to_cluster_id VARCHAR(66),
                betweenness_score DOUBLE PRECISION,
                is_critical_path BOOLEAN DEFAULT FALSE,
                
                -- ENRICHMENT DATA
                token_distribution JSONB,
                primary_token VARCHAR(42),
                token_diversity INTEGER DEFAULT 0,
                
                -- MANUAL REVIEW
                manually_verified BOOLEAN DEFAULT FALSE,
                verified_by VARCHAR(100),
                verified_at TIMESTAMP,
                notes JSONB,
                
                -- FLAGS & STATUS
                is_active BOOLEAN DEFAULT TRUE,
                needs_refresh BOOLEAN DEFAULT FALSE,
                alert_triggered BOOLEAN DEFAULT FALSE,
                alert_type VARCHAR(50),
                
                -- METADATA
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_calculated TIMESTAMP
            );
        """)
        
        db.execute(create_table_sql)
        db.commit()
        
        logger.info("✅ wallet_links table created")
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_wl_from_address ON wallet_links (from_address);",
            "CREATE INDEX IF NOT EXISTS idx_wl_to_address ON wallet_links (to_address);",
            "CREATE INDEX IF NOT EXISTS idx_wl_transaction_count ON wallet_links (transaction_count);",
            "CREATE INDEX IF NOT EXISTS idx_wl_total_volume_usd ON wallet_links (total_volume_usd);",
            "CREATE INDEX IF NOT EXISTS idx_wl_link_strength ON wallet_links (link_strength);",
            "CREATE INDEX IF NOT EXISTS idx_wl_is_suspected_otc ON wallet_links (is_suspected_otc);",
            "CREATE INDEX IF NOT EXISTS idx_wl_is_active ON wallet_links (is_active);",
            "CREATE INDEX IF NOT EXISTS idx_wl_created_at ON wallet_links (created_at);",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_link ON wallet_links (from_address, to_address, analysis_start);",
            "CREATE INDEX IF NOT EXISTS idx_volume_strength ON wallet_links (total_volume_usd, link_strength);",
            "CREATE INDEX IF NOT EXISTS idx_otc_links ON wallet_links (is_suspected_otc, otc_confidence);",
            "CREATE INDEX IF NOT EXISTS idx_active_links ON wallet_links (is_active, last_transaction);",
            "CREATE INDEX IF NOT EXISTS idx_wallet_types ON wallet_links (from_wallet_type, to_wallet_type);",
            "CREATE INDEX IF NOT EXISTS idx_clusters ON wallet_links (from_cluster_id, to_cluster_id);",
            "CREATE INDEX IF NOT EXISTS idx_critical_paths ON wallet_links (is_critical_path, betweenness_score);"
        ]
        
        for idx_sql in indexes:
            db.execute(text(idx_sql))
        
        db.commit()
        
        logger.info(f"✅ Created {len(indexes)} indexes")
        
        # Create updated_at trigger
        logger.info("⚙️ Creating updated_at trigger...")
        
        trigger_sql = text("""
            CREATE OR REPLACE FUNCTION update_wallet_links_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS update_wallet_links_updated_at ON wallet_links;
            
            CREATE TRIGGER update_wallet_links_updated_at
            BEFORE UPDATE ON wallet_links
            FOR EACH ROW
            EXECUTE FUNCTION update_wallet_links_timestamp();
        """)
        
        db.execute(trigger_sql)
        db.commit()
        
        logger.info("✅ Trigger created")
        logger.info("=" * 60)
        logger.info("✅ wallet_links migration completed successfully!")
        logger.info("=" * 60)
        
        return {
            "success": True,
            "table_created": True,
            "indexes_created": len(indexes),
            "message": "wallet_links table created with all indexes"
        }
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}", exc_info=True)
        return {
            "success": False,
            "table_created": False,
            "error": str(e),
            "message": "Migration failed - app will continue without saved links"
        }
    finally:
        db.close()


if __name__ == "__main__":
    # Can be run standalone for testing
    logging.basicConfig(level=logging.INFO)
    result = setup_wallet_links_table()
    print(f"\nResult: {result}")
