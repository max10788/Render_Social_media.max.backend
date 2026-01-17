-- ============================================================================
-- Migration: Create wallet_links Table
-- ============================================================================
-- Erstellt die wallet_links Tabelle für persistente Wallet-Verbindungen
-- ============================================================================

-- Create wallet_links table (if not exists)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = 'wallet_links'
    ) THEN
        CREATE TABLE wallet_links (
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
        
        RAISE NOTICE '✅ wallet_links table created';
    ELSE
        RAISE NOTICE '⏭️ wallet_links table already exists';
    END IF;
END $$;

-- Create indexes
DO $$ 
BEGIN
    -- Unique constraint: one link per direction per time window
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE tablename = 'wallet_links' AND indexname = 'idx_unique_link'
    ) THEN
        CREATE UNIQUE INDEX idx_unique_link 
        ON wallet_links (from_address, to_address, analysis_start);
        RAISE NOTICE '✅ idx_unique_link created';
    END IF;
    
    -- Basic indexes
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_wl_from_address') THEN
        CREATE INDEX idx_wl_from_address ON wallet_links (from_address);
        RAISE NOTICE '✅ idx_wl_from_address created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_wl_to_address') THEN
        CREATE INDEX idx_wl_to_address ON wallet_links (to_address);
        RAISE NOTICE '✅ idx_wl_to_address created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_wl_transaction_count') THEN
        CREATE INDEX idx_wl_transaction_count ON wallet_links (transaction_count);
        RAISE NOTICE '✅ idx_wl_transaction_count created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_wl_total_volume_usd') THEN
        CREATE INDEX idx_wl_total_volume_usd ON wallet_links (total_volume_usd);
        RAISE NOTICE '✅ idx_wl_total_volume_usd created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_wl_link_strength') THEN
        CREATE INDEX idx_wl_link_strength ON wallet_links (link_strength);
        RAISE NOTICE '✅ idx_wl_link_strength created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_wl_is_suspected_otc') THEN
        CREATE INDEX idx_wl_is_suspected_otc ON wallet_links (is_suspected_otc);
        RAISE NOTICE '✅ idx_wl_is_suspected_otc created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_wl_is_active') THEN
        CREATE INDEX idx_wl_is_active ON wallet_links (is_active);
        RAISE NOTICE '✅ idx_wl_is_active created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_wl_created_at') THEN
        CREATE INDEX idx_wl_created_at ON wallet_links (created_at);
        RAISE NOTICE '✅ idx_wl_created_at created';
    END IF;
    
    -- Composite indexes
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_volume_strength') THEN
        CREATE INDEX idx_volume_strength ON wallet_links (total_volume_usd, link_strength);
        RAISE NOTICE '✅ idx_volume_strength created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_otc_links') THEN
        CREATE INDEX idx_otc_links ON wallet_links (is_suspected_otc, otc_confidence);
        RAISE NOTICE '✅ idx_otc_links created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_active_links') THEN
        CREATE INDEX idx_active_links ON wallet_links (is_active, last_transaction);
        RAISE NOTICE '✅ idx_active_links created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_wallet_types') THEN
        CREATE INDEX idx_wallet_types ON wallet_links (from_wallet_type, to_wallet_type);
        RAISE NOTICE '✅ idx_wallet_types created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_clusters') THEN
        CREATE INDEX idx_clusters ON wallet_links (from_cluster_id, to_cluster_id);
        RAISE NOTICE '✅ idx_clusters created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'wallet_links' AND indexname = 'idx_critical_paths') THEN
        CREATE INDEX idx_critical_paths ON wallet_links (is_critical_path, betweenness_score);
        RAISE NOTICE '✅ idx_critical_paths created';
    END IF;
END $$;

-- Create trigger for updated_at
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'update_wallet_links_updated_at'
    ) THEN
        CREATE OR REPLACE FUNCTION update_wallet_links_timestamp()
        RETURNS TRIGGER AS $func$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $func$ LANGUAGE plpgsql;
        
        CREATE TRIGGER update_wallet_links_updated_at
        BEFORE UPDATE ON wallet_links
        FOR EACH ROW
        EXECUTE FUNCTION update_wallet_links_timestamp();
        
        RAISE NOTICE '✅ updated_at trigger created';
    END IF;
END $$;

-- Verify migration
SELECT 
    'wallet_links' as table_name,
    COUNT(*) as total_columns,
    COUNT(*) FILTER (WHERE is_nullable = 'NO') as required_columns
FROM information_schema.columns 
WHERE table_name = 'wallet_links';

-- Show all indexes
SELECT 
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'wallet_links'
ORDER BY indexname;

-- Success message
DO $$ 
BEGIN
    RAISE NOTICE '✅ Migration completed successfully!';
    RAISE NOTICE '📊 wallet_links table is ready for use';
END $$;
