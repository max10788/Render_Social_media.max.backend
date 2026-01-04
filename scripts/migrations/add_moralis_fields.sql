-- ============================================================================
-- Migration: Add Moralis Label Fields
-- ============================================================================
-- Fügt entity_label und entity_logo Spalten hinzu
-- ============================================================================

-- Add entity_label column (if not exists)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'otc_wallets' 
          AND column_name = 'entity_label'
    ) THEN
        ALTER TABLE otc_wallets 
        ADD COLUMN entity_label VARCHAR(255);
        
        RAISE NOTICE '✅ entity_label column added';
    ELSE
        RAISE NOTICE '⏭️ entity_label already exists';
    END IF;
END $$;

-- Add entity_logo column (if not exists)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'otc_wallets' 
          AND column_name = 'entity_logo'
    ) THEN
        ALTER TABLE otc_wallets 
        ADD COLUMN entity_logo VARCHAR(512);
        
        RAISE NOTICE '✅ entity_logo column added';
    ELSE
        RAISE NOTICE '⏭️ entity_logo already exists';
    END IF;
END $$;

-- Verify migration
SELECT 
    column_name, 
    data_type, 
    character_maximum_length,
    is_nullable
FROM information_schema.columns 
WHERE table_name = 'otc_wallets' 
  AND column_name IN ('entity_label', 'entity_logo')
ORDER BY column_name;

-- Success message
DO $$ 
BEGIN
    RAISE NOTICE '✅ Migration completed successfully!';
END $$;
