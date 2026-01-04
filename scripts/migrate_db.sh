#!/bin/bash
set -e

echo "========================================"
echo "🔧 Database Migration: Moralis Fields"
echo "========================================"

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "❌ ERROR: DATABASE_URL not set"
    exit 1
fi

echo "📊 Database: ${DATABASE_URL%%@*}@***"
echo ""

# Run migration SQL
echo "➕ Adding Moralis fields..."

psql "$DATABASE_URL" <<EOF
-- Add entity_label column if not exists
DO \$\$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'otc_wallets' 
        AND column_name = 'entity_label'
    ) THEN
        ALTER TABLE otc_wallets ADD COLUMN entity_label VARCHAR(255);
        RAISE NOTICE '✅ entity_label column added';
    ELSE
        RAISE NOTICE '⏭️  entity_label already exists';
    END IF;
END \$\$;

-- Add entity_logo column if not exists
DO \$\$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'otc_wallets' 
        AND column_name = 'entity_logo'
    ) THEN
        ALTER TABLE otc_wallets ADD COLUMN entity_logo VARCHAR(512);
        RAISE NOTICE '✅ entity_logo column added';
    ELSE
        RAISE NOTICE '⏭️  entity_logo already exists';
    END IF;
END \$\$;

-- Verify columns exist
SELECT 
    column_name, 
    data_type, 
    character_maximum_length
FROM information_schema.columns 
WHERE table_name = 'otc_wallets' 
  AND column_name IN ('entity_label', 'entity_logo')
ORDER BY column_name;
EOF

MIGRATION_EXIT_CODE=$?

if [ $MIGRATION_EXIT_CODE -eq 0 ]; then
    echo ""
    echo "========================================"
    echo "✅ Migration completed successfully!"
    echo "========================================"
    exit 0
else
    echo ""
    echo "========================================"
    echo "❌ Migration failed with code $MIGRATION_EXIT_CODE"
    echo "========================================"
    exit 1
fi
