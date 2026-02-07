# Database Management Scripts

This directory contains scripts for managing and maintaining the BlockIntel database schema.

## Quick Start

### 1. Verify Database Schema
Check if your database has all required tables and columns:

```bash
python3 scripts/verify_database_schema.py
```

### 2. Fix Any Issues
If verification shows problems, run the appropriate fix script:

```bash
# For missing tables
python3 scripts/fix_database_schema.py

# For transactions table specifically
python3 scripts/fix_transactions_table.py

# For missing columns in existing tables
python3 scripts/fix_remaining_tables.py
```

### 3. Initialize Sample Data
Populate with sample OTC wallets (optional):

```bash
python3 scripts/init_otc_db.py
```

## Scripts Overview

### Verification Scripts

#### `verify_database_schema.py`
**Purpose**: Check database schema health

**Usage**:
```bash
python3 scripts/verify_database_schema.py
```

**Output**:
- ✅ Shows which tables exist and are complete
- ⚠️  Identifies missing tables or columns
- 📊 Lists all tables in the schema

**When to use**:
- After any schema changes
- Before deploying to production
- When troubleshooting database errors

---

### Fix Scripts

#### `fix_database_schema.py`
**Purpose**: Create all missing database tables

**Usage**:
```bash
python3 scripts/fix_database_schema.py
```

**What it does**:
- Creates missing tables (custom_analyses, addresses, tokens, etc.)
- Adds necessary indexes
- Safe to run multiple times (checks existence first)

**⚠️ Note**: Does not drop existing tables

---

#### `fix_transactions_table.py`
**Purpose**: Recreate transactions table with proper id column

**Usage**:
```bash
python3 scripts/fix_transactions_table.py
```

**What it does**:
- Drops existing transactions table
- Recreates with proper schema including id column
- Adds all necessary indexes

**⚠️ WARNING**: This DELETES all transaction data!
- Script will prompt for confirmation if data exists
- Only use in development or if you're sure about data loss

---

#### `fix_remaining_tables.py`
**Purpose**: Add missing columns to existing tables

**Usage**:
```bash
python3 scripts/fix_remaining_tables.py
```

**What it does**:
- Adds missing columns to clusters, scan_jobs, scan_results
- Does not delete any data
- Safe to run multiple times

---

### Initialization Scripts

#### `init_all_db_tables.py`
**Purpose**: Comprehensive database initialization

**Usage**:
```bash
python3 scripts/init_all_db_tables.py
```

**What it does**:
- Creates schema if needed
- Imports and creates tables from all model bases
- Comprehensive initialization for first-time setup

**When to use**:
- First time setting up the database
- When starting fresh with a new database

---

#### `init_otc_db.py`
**Purpose**: Initialize OTC analysis with sample data

**Usage**:
```bash
python3 scripts/init_otc_db.py
```

**What it does**:
- Creates OTC wallet tables
- Populates with 5 sample wallets (Wintermute, Binance, etc.)
- Total sample volume: ~$407M

**Sample wallets**:
- Wintermute Trading (Market Maker)
- Binance 14 (CEX)
- Jump Trading (Prop Trading)
- Cumberland DRW (OTC Desk)
- Kraken 7 (CEX)

---

## Environment Setup

All scripts require the `DATABASE_URL` environment variable:

```bash
# Option 1: Load from .env file
export $(grep -v '^#' .env | xargs) && python3 scripts/verify_database_schema.py

# Option 2: Set manually
export DATABASE_URL="postgresql://user:password@host:5432/database"
python3 scripts/verify_database_schema.py
```

## Common Workflows

### New Development Setup
```bash
# 1. Verify current state
python3 scripts/verify_database_schema.py

# 2. Fix any issues
python3 scripts/fix_database_schema.py
python3 scripts/fix_transactions_table.py
python3 scripts/fix_remaining_tables.py

# 3. Verify fixes
python3 scripts/verify_database_schema.py

# 4. Add sample data (optional)
python3 scripts/init_otc_db.py
```

### Production Deployment
```bash
# 1. Backup database first!
pg_dump -h host -U user database > backup.sql

# 2. Verify current state
python3 scripts/verify_database_schema.py

# 3. Apply fixes carefully
python3 scripts/fix_database_schema.py
python3 scripts/fix_remaining_tables.py

# 4. Verify again
python3 scripts/verify_database_schema.py

# 5. Test application
curl http://your-api/health
```

### Troubleshooting Failed Endpoints

If you see these errors in logs:

**"relation 'custom_analyses' does not exist"**
```bash
python3 scripts/fix_database_schema.py
```

**"column transactions.id does not exist"**
```bash
python3 scripts/fix_transactions_table.py
```

**"Database error" (generic)**
```bash
# Check what's missing
python3 scripts/verify_database_schema.py

# Fix based on results
python3 scripts/fix_remaining_tables.py
```

## Dependencies

Most scripts only require:
- `psycopg2` (PostgreSQL adapter)
- Standard library modules

For full application scripts:
- SQLAlchemy
- Application models

## Database Schema

### Schema Name
Default: `otc_analysis`
Override with env var: `OTC_SCHEMA`

### Critical Tables
1. **custom_analyses** - Token analysis results
2. **transactions** - Blockchain transactions (must have id column!)
3. **addresses** - Address metadata
4. **tokens** - Token information
5. **wallet_analyses** - Wallet analysis results
6. **scan_jobs** - Background scanning jobs
7. **scan_results** - Scan results
8. **clusters** - Address clustering

### OTC Tables (Optional)
- **otc_wallets** - OTC wallet registry
- **otc_watchlist** - Watched addresses
- **otc_alerts** - Alert configuration

## Safety Features

All scripts include:
- ✅ Existence checks (don't create if exists)
- ✅ Confirmation prompts for destructive operations
- ✅ Clear output with status indicators
- ✅ Error handling and rollback
- ✅ Detailed logging

## Tips

1. **Always verify first**: Run `verify_database_schema.py` before making changes
2. **Read the output**: Scripts provide detailed information about what they're doing
3. **Backup production**: Always backup before running fix scripts in production
4. **Test locally first**: Test all scripts in development before production use
5. **Check logs**: Application logs will show database-related errors

## Exit Codes

All scripts use standard exit codes:
- `0` - Success
- `1` - Error occurred

Use in scripts:
```bash
if python3 scripts/verify_database_schema.py; then
    echo "Database OK"
else
    echo "Database has issues"
fi
```

## Getting Help

If you encounter issues:

1. Check the output - scripts provide detailed error messages
2. Verify DATABASE_URL is set correctly
3. Check database connection (can you connect with psql?)
4. Check permissions (does user have CREATE TABLE rights?)
5. Review DATABASE_FIX_SUMMARY.md for context

## Migration to Alembic

For future schema changes, consider using Alembic:

```bash
# Install
pip install alembic

# Create migration
alembic revision -m "add new column"

# Edit migration file
# alembic/versions/xxx_add_new_column.py

# Apply
alembic upgrade head
```

See `alembic/` directory for existing migration infrastructure.

---

**Last Updated**: 2026-02-07
**Maintained by**: BlockIntel Team
