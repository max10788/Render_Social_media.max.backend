"""Create all missing tables

Revision ID: 001
Revises:
Create Date: 2026-02-07 14:00:00.000000

This migration creates all missing database tables:
- custom_analyses
- transactions (with proper id column)
- addresses
- clusters
- tokens
- wallets
- scan_jobs
- scan_results
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create custom_analyses table
    op.create_table(
        'custom_analyses',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('token_address', sa.String(), nullable=False, index=True),
        sa.Column('chain', sa.String(), nullable=False, index=True),
        sa.Column('analysis_date', sa.DateTime(), nullable=False),
        sa.Column('token_name', sa.String(), nullable=True),
        sa.Column('token_symbol', sa.String(), nullable=True),
        sa.Column('market_cap', sa.Float(), nullable=True),
        sa.Column('volume_24h', sa.Float(), nullable=True),
        sa.Column('liquidity', sa.Float(), nullable=True),
        sa.Column('holders_count', sa.Integer(), nullable=True),
        sa.Column('total_score', sa.Float(), nullable=False),
        sa.Column('metrics', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('risk_flags', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('analysis_status', sa.String(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('user_id', sa.String(), nullable=True, index=True),
        sa.Column('session_id', sa.String(), nullable=True, index=True),
        schema='otc_analysis',
        if_not_exists=True
    )

    # Create transactions table if it doesn't exist, or add missing columns
    # First, check if table exists and handle accordingly
    op.create_table(
        'transactions',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('tx_hash', sa.String(255), unique=True, index=True, nullable=False),
        sa.Column('chain', sa.String(20), nullable=False, index=True),
        sa.Column('block_number', sa.Integer(), nullable=True),
        sa.Column('from_address', sa.String(255), nullable=False),
        sa.Column('to_address', sa.String(255), nullable=True),
        sa.Column('value', sa.Numeric(36, 18), nullable=True),
        sa.Column('gas_used', sa.Integer(), nullable=True),
        sa.Column('gas_price', sa.Numeric(36, 18), nullable=True),
        sa.Column('fee', sa.Numeric(36, 18), nullable=True),
        sa.Column('token_address', sa.String(255), nullable=True),
        sa.Column('token_amount', sa.Numeric(36, 18), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('status', sa.String(20), nullable=True),
        sa.Column('method', sa.String(100), nullable=True),
        sa.Column('transaction_metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        schema='otc_analysis',
        if_not_exists=True
    )

    # Create indexes for transactions table
    try:
        op.create_index('idx_transaction_hash', 'transactions', ['tx_hash'],
                       schema='otc_analysis', unique=False, if_not_exists=True)
        op.create_index('idx_transaction_addresses', 'transactions', ['from_address', 'to_address'],
                       schema='otc_analysis', unique=False, if_not_exists=True)
        op.create_index('idx_transaction_token', 'transactions', ['token_address'],
                       schema='otc_analysis', unique=False, if_not_exists=True)
        op.create_index('idx_transaction_timestamp', 'transactions', ['timestamp'],
                       schema='otc_analysis', unique=False, if_not_exists=True)
        op.create_index('idx_transaction_chain_block', 'transactions', ['chain', 'block_number'],
                       schema='otc_analysis', unique=False, if_not_exists=True)
    except Exception:
        # Indexes might already exist
        pass

    # Create addresses table
    op.create_table(
        'addresses',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('address', sa.String(255), unique=True, index=True, nullable=False),
        sa.Column('chain', sa.String(20), nullable=False),
        sa.Column('label', sa.String(255), nullable=True),
        sa.Column('address_type', sa.String(50), nullable=True),
        sa.Column('first_seen', sa.DateTime(), nullable=True),
        sa.Column('last_seen', sa.DateTime(), nullable=True),
        sa.Column('transaction_count', sa.Integer(), default=0),
        sa.Column('total_volume', sa.Numeric(36, 18), nullable=True),
        sa.Column('risk_score', sa.Float(), nullable=True),
        sa.Column('tags', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        schema='otc_analysis',
        if_not_exists=True
    )

    # Create tokens table
    op.create_table(
        'tokens',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('address', sa.String(255), unique=True, index=True, nullable=False),
        sa.Column('chain', sa.String(20), nullable=False),
        sa.Column('name', sa.String(255), nullable=True),
        sa.Column('symbol', sa.String(50), nullable=True),
        sa.Column('decimals', sa.Integer(), nullable=True),
        sa.Column('total_supply', sa.Numeric(36, 18), nullable=True),
        sa.Column('market_cap', sa.Float(), nullable=True),
        sa.Column('volume_24h', sa.Float(), nullable=True),
        sa.Column('liquidity', sa.Float(), nullable=True),
        sa.Column('holders_count', sa.Integer(), nullable=True),
        sa.Column('contract_verified', sa.Boolean(), default=False),
        sa.Column('creation_date', sa.DateTime(), nullable=True),
        sa.Column('token_score', sa.Float(), nullable=True),
        sa.Column('last_analyzed', sa.DateTime(), nullable=True),
        schema='otc_analysis',
        if_not_exists=True
    )

    # Create clusters table
    op.create_table(
        'clusters',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('cluster_id', sa.String(255), unique=True, index=True, nullable=False),
        sa.Column('label', sa.String(255), nullable=True),
        sa.Column('cluster_type', sa.String(50), nullable=True),
        sa.Column('member_count', sa.Integer(), default=0),
        sa.Column('total_volume', sa.Numeric(36, 18), nullable=True),
        sa.Column('risk_score', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        schema='otc_analysis',
        if_not_exists=True
    )

    # Create scan_jobs table
    op.create_table(
        'scan_jobs',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('job_id', sa.String(255), unique=True, index=True, nullable=False),
        sa.Column('token_address', sa.String(255), nullable=False),
        sa.Column('chain', sa.String(20), nullable=False),
        sa.Column('status', sa.String(50), nullable=True),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        schema='otc_analysis',
        if_not_exists=True
    )

    # Create scan_results table
    op.create_table(
        'scan_results',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('token_id', sa.Integer(), nullable=False),
        sa.Column('token_score', sa.Float(), nullable=True),
        sa.Column('metrics', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('risk_flags', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('scan_date', sa.DateTime(), nullable=True),
        schema='otc_analysis',
        if_not_exists=True
    )

    # Create wallet_analyses table
    op.create_table(
        'wallet_analyses',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('token_id', sa.Integer(), nullable=False),
        sa.Column('wallet_address', sa.String(255), nullable=False),
        sa.Column('wallet_type', sa.String(50), nullable=True),
        sa.Column('balance', sa.Numeric(36, 18), nullable=True),
        sa.Column('percentage_of_supply', sa.Float(), nullable=True),
        sa.Column('transaction_count', sa.Integer(), default=0),
        sa.Column('first_transaction', sa.DateTime(), nullable=True),
        sa.Column('last_transaction', sa.DateTime(), nullable=True),
        sa.Column('risk_score', sa.Float(), nullable=True),
        sa.Column('analysis_date', sa.DateTime(), nullable=True),
        schema='otc_analysis',
        if_not_exists=True
    )


def downgrade() -> None:
    # Drop all tables in reverse order (due to potential foreign keys)
    op.drop_table('wallet_analyses', schema='otc_analysis', if_exists=True)
    op.drop_table('scan_results', schema='otc_analysis', if_exists=True)
    op.drop_table('scan_jobs', schema='otc_analysis', if_exists=True)
    op.drop_table('clusters', schema='otc_analysis', if_exists=True)
    op.drop_table('tokens', schema='otc_analysis', if_exists=True)
    op.drop_table('addresses', schema='otc_analysis', if_exists=True)
    op.drop_table('transactions', schema='otc_analysis', if_exists=True)
    op.drop_table('custom_analyses', schema='otc_analysis', if_exists=True)
