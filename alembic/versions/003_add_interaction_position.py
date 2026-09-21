"""add position column to interactions

Revision ID: 003
Revises: 002
Create Date: 2026-07-19
"""
import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "interactions",
        sa.Column("position", sa.Integer(), nullable=False, server_default="-1"),
    )


def downgrade() -> None:
    op.drop_column("interactions", "position")
