"""add experiment lifecycle fields

Revision ID: 006
Revises: 005
Create Date: 2026-09-21
"""
from alembic import op
import sqlalchemy as sa

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "experiments",
        sa.Column("max_duration_days", sa.Integer(), nullable=False, server_default="14"),
    )
    op.add_column(
        "experiments",
        sa.Column("concluded_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "experiments",
        sa.Column("winning_variant", sa.String(16), nullable=True),
    )
    op.add_column(
        "experiments",
        sa.Column("conclusion_reason", sa.String(32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("experiments", "conclusion_reason")
    op.drop_column("experiments", "winning_variant")
    op.drop_column("experiments", "concluded_at")
    op.drop_column("experiments", "max_duration_days")
