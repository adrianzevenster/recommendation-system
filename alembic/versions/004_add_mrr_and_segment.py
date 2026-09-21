"""add mrr_at_10 to model_evaluations and segment to ranking_weights

Revision ID: 004
Revises: 003
Create Date: 2026-09-21
"""
from alembic import op
import sqlalchemy as sa

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "model_evaluations",
        sa.Column("mrr_at_10", sa.Float(), nullable=False, server_default="0"),
    )
    op.add_column(
        "ranking_weights",
        sa.Column("segment", sa.String(32), nullable=True),
    )
    op.create_index("ix_ranking_weights_segment", "ranking_weights", ["segment"])


def downgrade() -> None:
    op.drop_index("ix_ranking_weights_segment", table_name="ranking_weights")
    op.drop_column("ranking_weights", "segment")
    op.drop_column("model_evaluations", "mrr_at_10")
