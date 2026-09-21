"""add business metrics to model_evaluations

Revision ID: 005
Revises: 004
Create Date: 2026-09-21
"""
from alembic import op
import sqlalchemy as sa

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "model_evaluations",
        sa.Column("watch_time_ndcg_at_10", sa.Float(), nullable=False, server_default="0"),
    )
    op.add_column(
        "model_evaluations",
        sa.Column("retention_7d", sa.Float(), nullable=False, server_default="0"),
    )
    op.add_column(
        "model_evaluations",
        sa.Column("avg_attributed_watch_s", sa.Float(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("model_evaluations", "avg_attributed_watch_s")
    op.drop_column("model_evaluations", "retention_7d")
    op.drop_column("model_evaluations", "watch_time_ndcg_at_10")
