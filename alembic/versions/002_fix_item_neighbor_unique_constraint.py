"""fix item_neighbor unique constraint to include algorithm column

Revision ID: 002
Revises: 001
Create Date: 2026-07-17
"""
from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("uq_item_neighbor", "item_neighbors", type_="unique")
    op.create_unique_constraint(
        "uq_item_neighbor",
        "item_neighbors",
        ["source_item_id", "neighbor_item_id", "algorithm"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_item_neighbor", "item_neighbors", type_="unique")
    op.create_unique_constraint(
        "uq_item_neighbor",
        "item_neighbors",
        ["source_item_id", "neighbor_item_id"],
    )
