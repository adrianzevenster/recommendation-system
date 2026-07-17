"""initial schema

Revision ID: 001
Revises:
Create Date: 2026-07-16
"""
from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("user_id", sa.String(64), primary_key=True),
        sa.Column("region", sa.String(32), nullable=False, index=True),
        sa.Column("preferred_language", sa.String(32), nullable=False, server_default="en"),
        sa.Column("maturity_rating", sa.String(16), nullable=False, server_default="PG-13"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "items",
        sa.Column("item_id", sa.String(64), primary_key=True),
        sa.Column("title", sa.String(256), nullable=False),
        sa.Column("item_type", sa.String(32), nullable=False),
        sa.Column("genres", sa.String(256), nullable=False),
        sa.Column("actors", sa.String(512), nullable=False, server_default=""),
        sa.Column("director", sa.String(256), nullable=False, server_default=""),
        sa.Column("synopsis", sa.Text(), nullable=False, server_default=""),
        sa.Column("language", sa.String(32), nullable=False, server_default="en"),
        sa.Column("release_year", sa.Integer(), nullable=False),
        sa.Column("maturity_rating", sa.String(16), nullable=False, server_default="PG-13"),
        sa.Column("available_regions", sa.String(256), nullable=False, server_default="GLOBAL"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_items_title", "items", ["title"])

    op.create_table(
        "interactions",
        sa.Column("event_id", sa.String(128), primary_key=True),
        sa.Column("user_id", sa.String(64), nullable=False),
        sa.Column("item_id", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("watch_seconds", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completion_pct", sa.Float(), nullable=False, server_default="0"),
        sa.Column("region", sa.String(32), nullable=False),
        sa.Column("device_type", sa.String(32), nullable=False, server_default="web"),
        sa.Column("event_ts", sa.DateTime(), nullable=False),
        sa.Column("ingestion_ts", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_interactions_user_id", "interactions", ["user_id"])
    op.create_index("ix_interactions_item_id", "interactions", ["item_id"])
    op.create_index("ix_interactions_event_type", "interactions", ["event_type"])
    op.create_index("ix_interactions_event_ts", "interactions", ["event_ts"])
    op.create_index("ix_interactions_region", "interactions", ["region"])

    op.create_table(
        "item_neighbors",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source_item_id", sa.String(64), nullable=False),
        sa.Column("neighbor_item_id", sa.String(64), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("algorithm", sa.String(32), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("source_item_id", "neighbor_item_id", "algorithm", name="uq_item_neighbor"),
    )
    op.create_index("ix_item_neighbors_source_item_id", "item_neighbors", ["source_item_id"])
    op.create_index("ix_item_neighbors_neighbor_item_id", "item_neighbors", ["neighbor_item_id"])
    op.create_index("ix_item_neighbors_algorithm", "item_neighbors", ["algorithm"])

    op.create_table(
        "trending_items",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("region", sa.String(32), nullable=False),
        sa.Column("item_id", sa.String(64), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("region", "item_id", name="uq_trending_region_item"),
    )
    op.create_index("ix_trending_items_region", "trending_items", ["region"])
    op.create_index("ix_trending_items_item_id", "trending_items", ["item_id"])

    op.create_table(
        "model_versions",
        sa.Column("version", sa.String(64), primary_key=True),
        sa.Column("description", sa.String(256), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "ranking_weights",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("model_version", sa.String(64), nullable=False),
        sa.Column("collaborative", sa.Float(), nullable=False, server_default="0.35"),
        sa.Column("content", sa.Float(), nullable=False, server_default="0.25"),
        sa.Column("session", sa.Float(), nullable=False, server_default="0.20"),
        sa.Column("trending", sa.Float(), nullable=False, server_default="0.10"),
        sa.Column("freshness", sa.Float(), nullable=False, server_default="0.05"),
        sa.Column("genre_bonus", sa.Float(), nullable=False, server_default="0.05"),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_ranking_weights_model_version", "ranking_weights", ["model_version"])

    op.create_table(
        "model_evaluations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("model_version", sa.String(64), nullable=False),
        sa.Column("ndcg_at_10", sa.Float(), nullable=False, server_default="0"),
        sa.Column("hit_rate_at_10", sa.Float(), nullable=False, server_default="0"),
        sa.Column("coverage", sa.Float(), nullable=False, server_default="0"),
        sa.Column("test_user_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_model_evaluations_model_version", "model_evaluations", ["model_version"])

    op.create_table(
        "experiments",
        sa.Column("name", sa.String(64), primary_key=True),
        sa.Column("description", sa.String(256), nullable=False, server_default=""),
        sa.Column("traffic_pct", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("variant_weights", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_experiments_is_active", "experiments", ["is_active"])


def downgrade() -> None:
    for table in [
        "experiments", "model_evaluations", "ranking_weights",
        "model_versions", "trending_items", "item_neighbors",
        "interactions", "items", "users",
    ]:
        op.drop_table(table)
