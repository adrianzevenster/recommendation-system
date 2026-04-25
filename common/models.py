from datetime import datetime
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    region: Mapped[str] = mapped_column(String(32), index=True)
    preferred_language: Mapped[str] = mapped_column(String(32), default="en")
    maturity_rating: Mapped[str] = mapped_column(String(16), default="PG-13")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Item(Base):
    __tablename__ = "items"

    item_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    title: Mapped[str] = mapped_column(String(256), index=True)
    item_type: Mapped[str] = mapped_column(String(32))
    genres: Mapped[str] = mapped_column(String(256))
    actors: Mapped[str] = mapped_column(String(512), default="")
    director: Mapped[str] = mapped_column(String(256), default="")
    synopsis: Mapped[str] = mapped_column(Text, default="")
    language: Mapped[str] = mapped_column(String(32), default="en")
    release_year: Mapped[int] = mapped_column(Integer)
    maturity_rating: Mapped[str] = mapped_column(String(16), default="PG-13")
    available_regions: Mapped[str] = mapped_column(String(256), default="GLOBAL")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Interaction(Base):
    __tablename__ = "interactions"

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    item_id: Mapped[str] = mapped_column(String(64), index=True)
    event_type: Mapped[str] = mapped_column(String(32), index=True)
    watch_seconds: Mapped[int] = mapped_column(Integer, default=0)
    completion_pct: Mapped[float] = mapped_column(Float, default=0.0)
    region: Mapped[str] = mapped_column(String(32), index=True)
    device_type: Mapped[str] = mapped_column(String(32), default="web")
    event_ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    ingestion_ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ItemNeighbor(Base):
    __tablename__ = "item_neighbors"
    __table_args__ = (UniqueConstraint("source_item_id", "neighbor_item_id", name="uq_item_neighbor"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_item_id: Mapped[str] = mapped_column(String(64), index=True)
    neighbor_item_id: Mapped[str] = mapped_column(String(64), index=True)
    score: Mapped[float] = mapped_column(Float)
    algorithm: Mapped[str] = mapped_column(String(32), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class TrendingItem(Base):
    __tablename__ = "trending_items"
    __table_args__ = (UniqueConstraint("region", "item_id", name="uq_trending_region_item"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    region: Mapped[str] = mapped_column(String(32), index=True)
    item_id: Mapped[str] = mapped_column(String(64), index=True)
    score: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ModelVersion(Base):
    __tablename__ = "model_versions"

    version: Mapped[str] = mapped_column(String(64), primary_key=True)
    description: Mapped[str] = mapped_column(String(256), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
