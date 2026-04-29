import uuid
from collections.abc import MutableMapping
from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, DateTime, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _FallbackPolymorphicMap(dict, MutableMapping):
    """Allow unknown ``asset_type`` values to load as the base ``Asset`` class."""

    def __missing__(self, key):
        return self["asset"]


class Base(DeclarativeBase):
    pass


class Asset(Base):
    __tablename__ = "assets"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    datastack: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    mat_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    uri: Mapped[str] = mapped_column(String, nullable=False)
    format: Mapped[str | None] = mapped_column(String, nullable=True)
    asset_type: Mapped[str] = mapped_column(String, nullable=False)
    owner: Mapped[int] = mapped_column(Integer, nullable=False)
    is_managed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    mutability: Mapped[str] = mapped_column(String, nullable=False, default="static")
    maturity: Mapped[str] = mapped_column(String, nullable=False, default="stable")
    properties: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    access_group: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __mapper_args__ = {
        "polymorphic_on": "asset_type",
        "polymorphic_identity": "asset",
        "with_polymorphic": "*",
    }

    # Table-specific nullable columns (populated only for asset_type="table")
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    cached_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    metadata_cached_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    column_annotations: Mapped[list | None] = mapped_column(JSON, nullable=True)

    __table_args__ = (
        # Uniqueness when mat_version is present
        Index(
            "assets_unique_with_mat",
            "datastack",
            "name",
            "mat_version",
            "revision",
            unique=True,
            postgresql_where=text("mat_version IS NOT NULL"),
        ),
        # Uniqueness when mat_version is absent
        Index(
            "assets_unique_without_mat",
            "datastack",
            "name",
            "revision",
            unique=True,
            postgresql_where=text("mat_version IS NULL"),
        ),
        # URI must be globally unique across all assets
        Index(
            "assets_unique_uri",
            "uri",
            unique=True,
        ),
    )


# Install fallback polymorphic map so unknown asset_type values load as Asset
Asset.__mapper__.polymorphic_map = _FallbackPolymorphicMap(
    Asset.__mapper__.polymorphic_map
)


class Table(Asset):
    """Table asset — single table inheritance subclass of Asset."""

    __mapper_args__ = {
        "polymorphic_identity": "table",
    }
