import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, DateTime, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


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
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    uri: Mapped[str] = mapped_column(String, nullable=False)
    format: Mapped[str] = mapped_column(String, nullable=False)
    asset_type: Mapped[str] = mapped_column(String, nullable=False)
    owner: Mapped[str] = mapped_column(String, nullable=False)
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
    )
