"""Initial assets table.

Revision ID: 0001
Revises:
Create Date: 2026-04-13
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "assets",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("datastack", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("mat_version", sa.Integer(), nullable=True),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("uri", sa.String(), nullable=False),
        sa.Column("format", sa.String(), nullable=False),
        sa.Column("asset_type", sa.String(), nullable=False),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column("is_managed", sa.Boolean(), nullable=False),
        sa.Column("mutability", sa.String(), nullable=False, server_default="static"),
        sa.Column("maturity", sa.String(), nullable=False, server_default="stable"),
        sa.Column(
            "properties",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("access_group", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # Partial unique index: uniqueness when mat_version IS NOT NULL
    op.create_index(
        "assets_unique_with_mat",
        "assets",
        ["datastack", "name", "mat_version", "revision"],
        unique=True,
        postgresql_where=sa.text("mat_version IS NOT NULL"),
    )

    # Partial unique index: uniqueness when mat_version IS NULL
    op.create_index(
        "assets_unique_without_mat",
        "assets",
        ["datastack", "name", "revision"],
        unique=True,
        postgresql_where=sa.text("mat_version IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("assets_unique_without_mat", table_name="assets")
    op.drop_index("assets_unique_with_mat", table_name="assets")
    op.drop_table("assets")
