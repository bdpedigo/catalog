import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class Mutability(StrEnum):
    STATIC = "static"
    MUTABLE = "mutable"


class Maturity(StrEnum):
    STABLE = "stable"
    DRAFT = "draft"
    DEPRECATED = "deprecated"


class AssetRequest(BaseModel):
    datastack: str
    name: str
    mat_version: int | None = None
    revision: int = Field(default=1, ge=1)
    uri: str
    format: str
    asset_type: str
    is_managed: bool
    mutability: Mutability = Mutability.STATIC
    maturity: Maturity = Maturity.STABLE
    properties: dict[str, Any] = Field(default_factory=dict)
    access_group: str | None = None
    expires_at: datetime | None = None


class AssetResponse(BaseModel):
    id: uuid.UUID
    datastack: str
    name: str
    mat_version: int | None
    revision: int
    uri: str
    format: str
    asset_type: str
    owner: str
    is_managed: bool
    mutability: Mutability
    maturity: Maturity
    properties: dict[str, Any]
    access_group: str | None
    created_at: datetime
    expires_at: datetime | None

    model_config = {"from_attributes": True}


class ValidationCheck(BaseModel):
    passed: bool
    message: str | None = None
    existing_id: uuid.UUID | None = None


class ValidationReport(BaseModel):
    auth_check: ValidationCheck | None = None
    duplicate_check: ValidationCheck | None = None
    name_reservation_check: ValidationCheck | None = None
    uri_reachable: ValidationCheck | None = None
    format_sniff: ValidationCheck | None = None
    mat_table_verify: ValidationCheck | None = None
