"""Tests for credential vending endpoint and provider (tasks 4.1–4.4)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------


def _asset_payload(**overrides):
    base = {
        "datastack": "minnie65_public",
        "name": "synapses",
        "mat_version": 943,
        "revision": 0,
        "uri": "gs://mybucket/minnie65/synapses/",
        "format": "delta",
        "asset_type": "table",
        "is_managed": True,
        "mutability": "static",
        "maturity": "stable",
        "properties": {},
    }
    base.update(overrides)
    return base


def _patch_validation(monkeypatch):
    from cave_catalog.schemas import ValidationCheck, ValidationReport

    ok = ValidationCheck(passed=True)
    monkeypatch.setattr(
        "cave_catalog.routers.assets.run_validation_pipeline",
        AsyncMock(
            return_value=ValidationReport(
                name_reservation_check=ok,
                uri_reachable=ok,
                format_sniff=ok,
            )
        ),
    )


async def _register(client, monkeypatch, **overrides) -> dict:
    _patch_validation(monkeypatch)
    resp = await client.post(
        "/api/v1/assets/register", json=_asset_payload(**overrides)
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# Task 4.1 — Unit tests for GCSCredentialProvider
# ---------------------------------------------------------------------------


def test_parse_gcs_uri():
    from cave_catalog.credentials.gcs import _parse_gcs_uri

    bucket, prefix = _parse_gcs_uri("gs://mybucket/path/to/data/")
    assert bucket == "mybucket"
    assert prefix == "path/to/data/"


def test_parse_gcs_uri_no_prefix():
    from cave_catalog.credentials.gcs import _parse_gcs_uri

    bucket, prefix = _parse_gcs_uri("gs://mybucket/")
    assert bucket == "mybucket"
    assert prefix == ""


@pytest.mark.parametrize(
    "uri",
    [
        "gs:///path",  # empty bucket
        "s3://mybucket/path/",  # wrong scheme
        "mybucket/path/",  # no scheme
        "gs://INVALID_UPPER/path/",  # uppercase bucket
        "gs://a/path/",  # bucket too short
    ],
)
def test_parse_gcs_uri_rejects_invalid(uri):
    from cave_catalog.credentials.gcs import _parse_gcs_uri

    with pytest.raises(ValueError):
        _parse_gcs_uri(uri)


def test_parse_gcs_uri_rejects_unsafe_prefix():
    from cave_catalog.credentials.gcs import _parse_gcs_uri

    with pytest.raises(ValueError, match="Invalid GCS prefix"):
        _parse_gcs_uri("gs://mybucket/path' OR true/")


def test_build_downscoped_credentials_sets_bucket_and_prefix():
    from cave_catalog.credentials.gcs import _build_downscoped_credentials
    from google.auth import downscoped

    source = MagicMock()
    constructed_creds = MagicMock()

    with patch(
        "cave_catalog.credentials.gcs.downscoped.Credentials",
        return_value=constructed_creds,
    ) as mock_credentials:
        creds = _build_downscoped_credentials(source, "mybucket", "myprefix/")

    assert creds is constructed_creds
    mock_credentials.assert_called_once()
    assert mock_credentials.call_args.kwargs["source_credentials"] is source

    boundary = mock_credentials.call_args.kwargs["credential_access_boundary"]
    rule = boundary.rules[0]
    assert "mybucket" in rule.available_resource
    assert "inRole:roles/storage.objectViewer" in rule.available_permissions
    assert "myprefix/" in rule.availability_condition.expression


async def test_gcs_provider_vend_returns_access_response():
    """GCSCredentialProvider.vend() should call google.auth and return AccessResponse."""
    from cave_catalog.credentials.gcs import GCSCredentialProvider

    fake_creds = MagicMock()
    fake_creds.token = "ya29.fake-downscoped-token"
    fake_creds.expiry = datetime.now(UTC) + timedelta(hours=1)

    with (
        patch(
            "cave_catalog.credentials.gcs.google.auth.default",
            return_value=(MagicMock(), "test-project"),
        ),
        patch(
            "cave_catalog.credentials.gcs._build_downscoped_credentials",
            return_value=fake_creds,
        ),
        patch(
            "cave_catalog.credentials.gcs._refresh_credentials",
            return_value=("ya29.fake-downscoped-token", 3600),
        ),
    ):
        provider = GCSCredentialProvider()
        result = await provider.vend("gs://mybucket/path/")

    assert result.token == "ya29.fake-downscoped-token"
    assert result.token_type == "Bearer"
    assert result.expires_in == 3600
    assert result.storage_provider == "gcs"
    assert result.is_managed is True
    assert result.uri == "gs://mybucket/path/"


def test_refresh_credentials_calculates_expiry():
    from cave_catalog.credentials.gcs import _refresh_credentials

    fake_creds = MagicMock()
    fake_creds.token = "tok"
    fake_creds.expiry = datetime.now(UTC) + timedelta(seconds=3605)

    with patch("cave_catalog.credentials.gcs.google.auth.transport.requests.Request"):
        token, expires_in = _refresh_credentials(fake_creds)

    assert token == "tok"
    # Allow a few seconds of slack
    assert 3590 <= expires_in <= 3610


# ---------------------------------------------------------------------------
# Task 4.2 — Integration tests for /access endpoint
# ---------------------------------------------------------------------------


def _patch_gcs_provider(monkeypatch, token: str = "ya29.fake"):
    """Patch GCSCredentialProvider.vend to return a fake AccessResponse."""
    from cave_catalog.schemas import AccessResponse

    async def _fake_vend(self, uri: str) -> AccessResponse:
        return AccessResponse(
            uri=uri,
            format="",
            token=token,
            token_type="Bearer",
            expires_in=3600,
            storage_provider="gcs",
            is_managed=True,
        )

    monkeypatch.setattr(
        "cave_catalog.credentials.gcs.GCSCredentialProvider.vend", _fake_vend
    )


async def test_access_managed_gcs_returns_token(client, monkeypatch):
    asset = await _register(
        client,
        monkeypatch,
        uri="gs://mybucket/minnie65/synapses/",
        is_managed=True,
        format="delta",
    )
    _patch_gcs_provider(monkeypatch)

    resp = await client.post(f"/api/v1/assets/{asset['id']}/access")

    assert resp.status_code == 200
    data = resp.json()
    assert data["token"] == "ya29.fake"
    assert data["token_type"] == "Bearer"
    assert data["expires_in"] == 3600
    assert data["storage_provider"] == "gcs"
    assert data["format"] == "delta"
    assert data["uri"] == "gs://mybucket/minnie65/synapses/"


async def test_access_unmanaged_returns_passthrough(client, monkeypatch):
    asset = await _register(
        client,
        monkeypatch,
        is_managed=False,
        uri="gs://publicbucket/path/",
    )

    resp = await client.post(f"/api/v1/assets/{asset['id']}/access")

    assert resp.status_code == 200
    data = resp.json()
    assert data["token"] is None
    assert data["token_type"] is None
    assert data["expires_in"] is None
    assert data["is_managed"] is False
    assert data["uri"] == "gs://publicbucket/path/"


async def test_access_expired_asset_returns_404(client, monkeypatch):
    past = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    asset = await _register(client, monkeypatch, expires_at=past)

    resp = await client.post(f"/api/v1/assets/{asset['id']}/access")

    assert resp.status_code == 404


async def test_access_missing_asset_returns_404(client, monkeypatch):
    resp = await client.post(f"/api/v1/assets/{uuid.uuid4()}/access")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Task 4.3 — Auth tests
# ---------------------------------------------------------------------------


async def test_access_auth_disabled_allows_all(client, monkeypatch):
    """AUTH_ENABLED=false (default in tests) → all requests succeed."""
    asset = await _register(client, monkeypatch, is_managed=False)

    resp = await client.post(f"/api/v1/assets/{asset['id']}/access")

    assert resp.status_code == 200


async def test_access_auth_enabled_no_permission_returns_403(
    client, monkeypatch, tmp_path
):
    """With auth enabled, a user without datastack view returns 403."""
    import httpx
    from cave_catalog.app import create_app
    from cave_catalog.auth.middleware import AuthUser, require_auth
    from cave_catalog.config import get_settings
    from cave_catalog.db.models import Base
    from cave_catalog.db.session import get_session
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    # Register asset first (auth disabled via module-level _env fixture)
    await _register(client, monkeypatch, is_managed=False)

    # Build a fresh app with auth enabled and the user dependency overridden
    monkeypatch.setenv("AUTH_ENABLED", "true")
    get_settings.cache_clear()

    no_perms_user = AuthUser(
        user_id=99,
        email="noperms@test.com",
        groups=[],
        permissions={},
    )

    db_path = tmp_path / "auth_test.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(db_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def _override_session():
        async with factory() as session:
            yield session

    app2 = create_app()
    app2.dependency_overrides[get_session] = _override_session
    app2.dependency_overrides[require_auth] = lambda: no_perms_user

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app2), base_url="http://test"
    ) as c2:
        # Override auth to an admin user just for registration in the new DB
        admin_user = AuthUser(
            user_id=1, email="a@b.com", is_admin=True, groups=[], permissions={}
        )
        app2.dependency_overrides[require_auth] = lambda: admin_user
        _patch_validation(monkeypatch)
        reg = await c2.post(
            "/api/v1/assets/register",
            json=_asset_payload(is_managed=False, name="auth-test-asset"),
        )
        assert reg.status_code == 201, reg.text
        new_id = reg.json()["id"]

        # Now switch back to no-perms user
        app2.dependency_overrides[require_auth] = lambda: no_perms_user
        resp = await c2.post(f"/api/v1/assets/{new_id}/access")
        assert resp.status_code == 403

    await engine.dispose()


async def test_access_group_membership_overrides_datastack_permission(
    client, monkeypatch, tmp_path
):
    """An asset with access_group: a user in that group is granted access."""
    import httpx
    from cave_catalog.app import create_app
    from cave_catalog.auth.middleware import AuthUser, require_auth
    from cave_catalog.config import get_settings
    from cave_catalog.db.models import Base
    from cave_catalog.db.session import get_session
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    monkeypatch.setenv("AUTH_ENABLED", "true")
    get_settings.cache_clear()

    db_path = tmp_path / "group_test.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(db_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def _override_session():
        async with factory() as session:
            yield session

    # User is in the access_group but has no datastack permission
    group_user = AuthUser(
        user_id=7,
        email="member@test.com",
        groups=["special-group"],
        permissions={},
    )
    admin_user = AuthUser(
        user_id=1, email="a@b.com", is_admin=True, groups=[], permissions={}
    )

    app2 = create_app()
    app2.dependency_overrides[get_session] = _override_session

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app2), base_url="http://test"
    ) as c2:
        # Register the asset as admin
        app2.dependency_overrides[require_auth] = lambda: admin_user
        _patch_validation(monkeypatch)
        reg = await c2.post(
            "/api/v1/assets/register",
            json=_asset_payload(access_group="special-group", is_managed=False),
        )
        assert reg.status_code == 201, reg.text
        new_id = reg.json()["id"]

        # Now request access as the group member
        app2.dependency_overrides[require_auth] = lambda: group_user
        resp = await c2.post(f"/api/v1/assets/{new_id}/access")
        assert resp.status_code == 200

    await engine.dispose()


# ---------------------------------------------------------------------------
# Task 4.4 — Unsupported URI scheme returns 422
# ---------------------------------------------------------------------------


async def test_access_unsupported_scheme_returns_422(client, monkeypatch):
    asset = await _register(
        client,
        monkeypatch,
        uri="az://mycontainer/path/",
        is_managed=True,
    )

    resp = await client.post(f"/api/v1/assets/{asset['id']}/access")

    assert resp.status_code == 422
    assert "az" in resp.text
