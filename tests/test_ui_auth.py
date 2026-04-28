"""Tests for the UI auth flow: login redirect, callback, logout, and auth guard."""

import pytest
from cave_catalog.auth.middleware import TOKEN_COOKIE_NAME


@pytest.fixture
def auth_client(client):
    """Alias for the base client fixture."""
    return client


@pytest.fixture
def auth_enabled_env(monkeypatch):
    """Enable auth for these tests."""
    monkeypatch.setenv("AUTH_ENABLED", "true")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class TestAuthGuard:
    """Unauthenticated users should be redirected to /ui/login."""

    async def test_register_redirects_when_unauthenticated(
        self, client, auth_enabled_env
    ):
        # Re-create client with auth enabled
        response = await client.get("/ui/register", follow_redirects=False)
        assert response.status_code == 302
        assert "/ui/login" in response.headers["location"]

    async def test_explore_redirects_when_unauthenticated(
        self, client, auth_enabled_env
    ):
        response = await client.get("/ui/explore", follow_redirects=False)
        assert response.status_code == 302
        assert "/ui/login" in response.headers["location"]

    async def test_pages_accessible_when_auth_disabled(self, client):
        """With AUTH_ENABLED=false (default), pages are accessible."""
        response = await client.get("/ui/register")
        assert response.status_code == 200
        assert "Register" in response.text


class TestLogin:
    """GET /ui/login should redirect to middle_auth authorize URL."""

    async def test_login_redirects_to_authorize(self, client):
        response = await client.get("/ui/login", follow_redirects=False)
        assert response.status_code == 307 or response.status_code == 302
        location = response.headers["location"]
        assert "/sticky_auth/api/v1/authorize" in location
        assert "redirect=" in location


class TestCallback:
    """GET /ui/callback should set cookie and redirect."""

    async def test_callback_sets_cookie(self, client):
        response = await client.get(
            f"/ui/callback?{TOKEN_COOKIE_NAME}=test-token-123&next=/ui/register",
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert response.headers["location"] == "/ui/register"
        # Cookie should be set in the response
        set_cookie = response.headers.get("set-cookie", "")
        assert TOKEN_COOKIE_NAME in set_cookie
        assert "test-token-123" in set_cookie

    async def test_callback_without_token_redirects_to_login(self, client):
        response = await client.get("/ui/callback", follow_redirects=False)
        assert response.status_code == 307 or response.status_code == 302
        location = response.headers["location"]
        assert "/ui/login" in location


class TestLogout:
    """GET /ui/logout should clear cookie and redirect to login."""

    async def test_logout_clears_cookie(self, client):
        response = await client.get("/ui/logout", follow_redirects=False)
        assert response.status_code == 302
        assert response.headers["location"] == "/ui/login"
        set_cookie = response.headers.get("set-cookie", "")
        # Cookie should be deleted (max-age=0 or expires in past)
        assert TOKEN_COOKIE_NAME in set_cookie
