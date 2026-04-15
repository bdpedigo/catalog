"""Authentication middleware for CAVE Catalog.

Async FastAPI dependencies for authentication and authorization via middle_auth.
Token is accepted as Authorization: Bearer header, middle_auth_token query param,
or middle_auth_token cookie — matching the existing CAVE stack conventions.

When AUTH_ENABLED=false (default for local dev), all requests are treated as an
anonymous user with full permissions, so tests and local runs work without a
live auth service.

Usage:
    from cave_catalog.auth.middleware import AuthUser, require_auth, require_permission

    @router.get("/datastacks")
    async def list(user: AuthUser = Depends(require_auth)):
        ...

    @router.post("/register")
    async def register(user: AuthUser = Depends(require_permission("edit", resource="body_datastack"))):
        ...
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated, Any
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

import httpx
import structlog
from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from cave_catalog.config import Settings, get_settings

logger = structlog.get_logger()

security = HTTPBearer(auto_error=False)

TOKEN_COOKIE_NAME = "middle_auth_token"


@dataclass
class AuthUser:
    user_id: str
    email: str
    name: str = ""
    groups: list[str] = field(default_factory=list)
    permissions: dict[str, list[str]] = field(default_factory=dict)
    is_admin: bool = False
    token: str = ""
    expires_at: datetime | None = None

    def has_permission(self, resource: str, permission: str) -> bool:
        if self.is_admin:
            return True
        resource_perms = self.permissions.get(resource, [])
        return permission in resource_perms or "admin" in resource_perms

    def in_group(self, group: str) -> bool:
        return group in self.groups

    def shares_group_with(self, required_groups: list[str]) -> bool:
        if self.is_admin:
            return True
        return bool(set(self.groups) & set(required_groups))


class AuthServiceError(Exception):
    def __init__(self, message: str, status_code: int = 503, **kwargs: Any) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.extra = kwargs


class AuthClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.settings.auth.service_url,
                timeout=httpx.Timeout(30.0),
            )
        return self._http_client

    async def close(self) -> None:
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def validate_token(self, token: str) -> AuthUser:
        try:
            response = await self.http_client.get(
                "/api/v1/user/cache",
                headers={"Authorization": f"Bearer {token}"},
            )
            if response.status_code == 401:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or expired token",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            if response.status_code != 200:
                logger.warning(
                    "auth_service_error",
                    status_code=response.status_code,
                    body=response.text[:200],
                )
                raise AuthServiceError(
                    f"Auth service returned {response.status_code}",
                    status_code=response.status_code,
                )
            data = response.json()
            return AuthUser(
                user_id=str(data.get("id", data.get("sub", ""))),
                email=data.get("email", ""),
                name=data.get("name", ""),
                groups=data.get("groups", []),
                permissions=data.get("permissions_v2", data.get("permissions", {})),
                is_admin=data.get("admin", False) or data.get("superadmin", False),
                token=token,
                expires_at=(
                    datetime.fromtimestamp(data["exp"], tz=UTC)
                    if "exp" in data
                    else None
                ),
            )
        except httpx.RequestError as e:
            logger.error("auth_service_unreachable", error=str(e))
            raise AuthServiceError(
                "Failed to contact authentication service", original_error=str(e)
            ) from e


_auth_client: AuthClient | None = None


def get_auth_client(settings: Settings = Depends(get_settings)) -> AuthClient:
    global _auth_client
    if _auth_client is None:
        _auth_client = AuthClient(settings)
    return _auth_client


@dataclass
class _ExtractedToken:
    token: str | None
    from_query_param: bool = False


def _extract_token(
    request: Request, credentials: HTTPAuthorizationCredentials | None
) -> _ExtractedToken:
    if credentials is not None:
        return _ExtractedToken(token=credentials.credentials, from_query_param=False)
    query_token = request.query_params.get(TOKEN_COOKIE_NAME)
    if query_token:
        return _ExtractedToken(token=query_token, from_query_param=True)
    cookie_token = request.cookies.get(TOKEN_COOKIE_NAME)
    if cookie_token:
        return _ExtractedToken(token=cookie_token, from_query_param=False)
    return _ExtractedToken(token=None)


def _get_url_without_token(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop(TOKEN_COOKIE_NAME, None)
    params.pop("token", None)
    new_query = urlencode(
        {k: v[0] if len(v) == 1 else v for k, v in params.items()}, doseq=True
    )
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        )
    )


def create_token_cookie_response(redirect_url: str, token: str) -> Response:
    response = RedirectResponse(url=redirect_url, status_code=302)
    response.set_cookie(
        key=TOKEN_COOKIE_NAME, value=token, secure=True, httponly=True, samesite="lax"
    )
    return response


def get_authorize_url(settings: Settings, redirect_url: str) -> str:
    auth_url = settings.auth.service_url.rstrip("/")
    return f"{auth_url}/api/v1/authorize?redirect={quote(redirect_url)}"


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
    settings: Settings = Depends(get_settings),
) -> AuthUser | None:
    if not settings.auth.enabled:
        logger.debug("auth_disabled_anonymous_user")
        return AuthUser(
            user_id="anonymous",
            email="anonymous@disabled",
            name="Anonymous (auth disabled)",
            groups=["public"],
            permissions={},
        )
    extracted = _extract_token(request, credentials)
    if extracted.token is None:
        logger.debug("no_token_found", path=request.url.path)
        return None
    logger.debug(
        "token_found",
        source="query_param" if extracted.from_query_param else "header_or_cookie",
        path=request.url.path,
    )
    auth_client = get_auth_client(settings)
    try:
        user = await auth_client.validate_token(extracted.token)
    except AuthServiceError as e:
        logger.error("auth_service_error", error=str(e), status_code=e.status_code)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service unavailable",
        ) from e
    logger.debug(
        "token_validated",
        user_id=user.user_id,
        email=user.email,
        is_admin=user.is_admin,
    )
    return user


async def require_auth(
    user: AuthUser | None = Depends(get_current_user),
) -> AuthUser:
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def require_permission(
    permission: str,
    resource_param: str = "datastack_name",
) -> Callable[..., Any]:
    """Return a FastAPI dependency that checks the user has `permission` on the
    datastack named by `resource_param` path parameter.

    For endpoints where the datastack comes from the request body or a query
    param, inject the user with `Depends(require_auth)` and check manually via
    user.has_permission(datastack, permission).
    """

    async def _dep(
        request: Request,
        user: AuthUser = Depends(require_auth),
        settings: Settings = Depends(get_settings),
    ) -> AuthUser:
        if not settings.auth.enabled:
            return user
        resource = request.path_params.get(resource_param)
        if resource is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing path parameter: {resource_param}",
            )
        if not user.has_permission(resource, permission):
            logger.warning(
                "permission_denied",
                user_id=user.user_id,
                resource=resource,
                permission=permission,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission '{permission}' required on '{resource}'",
            )
        return user

    return _dep


async def require_admin(
    user: AuthUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> AuthUser:
    if not settings.auth.enabled:
        return user
    if not user.is_admin:
        logger.warning("admin_access_denied", user_id=user.user_id, email=user.email)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )
    return user


def require_group(*groups: str) -> Callable[..., Any]:
    async def _dep(
        user: AuthUser = Depends(require_auth),
        settings: Settings = Depends(get_settings),
    ) -> AuthUser:
        if not settings.auth.enabled:
            return user
        if not user.shares_group_with(list(groups)):
            logger.warning(
                "group_access_denied",
                user_id=user.user_id,
                user_groups=user.groups,
                required_groups=groups,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Membership in one of {list(groups)} required",
            )
        return user

    return _dep


RequireAuth = Annotated[AuthUser, Depends(require_auth)]
