"""UI route handlers for the server-rendered frontend."""

import httpx
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from cave_catalog.auth.middleware import (
    AuthUser,
    TOKEN_COOKIE_NAME,
    create_token_cookie_response,
    get_authorize_url,
    get_current_user,
)
from cave_catalog.config import Settings, get_settings
from cave_catalog.db.session import get_session
from cave_catalog.extractors import get_extractor
from cave_catalog.mat_proxy import (
    MatProxyError,
    get_linkable_targets,
    get_target_columns,
)
from cave_catalog.routers.helpers import find_duplicate, get_http_client
from cave_catalog.templating import templates
from cave_catalog.validation import check_name_reservation

router = APIRouter(prefix="/ui", tags=["ui"])


# ---------------------------------------------------------------------------
# Auth guard dependency — redirects to login instead of returning 401 JSON
# ---------------------------------------------------------------------------


async def require_ui_auth(
    request: Request,
    user: AuthUser | None = Depends(get_current_user),
    settings: Settings = Depends(get_settings),
) -> AuthUser:
    """If user is authenticated, return the user. Otherwise redirect to login."""
    if user is None:
        login_url = f"/ui/login?next={request.url.path}"
        raise _redirect_exception(login_url)
    return user


class _RedirectException(Exception):
    def __init__(self, url: str) -> None:
        self.url = url


def _redirect_exception(url: str) -> _RedirectException:
    return _RedirectException(url)


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------


@router.get("/login")
async def login(
    request: Request,
    next: str = "/ui/register",
    settings: Settings = Depends(get_settings),
):
    """Redirect to middle_auth OAuth authorize endpoint."""
    # Build callback URL that includes the final destination
    callback_url = str(request.url_for("ui_callback")) + f"?next={next}"
    authorize_url = get_authorize_url(settings, callback_url)
    return RedirectResponse(url=authorize_url)


@router.get("/callback", name="ui_callback")
async def callback(
    request: Request,
    next: str = "/ui/register",
):
    """OAuth callback — extract token from query param, set cookie, redirect."""
    token = request.query_params.get(TOKEN_COOKIE_NAME) or request.query_params.get(
        "token"
    )
    if not token:
        return RedirectResponse(url="/ui/login")
    return create_token_cookie_response(redirect_url=next, token=token)


@router.get("/logout")
async def logout():
    """Clear the auth cookie and redirect to login."""
    response = RedirectResponse(url="/ui/login", status_code=302)
    response.delete_cookie(key=TOKEN_COOKIE_NAME)
    return response


# ---------------------------------------------------------------------------
# Page routes (auth-guarded)
# ---------------------------------------------------------------------------

DATASTACK_COOKIE = "cave_catalog_datastack"


def _get_current_datastack(request: Request, settings: Settings) -> str | None:
    """Read selected datastack from cookie, falling back to first configured."""
    cookie_val = request.cookies.get(DATASTACK_COOKIE)
    if cookie_val and cookie_val in settings.datastacks:
        return cookie_val
    if settings.datastacks:
        return settings.datastacks[0]
    return None


def _page_context(
    request: Request, user: AuthUser, settings: Settings, active_page: str
) -> dict:
    """Build common template context for all pages."""
    return {
        "active_page": active_page,
        "user": user,
        "datastacks": settings.datastacks,
        "current_datastack": _get_current_datastack(request, settings),
    }


@router.get("/select-datastack")
async def select_datastack(
    request: Request,
    datastack: str = "",
    settings: Settings = Depends(get_settings),
):
    """Set the selected datastack cookie (called via HTMX from the selector)."""
    referer = request.headers.get("referer", "/ui/register")
    response = RedirectResponse(url=referer, status_code=302)
    if datastack and datastack in settings.datastacks:
        response.set_cookie(
            key=DATASTACK_COOKIE, value=datastack, httponly=True, samesite="lax"
        )
    return response


@router.get("/register", response_class=HTMLResponse)
async def register_page(
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
):
    return templates.TemplateResponse(
        request,
        "register.html",
        _page_context(request, user, settings, "register"),
    )


@router.get("/explore", response_class=HTMLResponse)
async def explore_page(
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
):
    return templates.TemplateResponse(
        request,
        "explore.html",
        _page_context(request, user, settings, "explore"),
    )


# ---------------------------------------------------------------------------
# Preview HTMX route (Section 6)
# ---------------------------------------------------------------------------


@router.post("/preview", response_class=HTMLResponse)
async def preview_table(
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
):
    """Extract metadata from a URI and return a preview HTML fragment."""
    form = await request.form()
    uri = str(form.get("uri", "")).strip()
    fmt = str(form.get("format", "delta")).strip()

    if not uri:
        return templates.TemplateResponse(
            request,
            "fragments/preview_result.html",
            {"error": "Please enter a URI."},
        )

    # Resolve extractor
    try:
        extractor = get_extractor(fmt)
    except ValueError:
        return templates.TemplateResponse(
            request,
            "fragments/preview_result.html",
            {"error": f"Unsupported format: '{fmt}'. Supported formats: delta, parquet."},
        )

    # Run extraction
    try:
        metadata = await extractor.extract(uri)
    except Exception as exc:
        error_msg = str(exc)
        # Distinguish error types for diagnostics
        lower = error_msg.lower()
        if "not found" in lower or "no such" in lower or "does not exist" in lower:
            diagnostic = f"URI unreachable — the path does not exist or is not accessible: {error_msg}"
        elif "permission" in lower or "forbidden" in lower or "access" in lower:
            diagnostic = f"URI unreachable — permission denied: {error_msg}"
        else:
            diagnostic = f"Failed to read {fmt} data: {error_msg}"
        return templates.TemplateResponse(
            request,
            "fragments/preview_result.html",
            {"error": diagnostic},
        )

    return templates.TemplateResponse(
        request,
        "fragments/preview_result.html",
        {"metadata": metadata, "format": fmt, "error": None},
    )


# ---------------------------------------------------------------------------
# Registration submit HTMX route (Section 8)
# ---------------------------------------------------------------------------


def _parse_column_annotations(form: dict) -> list[dict]:
    """Parse column annotations and links from flat form data."""
    n_columns = int(form.get("n_columns", 0))
    annotations = []
    for i in range(n_columns):
        col_name = form.get(f"col_name_{i}", "")
        description = form.get(f"col_desc_{i}", "").strip() or None

        # Collect links for this column
        links = []
        for key, val in form.items():
            if key.startswith(f"link_type_{i}_"):
                link_id = key.split("_")[-1]
                link_type = val
                target = form.get(f"link_target_{i}_{link_id}", "")
                column = form.get(f"link_column_{i}_{link_id}", "")
                if target and column:
                    links.append(
                        {
                            "link_type": link_type,
                            "target_table": target,
                            "target_column": column,
                        }
                    )

        if description or links:
            annotations.append(
                {
                    "column_name": col_name,
                    "description": description,
                    "links": links,
                }
            )
    return annotations


@router.post("/register/submit", response_class=HTMLResponse)
async def register_submit(
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
    session: AsyncSession = Depends(get_session),
):
    """Handle registration form submission — call the tables API internally."""
    form_data = await request.form()
    form = dict(form_data)

    uri = str(form.get("uri", "")).strip()
    fmt = str(form.get("format", "delta")).strip()
    name = str(form.get("name", "")).strip()
    datastack = _get_current_datastack(request, settings)

    if not uri or not name or not datastack:
        return templates.TemplateResponse(
            request,
            "fragments/register_error.html",
            {"error": "URI, name, and datastack are required."},
        )

    mat_version_raw = str(form.get("mat_version", "")).strip()
    mat_version = int(mat_version_raw) if mat_version_raw else None

    column_annotations = _parse_column_annotations(form)

    # Build the request payload for the tables API
    import httpx

    payload = {
        "datastack": datastack,
        "name": name,
        "mat_version": mat_version,
        "revision": 0,
        "uri": uri,
        "format": fmt,
        "asset_type": "table",
        "is_managed": True,
        "mutability": "static",
        "maturity": "stable",
        "properties": {},
        "column_annotations": column_annotations,
    }

    # Call tables register API internally via ASGI transport
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=request.app),
        base_url="http://localhost",
    ) as client:
        resp = await client.post(
            "/api/v1/tables/register",
            json=payload,
            headers={"Authorization": f"Bearer {user.token}"},
        )

    if resp.status_code == 201:
        data = resp.json()
        return templates.TemplateResponse(
            request,
            "fragments/register_success.html",
            {
                "table_id": data.get("id", ""),
                "table_name": data.get("name", name),
                "datastack": data.get("datastack", datastack),
                "mat_version": data.get("mat_version"),
                "uri": data.get("uri", uri),
                "format": data.get("format", fmt),
            },
        )
    else:
        detail = resp.json().get("detail", "Unknown error")
        if isinstance(detail, dict):
            error_msg = detail.get("message", str(detail))
            details = []
            for err in detail.get("errors", []):
                details.append(
                    f"{err.get('column_name', '?')}: {err.get('reason', '?')} "
                    f"(target: {err.get('target_table', '?')}.{err.get('target_column', '?')})"
                )
            return templates.TemplateResponse(
                request,
                "fragments/register_error.html",
                {"error": error_msg, "details": details or None},
            )
        return templates.TemplateResponse(
            request,
            "fragments/register_error.html",
            {"error": str(detail)},
        )


# ---------------------------------------------------------------------------
# Mat proxy HTMX fragment routes (for link builder)
# ---------------------------------------------------------------------------


@router.get("/fragments/linkable-targets", response_class=HTMLResponse)
async def linkable_targets_fragment(
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
    version: int | None = None,
):
    """Return HTML <option> list of linkable targets for the current datastack."""
    datastack = _get_current_datastack(request, settings)
    if not datastack:
        return HTMLResponse('<option value="">No datastack selected</option>')
    try:
        targets = await get_linkable_targets(datastack, version)
    except MatProxyError as e:
        return HTMLResponse(f'<option value="">Error: {e}</option>')
    options = ['<option value="">-- Select target --</option>']
    for t in targets:
        label = f"{t.name} ({t.target_type})"
        options.append(
            f'<option value="{t.name}" data-type="{t.target_type}">{label}</option>'
        )
    return HTMLResponse("\n".join(options))


@router.get("/fragments/target-columns", response_class=HTMLResponse)
async def target_columns_fragment(
    request: Request,
    target_name: str,
    target_type: str = "table",
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
    version: int | None = None,
):
    """Return HTML <option> list of columns for a selected link target."""
    datastack = _get_current_datastack(request, settings)
    if not datastack:
        return HTMLResponse('<option value="">No datastack selected</option>')
    try:
        columns = await get_target_columns(datastack, target_name, target_type, version)
    except MatProxyError as e:
        return HTMLResponse(f'<option value="">Error: {e}</option>')
    options = ['<option value="">-- Select column --</option>']
    for col in columns:
        options.append(f'<option value="{col["name"]}">{col["name"]}</option>')
    return HTMLResponse("\n".join(options))


@router.get("/fragments/check-name", response_class=HTMLResponse)
async def check_name_fragment(
    request: Request,
    name: str = Query(""),
    mat_version: int | None = Query(default=None),
    revision: int = Query(default=0),
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
    session: AsyncSession = Depends(get_session),
):
    """Return HTML fragment with ✓/✗ name availability indicator."""
    name = name.strip()
    if not name:
        return HTMLResponse("")

    datastack = _get_current_datastack(request, settings)
    if not datastack:
        return HTMLResponse(
            '<span class="name-check error">No datastack selected</span>'
        )

    # Check reservation against mat tables
    reservation = await check_name_reservation(
        datastack=datastack,
        name=name,
        is_mat_source=False,
        client=get_http_client(),
        token=user.token,
    )
    if not reservation.passed:
        return HTMLResponse(
            '<span class="name-check unavailable">&#10007; Name is reserved for materialization</span>'
        )

    # Check for duplicate in DB
    existing = await find_duplicate(session, datastack, name, mat_version, revision)
    if existing is not None:
        return HTMLResponse(
            f'<span class="name-check unavailable">&#10007; Already registered (ID: {existing.id})</span>'
        )

    return HTMLResponse(
        '<span class="name-check available">&#10003; Available</span>'
    )
