"""UI route handlers for the server-rendered frontend."""

import asyncio

import httpx
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from cave_catalog.auth.middleware import (
    TOKEN_COOKIE_NAME,
    AuthUser,
    create_token_cookie_response,
    get_authorize_url,
    get_current_user,
)
from cave_catalog.config import Settings, get_settings
from cave_catalog.db.session import get_session
from cave_catalog.extractors import get_extractor
from cave_catalog.field_registry import ASSET_FIELDS, get_default_fields
from cave_catalog.mat_proxy import (
    MatProxyError,
    get_linkable_targets,
    get_target_columns,
    warm_cache,
)
from cave_catalog.routers.helpers import find_by_uri, find_duplicate, get_http_client
from cave_catalog.schemas import Maturity, Mutability
from cave_catalog.templating import templates
from cave_catalog.validation import check_name_reservation, validate_asset_name

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
    datastack = _get_current_datastack(request, settings)
    if datastack:
        asyncio.create_task(warm_cache(datastack))
    context = _page_context(request, user, settings, "register")
    context["mutability_options"] = [v.value for v in Mutability]
    context["maturity_options"] = [v.value for v in Maturity]
    return templates.TemplateResponse(
        request,
        "register.html",
        context,
    )


@router.get("/explore", response_class=HTMLResponse)
async def explore_page(
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
):
    datastack = _get_current_datastack(request, settings)
    fields = ASSET_FIELDS
    default_fields = get_default_fields()
    visible_columns = [f.key for f in default_fields]

    # Fetch initial page of assets
    limit = 25
    assets, total = await _fetch_assets(request, user, datastack, limit=limit)

    ctx = _page_context(request, user, settings, "explore")
    ctx.update(
        {
            "fields": fields,
            "visible_columns": visible_columns,
            "assets": assets,
            "total": total,
            "limit": limit,
            "offset": 0,
            "sort_by": "name",
            "sort_order": "asc",
            "filters": {},
        }
    )
    return templates.TemplateResponse(request, "explore.html", ctx)


@router.get("/fragments/assets", response_class=HTMLResponse)
async def explore_assets_fragment(
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    sort_by: str = Query(default="name"),
    sort_order: str = Query(default="asc"),
    # Filters
    name: str = Query(default=""),
    format: str = Query(default=""),
    maturity: str = Query(default=""),
    mat_version: str = Query(default=""),
    asset_type: str = Query(default=""),
    mutability: str = Query(default=""),
    source: str = Query(default=""),
):
    datastack = _get_current_datastack(request, settings)

    # Build filter params for API call
    filters = {}
    if name:
        filters["name_contains"] = name
    if format:
        filters["format"] = format
    if maturity:
        filters["maturity"] = maturity
    if mat_version:
        filters["mat_version"] = mat_version
    if asset_type:
        filters["asset_type"] = asset_type
    if mutability:
        filters["mutability"] = mutability
    if source:
        filters["source"] = source

    assets, total = await _fetch_assets(
        request,
        user,
        datastack,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
        **filters,
    )

    return templates.TemplateResponse(
        request,
        "fragments/asset_table.html",
        {
            "fields": ASSET_FIELDS,
            "assets": assets,
            "total": total,
            "limit": limit,
            "offset": offset,
            "sort_by": sort_by,
            "sort_order": sort_order,
        },
    )


async def _fetch_assets(
    request: Request,
    user: AuthUser,
    datastack: str | None,
    *,
    limit: int = 25,
    offset: int = 0,
    sort_by: str = "name",
    sort_order: str = "asc",
    **filters,
) -> tuple[list[dict], int]:
    """Fetch assets from the internal API via ASGI transport."""
    if not datastack:
        return [], 0

    params = {
        "datastack": datastack,
        "limit": limit,
        "offset": offset,
        "sort_by": sort_by,
        "sort_order": sort_order,
    }
    params.update(filters)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=request.app),
        base_url="http://localhost",
    ) as client:
        resp = await client.get(
            "/api/v1/assets/",
            params=params,
            headers={"Authorization": f"Bearer {user.token}"},
        )

    if resp.status_code != 200:
        return [], 0

    total = int(resp.headers.get("X-Total-Count", 0))
    assets = resp.json()
    return assets, total


# ---------------------------------------------------------------------------
# Asset detail page
# ---------------------------------------------------------------------------


@router.get("/explore/{asset_id}", response_class=HTMLResponse)
async def explore_detail_page(
    asset_id: str,
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
):
    asset = await _fetch_asset(request, asset_id, user)
    ctx = _page_context(request, user, settings, "explore")
    ctx["asset"] = asset
    return templates.TemplateResponse(request, "explore_detail.html", ctx)


# ---------------------------------------------------------------------------
# Asset edit page
# ---------------------------------------------------------------------------


@router.get("/explore/{asset_id}/edit", response_class=HTMLResponse)
async def explore_edit_page(
    asset_id: str,
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
):
    asset = await _fetch_asset(request, asset_id, user)
    ctx = _page_context(request, user, settings, "explore")
    ctx["asset"] = asset
    ctx["error"] = None
    return templates.TemplateResponse(request, "explore_edit.html", ctx)


@router.post("/explore/{asset_id}/edit", response_class=HTMLResponse)
async def explore_edit_submit(
    asset_id: str,
    request: Request,
    user: AuthUser = Depends(require_ui_auth),
    settings: Settings = Depends(get_settings),
):
    form = await request.form()

    # Build PATCH payload for mutable fields
    patch_body: dict = {}
    maturity = form.get("maturity")
    if maturity:
        patch_body["maturity"] = maturity
    access_group = form.get("access_group", "").strip()
    patch_body["access_group"] = access_group if access_group else None
    expires_at = form.get("expires_at", "").strip()
    patch_body["expires_at"] = expires_at if expires_at else None

    # Build annotation payload for tables (if columns present)
    annotations: list[dict] | None = None
    col_idx = 0
    while f"col_name_{col_idx}" in form:
        if annotations is None:
            annotations = []
        col_name = form[f"col_name_{col_idx}"]
        description = form.get(f"col_desc_{col_idx}", "").strip() or None

        # Parse kind for this column
        kind_type = form.get(f"col_kind_{col_idx}", "").strip()
        kind = None
        if kind_type == "materialization":
            target_table = form.get(f"col_kind_target_table_{col_idx}", "").strip()
            target_column = form.get(f"col_kind_target_column_{col_idx}", "").strip()
            if target_table and target_column:
                kind = {
                    "kind": "materialization",
                    "target_table": target_table,
                    "target_column": target_column,
                }
        elif kind_type == "segmentation":
            node_level = form.get(f"col_kind_node_level_{col_idx}", "").strip()
            custom_level = form.get(f"col_kind_custom_level_{col_idx}", "").strip()
            if node_level == "custom" and custom_level:
                node_level = f"level{custom_level}_id"
            if node_level and node_level != "custom":
                kind = {
                    "kind": "segmentation",
                    "node_level": node_level,
                }
        elif kind_type == "packed_point":
            resolution_raw = form.get(f"col_kind_resolution_{col_idx}", "").strip()
            resolution = None
            if resolution_raw:
                parts = [
                    float(x.strip()) for x in resolution_raw.split(",") if x.strip()
                ]
                if len(parts) == 3:
                    resolution = parts
            kind = {
                "kind": "packed_point",
                "resolution": resolution,
            }
        elif kind_type == "split_point":
            axis = form.get(f"col_kind_axis_{col_idx}", "").strip() or None
            point_group = (
                form.get(f"col_kind_point_group_{col_idx}", "").strip() or None
            )
            resolution_raw = form.get(f"col_kind_resolution_{col_idx}", "").strip()
            resolution = float(resolution_raw) if resolution_raw else None
            if axis:
                kind = {
                    "kind": "split_point",
                    "axis": axis,
                    "point_group": point_group,
                    "resolution": resolution,
                }

        annotations.append(
            {
                "column_name": col_name,
                "description": description,
                "kind": kind,
            }
        )
        col_idx += 1

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=request.app),
        base_url="http://localhost",
    ) as client:
        headers = {"Authorization": f"Bearer {user.token}"}

        # PATCH mutable fields
        resp = await client.patch(
            f"/api/v1/assets/{asset_id}",
            json=patch_body,
            headers=headers,
        )
        if resp.status_code >= 400:
            error = resp.json().get("detail", "Failed to update asset.")
            asset = await _fetch_asset(request, asset_id, user)
            ctx = _page_context(request, user, settings, "explore")
            ctx["asset"] = asset
            ctx["error"] = error
            return templates.TemplateResponse(
                request, "explore_edit.html", ctx, status_code=422
            )

        # PATCH annotations (tables only)
        if annotations is not None:
            ann_resp = await client.patch(
                f"/api/v1/tables/{asset_id}/annotations",
                json={"column_annotations": annotations},
                headers=headers,
            )
            if ann_resp.status_code >= 400:
                detail = ann_resp.json().get("detail", "")
                if isinstance(detail, dict):
                    error = detail.get("message", "Annotation update failed.")
                else:
                    error = str(detail) or "Annotation update failed."
                asset = await _fetch_asset(request, asset_id, user)
                ctx = _page_context(request, user, settings, "explore")
                ctx["asset"] = asset
                ctx["error"] = error
                return templates.TemplateResponse(
                    request, "explore_edit.html", ctx, status_code=422
                )

    # Success — redirect to detail page
    from starlette.responses import RedirectResponse

    return RedirectResponse(url=f"/ui/explore/{asset_id}", status_code=303)


async def _fetch_asset(request: Request, asset_id: str, user: AuthUser) -> dict:
    """Fetch a single asset via internal ASGI transport."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=request.app),
        base_url="http://localhost",
    ) as client:
        resp = await client.get(
            f"/api/v1/assets/{asset_id}",
            headers={"Authorization": f"Bearer {user.token}"},
        )
    if resp.status_code == 404:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Asset not found")
    if resp.status_code == 403:
        from fastapi import HTTPException

        raise HTTPException(status_code=403, detail="Not authorized to edit this asset")
    return resp.json()


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
            {
                "error": f"Unsupported format: '{fmt}'. Supported formats: delta, parquet."
            },
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
    """Parse column annotations and kinds from flat form data."""
    n_columns = int(form.get("n_columns", 0))
    annotations = []
    for i in range(n_columns):
        col_name = form.get(f"col_name_{i}", "")
        description = form.get(f"col_desc_{i}", "").strip() or None

        # Parse kind for this column
        kind_type = form.get(f"col_kind_{i}", "").strip()
        kind = None
        if kind_type == "materialization":
            target_table = form.get(f"col_kind_target_table_{i}", "").strip()
            target_column = form.get(f"col_kind_target_column_{i}", "").strip()
            if target_table and target_column:
                kind = {
                    "kind": "materialization",
                    "target_table": target_table,
                    "target_column": target_column,
                }
        elif kind_type == "segmentation":
            node_level = form.get(f"col_kind_node_level_{i}", "").strip()
            custom_level = form.get(f"col_kind_custom_level_{i}", "").strip()
            if node_level == "custom" and custom_level:
                node_level = f"level{custom_level}_id"
            if node_level and node_level != "custom":
                kind = {
                    "kind": "segmentation",
                    "node_level": node_level,
                }
        elif kind_type == "packed_point":
            resolution_raw = form.get(f"col_kind_resolution_{i}", "").strip()
            resolution = None
            if resolution_raw:
                parts = [
                    float(x.strip()) for x in resolution_raw.split(",") if x.strip()
                ]
                if len(parts) == 3:
                    resolution = parts
            kind = {
                "kind": "packed_point",
                "resolution": resolution,
            }
        elif kind_type == "split_point":
            axis = form.get(f"col_kind_axis_{i}", "").strip() or None
            point_group = form.get(f"col_kind_point_group_{i}", "").strip() or None
            resolution_raw = form.get(f"col_kind_resolution_{i}", "").strip()
            resolution = float(resolution_raw) if resolution_raw else None
            if axis:
                kind = {
                    "kind": "split_point",
                    "axis": axis,
                    "point_group": point_group,
                    "resolution": resolution,
                }

        if description or kind:
            annotations.append(
                {
                    "column_name": col_name,
                    "description": description,
                    "kind": kind,
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
    if mat_version_raw:
        try:
            mat_version = int(mat_version_raw)
        except ValueError:
            return templates.TemplateResponse(
                request,
                "fragments/register_error.html",
                {"error": "Mat version must be an integer."},
            )
    else:
        mat_version = None

    revision_raw = str(form.get("revision", "0")).strip()
    if revision_raw:
        try:
            revision = int(revision_raw)
        except ValueError:
            return templates.TemplateResponse(
                request,
                "fragments/register_error.html",
                {"error": "Revision must be an integer."},
            )
    else:
        revision = 0

    mutability = str(form.get("mutability", "static")).strip()
    maturity = str(form.get("maturity", "stable")).strip()
    is_managed = form.get("is_managed") == "true"
    access_group = str(form.get("access_group", "")).strip() or None

    expires_at_raw = str(form.get("expires_at", "")).strip()
    expires_at = expires_at_raw if expires_at_raw else None

    properties_raw = str(form.get("properties", "{}")).strip()
    try:
        import json

        properties = json.loads(properties_raw) if properties_raw else {}
    except (json.JSONDecodeError, ValueError):
        properties = {}

    column_annotations = _parse_column_annotations(form)

    # Build the request payload for the tables API

    payload = {
        "datastack": datastack,
        "name": name,
        "mat_version": mat_version,
        "revision": revision,
        "uri": uri,
        "format": fmt,
        "asset_type": "table",
        "is_managed": is_managed,
        "mutability": mutability,
        "maturity": maturity,
        "properties": properties,
        "access_group": access_group,
        "expires_at": expires_at,
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
        options.append(f'<option value="{col}">{col}</option>')
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

    # Check name format
    try:
        validate_asset_name(name)
    except ValueError as exc:
        return HTMLResponse(
            f'<span class="name-check unavailable">&#10007; {exc}</span>'
        )

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

    return HTMLResponse('<span class="name-check available">&#10003; Available</span>')


@router.get("/fragments/check-uri", response_class=HTMLResponse)
async def check_uri_fragment(
    request: Request,
    uri: str = Query(""),
    user: AuthUser = Depends(require_ui_auth),
    session: AsyncSession = Depends(get_session),
):
    """Return HTML fragment with ✓/✗ URI uniqueness indicator."""
    uri = uri.strip()
    if not uri:
        return HTMLResponse("")

    existing = await find_by_uri(session, uri)
    if existing is not None:
        return HTMLResponse(
            f'<span class="name-check unavailable">&#10007; URI already registered (ID: {existing.id})</span>'
        )

    return HTMLResponse(
        '<span class="name-check available">&#10003; URI available</span>'
    )
