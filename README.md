# CAVE Catalog

Asset registry, discovery, and credential vending for the CAVE stack.

## Local Development

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (for PostgreSQL)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)

### Quick Start

```bash
# Clone and enter the repo
git clone <repo-url> && cd catalog

# Copy the example env file
cp .env.example .env

# Start PostgreSQL + the catalog service
docker compose up --build -d

# Apply database migrations
uv run alembic upgrade head

# Verify the service is running
open http://localhost:8000/docs
```

The Swagger UI at `http://localhost:8000/docs` shows all available endpoints.

To reset the database (wipes all data):

```bash
docker compose down -v
```

## Live-Reload Development

For faster iteration, run only PostgreSQL in Docker and the service directly on your host:

```bash
# Start just postgres
docker compose up postgres -d

# Install dependencies (if not already done)
uv sync --dev

# Run the service with auto-reload
DATABASE_URL=postgresql+asyncpg://cave_catalog:cave_catalog@localhost:5432/cave_catalog \
AUTH_ENABLED=false \
LOG_LEVEL=DEBUG \
uv run uvicorn cave_catalog.app:create_app --factory --reload --port 8000
```

Code changes in `src/` will trigger an automatic restart.

## Running Tests

Tests use an in-memory SQLite database — no Docker or PostgreSQL needed.

```bash
uv run pytest
```

## Code Quality

Run type checking (mypy):

```bash
just typecheck
```

Run all checks (tests + type checking):

```bash
just checks
```

The test fixtures in `tests/conftest.py` provide:
- **`_env`** (autouse): sets `AUTH_ENABLED=false` and `DATABASE_URL` to SQLite, clears config caches between tests
- **`client`**: an async `httpx.AsyncClient` wired to the app with a per-test SQLite DB, tables auto-created

## Running Migrations

Apply all pending migrations to your local database:

```bash
uv run alembic upgrade head
```

Generate a new migration after changing SQLAlchemy models:

```bash
uv run alembic revision --autogenerate -m "describe your change"
```

Review the generated file in `migrations/versions/` before committing.

> **Note**: Alembic reads `DATABASE_URL` from `alembic.ini` by default (pointing at `localhost:5432`). Make sure your local PostgreSQL is running.

## Auth in Local Dev

By default, `.env.example` sets `AUTH_ENABLED=false`, which disables all authentication and permission checks. Any request is accepted regardless of headers.

To test with real authentication against the CAVE middle_auth service:

```bash
AUTH_ENABLED=true
AUTH_SERVICE_URL=https://globalv1.daf-apis.com/auth
```

With auth enabled, requests must include a valid `Authorization: Bearer <token>` header. The server validates tokens against the middle_auth instance and checks datastack-level permissions for read/write operations.
