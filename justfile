# Copy example env file to .env (one-time setup)
setup:
    cp .env.example .env

# Start PostgreSQL + catalog service (docker compose)
up:
    docker compose up --build -d

# Stop all containers
down:
    docker compose down

# Stop all containers and wipe volumes (resets the database)
reset:
    docker compose down -v

# Apply database migrations
migrate:
    uv run alembic upgrade head

# Run tests (SQLite in-memory, no Docker needed)
test:
    uv run pytest

# Type-check the source (mypy)
typecheck:
    uv run mypy src/

# Run all checks (tests + type-checking)
checks: test typecheck

# Live-reload dev: postgres in Docker, uvicorn on host
dev:
    docker compose up postgres -d
    DATABASE_URL=postgresql+asyncpg://cave_catalog:cave_catalog@localhost:5432/cave_catalog \
    AUTH_ENABLED=false \
    LOG_LEVEL=DEBUG \
    uv run uvicorn cave_catalog.app:create_app --factory --reload --port 8000

# Tail logs from the catalog service container
logs:
    docker compose logs -f catalog-service

# Generate a new migration after changing SQLAlchemy models
new-migration msg:
    uv run alembic revision --autogenerate -m "{{msg}}"
