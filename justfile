# Start PostgreSQL + catalog service (docker compose)
up:
    docker compose up --build -d

# Stop all containers
down:
    docker compose down

# Apply database migrations
migrate:
    uv run alembic upgrade head

# Run tests (SQLite in-memory, no Docker needed)
test:
    uv run pytest

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
