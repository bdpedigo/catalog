import json
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AuthSettings(BaseSettings):
    enabled: bool = Field(default=False, alias="AUTH_ENABLED")
    service_url: str = Field(default="http://localhost:7777", alias="AUTH_SERVICE_URL")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",
    )


class Settings(BaseSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://cave_catalog:cave_catalog@localhost:5432/cave_catalog",
        alias="DATABASE_URL",
    )
    service_name: str = Field(default="cave-catalog", alias="SERVICE_NAME")
    mat_engine_url: str | None = Field(default=None, alias="MAT_ENGINE_URL")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    datastacks_raw: str = Field(default="", alias="DATASTACKS")
    cave_token: str | None = Field(default=None, alias="CAVE_TOKEN")
    caveclient_server_address: str | None = Field(
        default=None, alias="CAVECLIENT_SERVER_ADDRESS"
    )
    auth: AuthSettings = Field(default_factory=AuthSettings)

    @property
    def datastacks(self) -> list[str]:
        raw = self.datastacks_raw.strip()
        if not raw:
            return []
        if raw.startswith("["):
            return json.loads(raw)
        return [s.strip() for s in raw.split(",") if s.strip()]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        env_nested_delimiter="__",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
