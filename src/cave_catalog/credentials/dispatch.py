from __future__ import annotations

from urllib.parse import urlparse

from fastapi import HTTPException, status

from cave_catalog.credentials.base import CredentialProvider


def get_provider(uri: str) -> CredentialProvider:
    """Return the :class:`CredentialProvider` for *uri*'s scheme.

    Raises HTTP 422 for unsupported schemes so callers can propagate directly.
    """
    scheme = urlparse(uri).scheme
    if scheme == "gs":
        from cave_catalog.credentials.gcs import GCSCredentialProvider

        return GCSCredentialProvider()
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=f"Unsupported storage scheme: '{scheme}'",
    )
