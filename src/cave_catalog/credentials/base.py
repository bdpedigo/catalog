from __future__ import annotations

from abc import ABC, abstractmethod

from cave_catalog.schemas import AccessResponse


class CredentialProvider(ABC):
    """Abstract base class for credential vending backends."""

    @abstractmethod
    async def vend(self, uri: str) -> AccessResponse:
        """Vend short-lived credentials scoped to *uri*.

        Parameters
        ----------
        uri:
            The asset URI (e.g. ``gs://bucket/path/``).

        Returns
        -------
        AccessResponse
            Credential bundle for the requested URI.
        """
