"""GCS credential provider using Credential Access Boundaries.

Required GCP permissions for the catalog service account
---------------------------------------------------------
1. **Storage read access on managed buckets**
   The service account must have ``roles/storage.objectViewer`` (or a custom
   role with ``storage.objects.get`` + ``storage.objects.list``) granted at
   the bucket level for every bucket that contains managed catalog assets.
   This lets the service account hold an OAuth token that *can* read those
   objects — it is then downscoped to a specific prefix before being handed
   to the caller.

   Recommended: grant per-bucket rather than project-wide to limit blast radius.

   Example (gcloud)::

       gcloud storage buckets add-iam-policy-binding gs://my-managed-bucket \\
           --member="serviceAccount:cave-catalog@PROJECT.iam.gserviceaccount.com" \\
           --role="roles/storage.objectViewer"

2. **Token self-impersonation for Credential Access Boundary exchange**
   Generating a downscoped token requires the service account to call the
   Security Token Service (STS) ``token`` endpoint with its own access token
   as the subject.  This works with any valid ``cloud-platform``-scoped token,
   so *no extra IAM role is required* for the STS exchange itself.

   However, if you want the service to generate downscoped tokens *on behalf
   of* a different service account (e.g., a dedicated data-reader SA), that
   SA must grant the catalog SA ``roles/iam.serviceAccountTokenCreator``.

   For the simpler single-SA setup used here (the catalog SA down-scopes its
   own token), no additional IAM binding is needed beyond (1) above.

3. **Workload Identity (GKE production)**
   In production the pod's Kubernetes Service Account (KSA) should be bound
   to the GCP service account via Workload Identity so no key file is needed:

       gcloud iam service-accounts add-iam-policy-binding \\
           cave-catalog@PROJECT.iam.gserviceaccount.com \\
           --role="roles/iam.workloadIdentityUser" \\
           --member="serviceAccount:PROJECT.svc.id.goog[NAMESPACE/KSA_NAME]"

   For local development set the ``GOOGLE_APPLICATION_CREDENTIALS`` env var
   to the path of a downloaded service account key JSON file.

Summary
~~~~~~~
+-----------------------------------------------+-----------------------------+
| Permission                                    | Scope                       |
+===============================================+=============================+
| roles/storage.objectViewer                    | Each managed GCS bucket     |
| roles/iam.workloadIdentityUser (GKE only)     | catalog GCP service account |
+-----------------------------------------------+-----------------------------+

No project-level roles are required.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from urllib.parse import urlparse

import google.auth
import google.auth.transport.requests
from google.auth import downscoped

from cave_catalog.credentials.base import CredentialProvider
from cave_catalog.schemas import AccessResponse

_EXPIRY_SECONDS = 3600


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    """Parse a ``gs://bucket/prefix`` URI into ``(bucket, prefix)``.

    The prefix is returned without a leading slash but with a trailing slash
    preserved if present.
    """
    parsed = urlparse(uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def _build_downscoped_credentials(
    source_credentials: google.auth.credentials.Credentials,
    bucket: str,
    prefix: str,
) -> downscoped.Credentials:
    availability_condition = downscoped.AvailabilityCondition(
        expression=(
            f"resource.name.startsWith('projects/_/buckets/{bucket}/objects/{prefix}')"
        ),
    )
    rule = downscoped.AccessBoundaryRule(
        available_resource=f"//storage.googleapis.com/projects/_/buckets/{bucket}",
        available_permissions=["inRole:roles/storage.objectViewer"],
        availability_condition=availability_condition,
    )
    boundary = downscoped.CredentialAccessBoundary(rules=[rule])
    return downscoped.Credentials(
        source_credentials=source_credentials,
        credential_access_boundary=boundary,
    )


def _refresh_credentials(
    credentials: downscoped.Credentials,
) -> tuple[str, int]:
    """Synchronously refresh *credentials* and return ``(token, expires_in)``."""
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    token: str = credentials.token
    if credentials.expiry is not None:
        expires_in = max(
            0,
            int(
                (
                    credentials.expiry.replace(tzinfo=UTC) - datetime.now(UTC)
                ).total_seconds()
            ),
        )
    else:
        expires_in = _EXPIRY_SECONDS
    return token, expires_in


class GCSCredentialProvider(CredentialProvider):
    """Credential provider that vends downscoped GCS OAuth tokens."""

    async def vend(self, uri: str) -> AccessResponse:
        bucket, prefix = _parse_gcs_uri(uri)

        source_credentials, _ = await asyncio.to_thread(
            google.auth.default,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        downscoped_creds = _build_downscoped_credentials(
            source_credentials, bucket, prefix
        )

        token, expires_in = await asyncio.to_thread(
            _refresh_credentials, downscoped_creds
        )

        return AccessResponse(
            uri=uri,
            format="",  # populated by the caller from the asset record
            token=token,
            token_type="Bearer",
            expires_in=expires_in,
            storage_provider="gcs",
            is_managed=True,
        )
