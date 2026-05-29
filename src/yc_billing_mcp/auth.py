from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Protocol

import httpx
import jwt as pyjwt

from .config import Settings

log = logging.getLogger(__name__)


class IamTokenProvider(Protocol):
    async def get_token(self) -> str: ...


class _CachedTokenProvider:
    """Base class — caches a fetched IAM token until shortly before expiry."""

    _skew_seconds = 60

    def __init__(self) -> None:
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._lock = asyncio.Lock()

    async def get_token(self) -> str:
        async with self._lock:
            now = time.time()
            if self._token and now < self._expires_at - self._skew_seconds:
                return self._token
            token, expires_in = await self._fetch()
            self._token = token
            self._expires_at = now + expires_in
            log.debug("Refreshed IAM token, valid for %ds", expires_in)
            return token

    async def _fetch(self) -> tuple[str, int]:
        raise NotImplementedError


class StaticIamTokenProvider:
    """User supplied a ready IAM token via env. We do not refresh — YC IAM tokens
    are valid for up to 12h; if it expires, the user must rotate the env var."""

    def __init__(self, token: str) -> None:
        self._token = token

    async def get_token(self) -> str:
        return self._token


class OAuthTokenProvider(_CachedTokenProvider):
    def __init__(self, oauth_token: str, http: httpx.AsyncClient, endpoint: str) -> None:
        super().__init__()
        self._oauth = oauth_token
        self._http = http
        self._endpoint = endpoint

    async def _fetch(self) -> tuple[str, int]:
        r = await self._http.post(
            self._endpoint,
            json={"yandexPassportOauthToken": self._oauth},
        )
        r.raise_for_status()
        data = r.json()
        # YC IAM tokens are valid up to 12h; refresh hourly to be safe.
        return data["iamToken"], 3600


class ServiceAccountKeyProvider(_CachedTokenProvider):
    def __init__(self, key: dict, http: httpx.AsyncClient, endpoint: str) -> None:
        super().__init__()
        try:
            self._key_id = key["id"]
            self._sa_id = key["service_account_id"]
            self._private_key = key["private_key"]
        except KeyError as e:
            raise ValueError(
                f"Service account key JSON is missing required field: {e.args[0]}"
            ) from None
        self._http = http
        self._endpoint = endpoint

    def _make_jwt(self) -> str:
        now = int(time.time())
        payload = {
            "aud": self._endpoint,
            "iss": self._sa_id,
            "iat": now,
            "exp": now + 3600,
        }
        return pyjwt.encode(
            payload,
            self._private_key,
            algorithm="PS256",
            headers={"kid": self._key_id},
        )

    async def _fetch(self) -> tuple[str, int]:
        token = await asyncio.to_thread(self._make_jwt)
        r = await self._http.post(self._endpoint, json={"jwt": token})
        r.raise_for_status()
        return r.json()["iamToken"], 3600


class WorkloadIdentityProvider(_CachedTokenProvider):
    """Exchange a Kubernetes-issued OIDC/JWT token for an IAM token via
    Yandex Cloud Workload Identity Federation."""

    def __init__(
        self,
        token_file: str,
        http: httpx.AsyncClient,
        endpoint: str,
        audience: str | None = None,
    ) -> None:
        super().__init__()
        self._token_file = Path(token_file)
        self._http = http
        self._endpoint = endpoint
        self._audience = audience

    async def _fetch(self) -> tuple[str, int]:
        # Re-read the file each refresh — kubelet rotates the projected token
        # well before its expiry.
        subject = await asyncio.to_thread(self._token_file.read_text)
        subject = subject.strip()
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token_type": "urn:ietf:params:oauth:token-type:id_token",
            "subject_token": subject,
        }
        if self._audience:
            data["audience"] = self._audience
        r = await self._http.post(
            self._endpoint,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        r.raise_for_status()
        body = r.json()
        return body["access_token"], int(body.get("expires_in", 3600))


class MetadataServiceProvider(_CachedTokenProvider):
    """For workloads inside Yandex Cloud Compute / Managed K8s with the
    instance service account attached — read the IAM token from the metadata
    service."""

    def __init__(self, http: httpx.AsyncClient, endpoint: str) -> None:
        super().__init__()
        self._http = http
        self._endpoint = endpoint

    async def _fetch(self) -> tuple[str, int]:
        r = await self._http.get(
            self._endpoint, headers={"Metadata-Flavor": "Google"}
        )
        r.raise_for_status()
        body = r.json()
        return body["access_token"], int(body.get("expires_in", 3600))


def make_provider(settings: Settings, http: httpx.AsyncClient) -> IamTokenProvider:
    """Select the first auth method that has its env var set, in this priority order.

    The order is chosen so explicit credentials win over ambient ones, and
    long-lived secrets (SA key) win over short-lived tokens (OAuth)."""
    if settings.iam_token:
        log.info("Auth: static IAM token (YC_IAM_TOKEN)")
        return StaticIamTokenProvider(settings.iam_token)
    if settings.sa_key_json:
        log.info("Auth: service account key (YC_SA_KEY_JSON)")
        return ServiceAccountKeyProvider(
            json.loads(settings.sa_key_json), http, settings.iam_endpoint
        )
    if settings.sa_key_file:
        log.info("Auth: service account key file (YC_SA_KEY_FILE=%s)", settings.sa_key_file)
        with open(settings.sa_key_file) as f:
            return ServiceAccountKeyProvider(json.load(f), http, settings.iam_endpoint)
    if settings.workload_token_file:
        log.info(
            "Auth: workload identity federation (YC_WORKLOAD_TOKEN_FILE=%s)",
            settings.workload_token_file,
        )
        return WorkloadIdentityProvider(
            settings.workload_token_file,
            http,
            settings.workload_endpoint,
            settings.workload_audience,
        )
    if settings.oauth_token:
        log.info("Auth: OAuth token (YC_OAUTH_TOKEN)")
        return OAuthTokenProvider(settings.oauth_token, http, settings.iam_endpoint)
    if settings.use_metadata:
        log.info("Auth: instance metadata service (YC_USE_METADATA)")
        return MetadataServiceProvider(http, settings.metadata_endpoint)
    raise RuntimeError(
        "No Yandex Cloud auth configured. Set one of: "
        "YC_IAM_TOKEN, YC_SA_KEY_JSON, YC_SA_KEY_FILE, "
        "YC_WORKLOAD_TOKEN_FILE, YC_OAUTH_TOKEN, or YC_USE_METADATA=true."
    )
