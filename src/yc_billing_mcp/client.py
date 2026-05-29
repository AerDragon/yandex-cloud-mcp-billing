from __future__ import annotations

import logging
from typing import Any

import httpx

from .auth import IamTokenProvider

log = logging.getLogger(__name__)


class YandexCloudError(Exception):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"Yandex Cloud API error {status}: {body[:1000]}")
        self.status = status
        self.body = body


class BillingClient:
    """Thin async client for the Yandex Cloud Billing REST API.

    Docs: https://yandex.cloud/en/docs/billing/api-ref/
    """

    def __init__(
        self,
        endpoint: str,
        token_provider: IamTokenProvider,
        http: httpx.AsyncClient,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._auth = token_provider
        self._http = http

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        token = await self._auth.get_token()
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        r = await self._http.get(
            f"{self._endpoint}{path}",
            params=clean,
            headers={"Authorization": f"Bearer {token}"},
        )
        if r.status_code >= 400:
            raise YandexCloudError(r.status_code, r.text)
        return r.json()

    async def _paginate(
        self,
        path: str,
        items_key: str,
        params: dict[str, Any] | None = None,
        max_items: int | None = None,
    ) -> list[dict]:
        items: list[dict] = []
        params = dict(params or {})
        while True:
            page = await self._get(path, params)
            items.extend(page.get(items_key, []))
            next_token = page.get("nextPageToken")
            if not next_token:
                break
            if max_items is not None and len(items) >= max_items:
                return items[:max_items]
            params["pageToken"] = next_token
        if max_items is not None:
            return items[:max_items]
        return items

    # --- Service ---

    async def list_services(
        self,
        filter_: str | None = None,
        page_size: int = 1000,
        max_items: int | None = None,
    ) -> list[dict]:
        return await self._paginate(
            "/billing/v1/services",
            "services",
            params={"pageSize": page_size, "filter": filter_},
            max_items=max_items,
        )

    async def get_service(self, service_id: str) -> dict:
        return await self._get(f"/billing/v1/services/{service_id}")

