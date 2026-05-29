"""gRPC client for the Yandex Cloud Billing Usage API (ConsumptionCoreService).

This is a separate API namespace from the REST Billing endpoints: it lives only
in gRPC and is not described under the public REST reference. It returns actual
consumption (cost, credits, expense) broken down by billing account, cloud,
folder, service, SKU, resource, label or service instance.

Docs: https://yandex.cloud/en/docs/billing/usage/api-ref/grpc/
Protos: https://github.com/yandex-cloud/cloudapi/tree/master/yandex/cloud/billing/usage_records/v1
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Iterable

import grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from yandex.cloud.billing.usage_records.v1 import (
    billing_types_pb2 as bt_pb,
)
from yandex.cloud.billing.usage_records.v1 import (
    consumption_core_service_pb2 as ccs_pb,
)
from yandex.cloud.billing.usage_records.v1 import (
    consumption_core_service_pb2_grpc as ccs_grpc,
)

from .auth import IamTokenProvider

log = logging.getLogger(__name__)

_AGG_FIELD = ccs_pb.UsageReportRequest.DESCRIPTOR.fields_by_name["aggregation_period"]
_AGG_VALUES = {v.name: v.number for v in _AGG_FIELD.enum_type.values}
AGG_PERIODS = tuple(name for name in _AGG_VALUES if name != "TIME_GROUPING_UNSPECIFIED")


def _parse_date(value: str) -> datetime:
    """Accept YYYY-MM-DD or full ISO 8601. Naive values are treated as UTC."""
    s = value.strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        dt = datetime.fromisoformat(s)
    else:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _to_ts(value: str) -> Timestamp:
    dt = _parse_date(value)
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def _agg(name: str | None) -> int:
    if not name:
        return _AGG_VALUES["MONTH"]
    upper = name.upper()
    if upper not in _AGG_VALUES or upper == "TIME_GROUPING_UNSPECIFIED":
        raise ValueError(
            f"aggregation_period must be one of {AGG_PERIODS}, got {name!r}"
        )
    return _AGG_VALUES[upper]


class UsageApiError(Exception):
    def __init__(self, code: grpc.StatusCode | None, details: str) -> None:
        super().__init__(f"Usage API error ({code}): {details}")
        self.code = code
        self.details = details


class UsageClient:
    """Async gRPC client with response caching to stay under the
    documented 1-request-per-minute rate limit."""

    def __init__(
        self,
        token_provider: IamTokenProvider,
        endpoint: str = "billing.api.cloud.yandex.net:443",
        cache_ttl_seconds: float = 300.0,
        cache_max_entries: int = 256,
    ) -> None:
        self._auth = token_provider
        self._endpoint = endpoint
        self._channel: grpc.aio.Channel | None = None
        self._stub: ccs_grpc.ConsumptionCoreServiceStub | None = None
        self._channel_lock = asyncio.Lock()
        self._cache: OrderedDict[tuple[str, bytes], tuple[float, dict[str, Any]]] = (
            OrderedDict()
        )
        self._cache_ttl = cache_ttl_seconds
        self._cache_max = cache_max_entries

    async def _ensure_stub(self) -> ccs_grpc.ConsumptionCoreServiceStub:
        if self._stub is not None:
            return self._stub
        async with self._channel_lock:
            if self._stub is None:
                creds = grpc.ssl_channel_credentials()
                self._channel = grpc.aio.secure_channel(self._endpoint, creds)
                self._stub = ccs_grpc.ConsumptionCoreServiceStub(self._channel)
        assert self._stub is not None
        return self._stub

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._stub = None

    def _cache_get(self, key: tuple[str, bytes]) -> dict[str, Any] | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        ts, value = entry
        if time.time() - ts > self._cache_ttl:
            self._cache.pop(key, None)
            return None
        self._cache.move_to_end(key)
        return value

    def _cache_put(self, key: tuple[str, bytes], value: dict[str, Any]) -> None:
        self._cache[key] = (time.time(), value)
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_max:
            self._cache.popitem(last=False)

    def _build_request(
        self,
        billing_account_id: str,
        from_date: str,
        to_date: str,
        aggregation_period: str | None = None,
        cloud_ids: Iterable[str] | None = None,
        folder_ids: Iterable[str] | None = None,
        service_ids: Iterable[str] | None = None,
        sku_ids: Iterable[str] | None = None,
        resource_ids: Iterable[str] | None = None,
        service_instance_ids: Iterable[str] | None = None,
        labels: dict[str, list[str]] | None = None,
        labels_or_filter_logic: bool = False,
    ) -> ccs_pb.UsageReportRequest:
        req = ccs_pb.UsageReportRequest(
            billing_account_id=billing_account_id,
            start_date=_to_ts(from_date),
            end_date=_to_ts(to_date),
            aggregation_period=_agg(aggregation_period),
            labels_or_filter_logic=labels_or_filter_logic,
        )
        if cloud_ids:
            req.cloud_ids.extend(cloud_ids)
        if folder_ids:
            req.folder_ids.extend(folder_ids)
        if service_ids:
            req.service_ids.extend(service_ids)
        if sku_ids:
            req.sku_ids.extend(sku_ids)
        if resource_ids:
            req.resource_ids.extend(resource_ids)
        if service_instance_ids:
            req.service_instance_ids.extend(service_instance_ids)
        if labels:
            for k, values in labels.items():
                if isinstance(values, str):
                    values = [values]
                req.labels[k].CopyFrom(bt_pb.LabelList(values=list(values)))
        return req

    async def _call(self, method: str, req: ccs_pb.UsageReportRequest) -> dict[str, Any]:
        key = (method, req.SerializeToString(deterministic=True))
        cached = self._cache_get(key)
        if cached is not None:
            log.debug("Usage API cache hit: %s", method)
            return cached
        stub = await self._ensure_stub()
        rpc = getattr(stub, method)
        token = await self._auth.get_token()
        try:
            resp = await rpc(req, metadata=(("authorization", f"Bearer {token}"),))
        except grpc.aio.AioRpcError as e:
            raise UsageApiError(e.code(), e.details() or str(e)) from None
        result = MessageToDict(resp, preserving_proto_field_name=True)
        self._cache_put(key, result)
        return result

    # --- Public API ---

    async def billing_account_report(self, **kw: Any) -> dict[str, Any]:
        return await self._call(
            "GetBillingAccountUsageReport", self._build_request(**kw)
        )

    async def cloud_report(self, **kw: Any) -> dict[str, Any]:
        return await self._call("GetCloudUsageReport", self._build_request(**kw))

    async def folder_report(self, **kw: Any) -> dict[str, Any]:
        return await self._call("GetFolderUsageReport", self._build_request(**kw))

    async def service_report(self, **kw: Any) -> dict[str, Any]:
        return await self._call("GetServiceUsageReport", self._build_request(**kw))

    async def sku_report(self, **kw: Any) -> dict[str, Any]:
        return await self._call("GetSKUUsageReport", self._build_request(**kw))

    async def resource_report(self, **kw: Any) -> dict[str, Any]:
        return await self._call("GetResourceUsageReport", self._build_request(**kw))

    async def label_key_report(self, **kw: Any) -> dict[str, Any]:
        return await self._call("GetLabelKeyUsageReport", self._build_request(**kw))

    async def service_instance_report(self, **kw: Any) -> dict[str, Any]:
        return await self._call(
            "GetServiceInstanceUsageReport", self._build_request(**kw)
        )
