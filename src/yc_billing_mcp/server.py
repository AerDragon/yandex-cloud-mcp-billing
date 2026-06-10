from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Literal

import httpx
from mcp.server.fastmcp import FastMCP
from pydantic import Field

import asyncio

from .auth import make_provider
from .client import BillingClient
from .config import Settings
from .fx import CbrFxProvider, FxRates
from .usage import UsageClient

log = logging.getLogger(__name__)

AggregationPeriod = Literal["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]

# A label key with more distinct values than this is "high cardinality" — grouping
# by it produces thousands of rows (e.g. managed-kubernetes-node-group-id). Flagged
# in list_label_keys so the caller filters/scopes instead of grouping blindly.
HIGH_CARDINALITY_THRESHOLD = 200


def _amount(d: Any, key: str) -> str:
    v = d.get(key) if isinstance(d, dict) else None
    return v.get("value", "0") if isinstance(v, dict) else "0"


def _to_float(s: str) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return 0.0


def _too_many_values_response(
    label_key: str, raw: dict[str, Any], value_count: int
) -> dict[str, Any]:
    """High-cardinality backstop for spend_grouped_by_label. When the full breakdown
    would exceed the response budget, return a summary the LLM can ACT on — the
    complete total + count, plus concrete, parameter-named ways to narrow — instead
    of truncating rows (silent loss) or sampling top-N (hidden tail). Validated
    against an LLM (3/3 intents recover correctly: coarser key for chargeback, scope
    for genuine per-value detail). NOT raised as an error: the query succeeded and
    the total is real — an error frame makes the model give up rather than narrow."""
    out: dict[str, Any] = {
        "label_key": label_key,
        "status": "too_many_values_to_list",
        "currency": raw.get("currency"),
        "total": raw.get("cost"),
        "value_count": value_count,
        "message": (
            f"This label key has {value_count} distinct values — the full per-value "
            "breakdown exceeds the response size limit and is NOT listed. The total "
            "above is complete and correct. To get a usable breakdown, narrow the "
            "query and call again:"
        ),
        "how_to_narrow": [
            "GROUP BY A COARSER KEY: call spend_grouped_by_label with a "
            "lower-cardinality label_key (e.g. 'project', 'team'). Use list_label_keys "
            "to see which keys exist and their cardinality.",
            "SCOPE TO A SUBSET: call spend_grouped_by_label with the SAME label_key "
            "plus service_ids / folder_ids / cloud_ids to restrict to one service / "
            "folder / cloud (far fewer values).",
            f'NAME SPECIFIC VALUES: spend_by_label(labels={{"{label_key}": ["<value>"]}}) '
            "if you already know the value ids.",
        ],
    }
    if raw.get("display"):
        out["display"] = raw["display"]
    return out


def _compact_entity_row(e: dict[str, Any]) -> dict[str, Any]:
    """Project one verbose ConsumptionCore entity row to a compact line: the label
    value + cost, plus expense ONLY when it differs from cost (credits applied) and
    the non-zero credit components ONLY when present. Lossless for the signal that
    matters; drops the all-zero credit padding that bloats the raw shape ~5x."""
    out: dict[str, Any] = {
        "value": (e.get("label") or {}).get("value"),
        "cost": _amount(e, "cost"),
    }
    expense = _amount(e, "expense")
    if expense != out["cost"]:
        out["expense"] = expense
    credits = {
        k: v.get("value")
        for k, v in (e.get("credit_details") or {}).items()
        if isinstance(v, dict) and _to_float(v.get("value", "0")) != 0
    }
    if credits:
        out["credits"] = credits
    return out


class DisplayCurrencyState:
    """Session-level preferred currency for converted output.
    Mutable singleton — changing it is one of the few side effects this server has."""

    def __init__(self, initial: str) -> None:
        self._value = initial.upper()
        self._lock = asyncio.Lock()

    @property
    def value(self) -> str:
        return self._value

    async def set(self, value: str) -> str:
        async with self._lock:
            self._value = value.upper()
            return self._value


def create_server(settings: Settings | None = None) -> tuple[FastMCP, Settings]:
    settings = settings or Settings.from_env()
    http = httpx.AsyncClient(timeout=30.0)
    provider = make_provider(settings, http)
    client = BillingClient(settings.billing_endpoint, provider, http)
    usage = UsageClient(
        provider,
        endpoint=settings.usage_endpoint,
        cache_ttl_seconds=settings.usage_cache_ttl,
        rpc_timeout_seconds=settings.usage_rpc_timeout,
    )
    fx = FxRates(
        CbrFxProvider(http, url=settings.fx_url),
        ttl_seconds=settings.fx_cache_ttl,
    )
    display = DisplayCurrencyState(settings.default_display_currency)

    async def _convert(
        amount: str | None, src: str | None
    ) -> dict[str, Any] | None:
        """Convert a YC monetary value to the current display currency.
        Returns None if input is unusable; returns an FX block (with rate, date,
        amount) otherwise. Same-currency conversions still return the block so
        downstream code can read amounts uniformly."""
        if not amount or not src:
            return None
        try:
            return await fx.convert(amount, src, display.value)
        except Exception as e:  # bad data, missing rate, network — log and skip
            log.warning("FX conversion %s→%s failed: %s", src, display.value, e)
            return None

    async def _convert_string_decimal(
        sd: Any, src: str | None
    ) -> dict[str, Any] | None:
        if not isinstance(sd, dict):
            return None
        return await _convert(sd.get("value"), src)

    async def _attach_spend_fx(resp: dict[str, Any]) -> dict[str, Any]:
        """Add a top-level `display` block to a ConsumptionCore response with the
        cost / expense / credit breakdown converted to the active display currency.
        Entity-level rows keep their native values — the LLM can call
        convert_amount on individual lines if needed."""
        src = resp.get("currency")
        target = display.value
        resp["display_currency"] = target
        if not src or src == target:
            return resp
        totals: dict[str, Any] = {}
        cost = await _convert_string_decimal(resp.get("cost"), src)
        if cost:
            totals["cost"] = cost
        expense = await _convert_string_decimal(resp.get("expense"), src)
        if expense:
            totals["expense"] = expense
        cd = resp.get("credit_details")
        if isinstance(cd, dict):
            cd_conv: dict[str, Any] = {}
            for k, v in cd.items():
                b = await _convert_string_decimal(v, src)
                if b:
                    cd_conv[k] = b
            if cd_conv:
                totals["credit_details"] = cd_conv
        if totals:
            sample = next(iter(totals.values()))
            if isinstance(sample, dict) and "credit_details" in totals and sample is totals.get("credit_details"):
                sample = next(iter(totals["credit_details"].values()), {})
            resp["display"] = {
                "currency": target,
                "source_currency": src,
                "rate": sample.get("rate") if isinstance(sample, dict) else None,
                "rate_date": sample.get("rate_date") if isinstance(sample, dict) else None,
                "totals": totals,
            }
        return resp

    def _resolve_account(billing_account_id: str | None) -> str:
        """Resolve the target billing account id: explicit arg wins, otherwise
        fall back to the server default (YC_BILLING_ACCOUNT_ID env). Raise a
        clear error if neither is set."""
        resolved = billing_account_id or settings.default_billing_account_id
        if not resolved:
            raise ValueError(
                "billing_account_id is not provided and no server default is "
                "configured. Either pass it explicitly to the tool, or set the "
                "YC_BILLING_ACCOUNT_ID environment variable on the server."
            )
        return resolved

    mcp = FastMCP(
        name="yandex-cloud-billing",
        instructions=(
            "Tools for the Yandex Cloud Billing API. You can:\n"
            "  - inspect billing accounts and their balances\n"
            "  - list the clouds bound to a billing account\n"
            "  - browse the Yandex Cloud service catalog\n"
            "  - look up SKU prices from the public price list\n"
            "  - read actual consumption (spend) via the ConsumptionCore gRPC API\n\n"
            "YC native pricing currency must be RUB, USD or KZT "
            f"(default: {settings.default_currency}). On top of that, this server "
            "keeps a session-level *display currency* that auto-converts billing "
            "amounts, SKU prices and spend totals to a single currency of the "
            "user's choice (default: "
            f"{settings.default_display_currency}). When the user asks for a "
            "different currency — including EUR or any other published by the "
            "Central Bank of Russia — call set_display_currency once; subsequent "
            "responses will include a `display_*` field with the converted value, "
            "the cross rate, and the rate date. Use get_exchange_rates for the "
            "current rate table or convert_amount for one-off math.\n\n"
            "The spend_* tools call a rate-limited gRPC API "
            f"(~1 request per minute per IP); responses are cached for {settings.usage_cache_ttl:.0f}s."
            + (
                "\n\nThe server has a default billing account configured "
                f"(`{settings.default_billing_account_id}`) — you can omit "
                "`billing_account_id` from spend_* calls unless the user asks "
                "about a different account."
                if settings.default_billing_account_id
                else "\n\nNo default billing account is configured — pass "
                "`billing_account_id` to every spend_* call. If the user "
                "doesn't know it, ask them."
            )
        ),
        host=settings.host,
        port=settings.port,
        streamable_http_path=settings.path,
    )

    # ----- Service catalog -----

    @mcp.tool(
        description=(
            "List Yandex Cloud services (the catalog used to group SKUs). "
            "Use filter=\"id='compute'\" to look up a specific service."
        )
    )
    async def list_services(
        filter: Annotated[
            str | None,
            Field(
                description=(
                    "Optional API-level filter, e.g. id=\"compute\". "
                    "Service id matches [a-z][-a-z0-9]{1,61}[a-z0-9]."
                )
            ),
        ] = None,
        max_items: Annotated[int, Field(ge=1, le=10000)] = 1000,
    ) -> dict[str, Any]:
        return {"services": await client.list_services(filter_=filter, max_items=max_items)}

    @mcp.tool(description="Get a single service by id (e.g. 'compute', 'storage', 'mk8s').")
    async def get_service(service_id: str) -> dict[str, Any]:
        return await client.get_service(service_id)

    # ----- Spend / consumption (ConsumptionCore gRPC API) -----
    #
    # All spend_* tools accept dates as YYYY-MM-DD or ISO 8601 datetimes.
    # Each response has three levels: totals (cost, credits, expense), per-entity
    # breakdown, and time series at the requested aggregation period.

    @mcp.tool(
        description=(
            "Total spend on a billing account between two dates. Returns cost, "
            "credits and expense, plus a time series at the chosen aggregation."
        )
    )
    async def spend_summary(        from_date: Annotated[
            str, Field(description="Inclusive start date (YYYY-MM-DD or ISO 8601).")
        ],
        to_date: Annotated[
            str, Field(description="Inclusive end date (YYYY-MM-DD or ISO 8601).")
        ],

        billing_account_id: str | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.billing_account_report(
            billing_account_id=_resolve_account(billing_account_id),
            from_date=from_date,
            to_date=to_date,
            aggregation_period=aggregation_period,
        ))

    @mcp.tool(
        description=(
            "Spend broken down by Yandex Cloud service (Compute, Storage, MK8s, …) "
            "for a billing account. This answers 'how much did we spend on service X "
            "in this period'. Optionally filter by cloud/folder/service ids."
        )
    )
    async def spend_by_service(        from_date: str,
        to_date: str,

        billing_account_id: str | None = None,
        service_ids: Annotated[
            list[str] | None,
            Field(description="Optional whitelist of service ids to include."),
        ] = None,
        cloud_ids: list[str] | None = None,
        folder_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.service_report(
            billing_account_id=_resolve_account(billing_account_id),
            from_date=from_date,
            to_date=to_date,
            service_ids=service_ids,
            cloud_ids=cloud_ids,
            folder_ids=folder_ids,
            aggregation_period=aggregation_period,
        ))

    @mcp.tool(
        description=(
            "Spend broken down by cloud for a billing account. Use to compare "
            "which cloud (i.e. tenant/project) is driving spend."
        )
    )
    async def spend_by_cloud(        from_date: str,
        to_date: str,

        billing_account_id: str | None = None,
        cloud_ids: list[str] | None = None,
        service_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.cloud_report(
            billing_account_id=_resolve_account(billing_account_id),
            from_date=from_date,
            to_date=to_date,
            cloud_ids=cloud_ids,
            service_ids=service_ids,
            aggregation_period=aggregation_period,
        ))

    @mcp.tool(
        description=(
            "Spend broken down by folder (a folder is the Yandex Cloud equivalent of "
            "a project inside a cloud). Use to attribute spend to teams/projects."
        )
    )
    async def spend_by_folder(        from_date: str,
        to_date: str,

        billing_account_id: str | None = None,
        folder_ids: list[str] | None = None,
        cloud_ids: list[str] | None = None,
        service_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.folder_report(
            billing_account_id=_resolve_account(billing_account_id),
            from_date=from_date,
            to_date=to_date,
            folder_ids=folder_ids,
            cloud_ids=cloud_ids,
            service_ids=service_ids,
            aggregation_period=aggregation_period,
        ))

    @mcp.tool(
        description=(
            "Spend broken down by SKU (line items, e.g. vCPU vs RAM vs storage vs "
            "egress). Use after spend_by_service to see what drives a service's cost. "
            "To get the SKU breakdown for ONE cluster / project / team, pass a `labels` "
            "filter (e.g. {\"project\": [\"clickhouse-commerce-pricing\"]}) or "
            "`resource_ids` / `service_ids` — without a filter this returns every SKU "
            "in the whole billing account, which is large and slow."
        )
    )
    async def spend_by_sku(
        from_date: str,
        to_date: str,
        billing_account_id: str | None = None,
        sku_ids: list[str] | None = None,
        service_ids: list[str] | None = None,
        cloud_ids: list[str] | None = None,
        folder_ids: list[str] | None = None,
        resource_ids: list[str] | None = None,
        labels: Annotated[
            dict[str, list[str]] | None,
            Field(
                description=(
                    "Optional label filter — map of key → list of allowed values, "
                    "e.g. {\"project\": [\"clickhouse-commerce-pricing\"]}. Use this to "
                    "scope the SKU breakdown to one team / project / cluster."
                )
            ),
        ] = None,
        labels_or_filter_logic: bool = False,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.sku_report(
            billing_account_id=_resolve_account(billing_account_id),
            from_date=from_date,
            to_date=to_date,
            sku_ids=sku_ids,
            service_ids=service_ids,
            cloud_ids=cloud_ids,
            folder_ids=folder_ids,
            resource_ids=resource_ids,
            labels=labels,
            labels_or_filter_logic=labels_or_filter_logic,
            aggregation_period=aggregation_period,
        ))

    @mcp.tool(
        description=(
            "Spend for SPECIFIC named cloud resources (VMs, buckets, clusters, …). "
            "`resource_ids` is REQUIRED — the Usage API rejects a resource report "
            "without it, so you must already know the resource UUIDs. To break down a "
            "cluster's cost when you only know its name or labels, use spend_by_label "
            "or spend_by_sku with a `labels` filter instead. Find resource UUIDs in the "
            "YC console or via `yc <service> list`."
        )
    )
    async def spend_by_resource(
        from_date: str,
        to_date: str,
        resource_ids: Annotated[
            list[str],
            Field(
                description=(
                    "REQUIRED. Resource UUIDs to report on. The Usage API returns "
                    "INVALID_ARGUMENT for a resource report with no resource_ids — "
                    "it does not enumerate every resource in the account."
                )
            ),
        ],
        billing_account_id: str | None = None,
        service_ids: list[str] | None = None,
        folder_ids: list[str] | None = None,
        cloud_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        if not resource_ids:
            raise ValueError(
                "spend_by_resource requires a non-empty resource_ids list — the "
                "Yandex Cloud Usage API rejects a resource report without it. If you "
                "only know the cluster/resource by name or label, use spend_by_label "
                'or spend_by_sku with a labels filter (e.g. {"project": ["<name>"]}) '
                "to break down its cost instead."
            )
        return await _attach_spend_fx(await usage.resource_report(
            billing_account_id=_resolve_account(billing_account_id),
            from_date=from_date,
            to_date=to_date,
            resource_ids=resource_ids,
            service_ids=service_ids,
            folder_ids=folder_ids,
            cloud_ids=cloud_ids,
            aggregation_period=aggregation_period,
        ))

    @mcp.tool(
        description=(
            "Spend for resources matching a SPECIFIC label filter (verbose, "
            "per-label-value rows). Pass `labels` as {key: [values]} (e.g. "
            "{\"project\": [\"commerce-pricing\"]}) and/or a service_ids / folder_ids "
            "/ cloud_ids scope. A filter OR scope is REQUIRED — an unfiltered call "
            "would enumerate every label key×value in the account (thousands of rows). "
            "To break a single key into its values (e.g. cost per project) use "
            "spend_grouped_by_label; to discover which label keys exist use "
            "list_label_keys."
        )
    )
    async def spend_by_label(
        from_date: str,
        to_date: str,
        billing_account_id: str | None = None,
        labels: Annotated[
            dict[str, list[str]] | None,
            Field(
                description=(
                    "Label filter — map of key → list of allowed values. "
                    "Example: {\"team\": [\"data\", \"platform\"], \"env\": [\"prod\"]}."
                )
            ),
        ] = None,
        labels_or_filter_logic: bool = False,
        service_ids: list[str] | None = None,
        cloud_ids: list[str] | None = None,
        folder_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        if not labels and not service_ids and not folder_ids and not cloud_ids:
            raise ValueError(
                "spend_by_label needs a `labels` filter or a service_ids / folder_ids "
                "/ cloud_ids scope — an unfiltered label report enumerates every label "
                "key×value in the account (thousands of rows, megabytes). To get cost "
                "broken down by ONE key's values use spend_grouped_by_label(label_key=…); "
                "to see which label keys exist use list_label_keys."
            )
        return await _attach_spend_fx(await usage.label_key_report(
            billing_account_id=_resolve_account(billing_account_id),
            from_date=from_date,
            to_date=to_date,
            labels=labels,
            labels_or_filter_logic=labels_or_filter_logic,
            service_ids=service_ids,
            cloud_ids=cloud_ids,
            folder_ids=folder_ids,
            aggregation_period=aggregation_period,
        ))

    @mcp.tool(
        description=(
            "Spend broken down by the values of ONE label key — the cost-allocation / "
            "chargeback workhorse (e.g. label_key=\"project\" → cost per project, "
            "label_key=\"team\" → cost per team). Returns the FULL breakdown in a COMPACT "
            "shape (value + cost, plus expense/credits only when non-trivial), sorted by "
            "cost descending — no truncation, `total` matches the sum of the breakdown. "
            "For low/medium-cardinality keys (project, team, cluster_id) this is small. "
            "Do NOT group by a key flagged high_cardinality in list_label_keys (thousands "
            "of values) — filter or scope with service_ids / folder_ids / cloud_ids "
            "instead. Use list_label_keys first if you don't know which keys exist."
        )
    )
    async def spend_grouped_by_label(
        label_key: Annotated[
            str,
            Field(description="The single label key to group by, e.g. \"project\"."),
        ],
        from_date: str,
        to_date: str,
        billing_account_id: str | None = None,
        service_ids: list[str] | None = None,
        folder_ids: list[str] | None = None,
        cloud_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        if not label_key:
            raise ValueError("label_key is required, e.g. label_key=\"project\".")
        # An empty values list for a key = "all values of this key" — the API's
        # group-by-single-key idiom, verified against ConsumptionCore.
        raw = await usage.label_key_report(
            billing_account_id=_resolve_account(billing_account_id),
            from_date=from_date,
            to_date=to_date,
            labels={label_key: []},
            service_ids=service_ids,
            folder_ids=folder_ids,
            cloud_ids=cloud_ids,
            aggregation_period=aggregation_period,
        )
        await _attach_spend_fx(raw)
        rows = [e for e in (raw.get("entities_data") or []) if isinstance(e, dict)]
        # Stateless: return the FULL breakdown (no cap / no pagination — the MCP may run
        # as several replicas, so any cursor/top-N that depends on a per-instance cache
        # would be inconsistent across calls). Sort cost desc, value asc as a stable
        # tiebreak so the output order is deterministic. High-cardinality keys are kept
        # out of scope by the list_label_keys `high_cardinality` flag, not by truncation.
        rows.sort(
            key=lambda e: (
                -_to_float(_amount(e, "cost")),
                (e.get("label") or {}).get("value") or "",
            )
        )
        out: dict[str, Any] = {
            "label_key": label_key,
            "currency": raw.get("currency"),
            "total": raw.get("cost"),
            "value_count": len(rows),
            "breakdown": [_compact_entity_row(e) for e in rows],
        }
        if raw.get("display"):
            out["display"] = raw["display"]
        # High-cardinality backstop (stateless — single call, replica-safe): if the
        # full breakdown would exceed the response budget it would be silently
        # truncated by the caller's tool-output token limit. Instead of truncating
        # (silent data loss) or sampling (top-N hides the tail), return a summary the
        # LLM can act on — the complete `total` + `value_count` + concrete ways to
        # narrow. Empirically (Codex, 3/3 intents) the LLM reads this and recovers:
        # coarser key for chargeback, scope for genuine per-value detail.
        if len(json.dumps(out, ensure_ascii=False)) > settings.grouped_max_bytes:
            out = _too_many_values_response(label_key, raw, len(rows))
        return out

    @mcp.tool(
        description=(
            "Discover which resource label keys exist (and how concentrated each is) "
            "so you can pick one for spend_grouped_by_label / spend_by_label. Returns a "
            "compact digest [{key, distinct_values, total_cost, high_cardinality}] over "
            "a recent window (defaults to the last few days; label keys are stable, so a "
            "recent snapshot reflects the current set). Keys flagged high_cardinality "
            "(e.g. per-node-group ids) should be filtered/scoped, not grouped wholesale."
        )
    )
    async def list_label_keys(
        billing_account_id: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        service_ids: list[str] | None = None,
        folder_ids: list[str] | None = None,
        cloud_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        # Default to a short recent window — keys are stable, and an unfiltered label
        # report over a long range is huge. Caller-supplied dates are honoured but a
        # scope is recommended for long ranges.
        if not from_date or not to_date:
            today = datetime.now(timezone.utc).date()
            from_date = (today - timedelta(days=2)).isoformat()
            to_date = today.isoformat()
        raw = await usage.label_key_report(
            billing_account_id=_resolve_account(billing_account_id),
            from_date=from_date,
            to_date=to_date,
            service_ids=service_ids,
            folder_ids=folder_ids,
            cloud_ids=cloud_ids,
            aggregation_period="MONTH",
        )
        agg: dict[str, dict[str, float | int]] = {}
        for e in raw.get("entities_data") or []:
            if not isinstance(e, dict):
                continue
            k = (e.get("label") or {}).get("key")
            if not k:
                continue
            slot = agg.setdefault(k, {"distinct_values": 0, "total_cost": 0.0})
            slot["distinct_values"] += 1
            slot["total_cost"] += _to_float(_amount(e, "cost"))
        keys = [
            {
                "key": k,
                "distinct_values": v["distinct_values"],
                "total_cost": f"{v['total_cost']:.4f}",
                "high_cardinality": v["distinct_values"] > HIGH_CARDINALITY_THRESHOLD,
            }
            for k, v in agg.items()
        ]
        keys.sort(key=lambda r: _to_float(r["total_cost"]), reverse=True)
        return {
            "currency": raw.get("currency"),
            "window": {"from": from_date, "to": to_date},
            "label_keys": keys,
        }

    # ----- Currency / FX -----

    @mcp.tool(
        description=(
            "Return the current session-level display currency. All price / spend "
            "tools auto-convert their results to this currency."
        )
    )
    async def get_display_currency() -> dict[str, str]:
        return {"display_currency": display.value}

    @mcp.tool(
        description=(
            "Set the session-level display currency. Affects every subsequent "
            "price / balance / spend response — they will include a `display_*` "
            "field with the value converted via CBR daily rates. Accepts any "
            "3-letter code published by the Central Bank of Russia (RUB, USD, EUR, "
            "KZT, CNY, GBP, JPY, …). Use get_exchange_rates to see the full list."
        )
    )
    async def set_display_currency(
        currency: Annotated[
            str, Field(description="3-letter ISO currency code, e.g. USD, EUR, RUB.")
        ],
    ) -> dict[str, Any]:
        supported = await fx.supported()
        upper = currency.upper()
        if upper not in supported:
            return {
                "error": f"Currency {upper!r} is not in the CBR rate table.",
                "supported_sample": supported[:20] + ["…"] if len(supported) > 20 else supported,
            }
        previous = display.value
        new = await display.set(upper)
        return {"display_currency": new, "previous": previous}

    @mcp.tool(
        description=(
            "Return the current CBR daily exchange rates, expressed as RUB per "
            "1 unit of foreign currency. Also returns the rate publication date."
        )
    )
    async def get_exchange_rates() -> dict[str, Any]:
        return await fx.snapshot()

    @mcp.tool(
        description=(
            "Convert an amount between currencies using CBR daily rates. "
            "Independent of the session display currency — use it for one-off math."
        )
    )
    async def convert_amount(
        amount: Annotated[
            str, Field(description="Numeric amount as a string (decimal-safe).")
        ],
        from_currency: Annotated[
            str, Field(description="Source 3-letter ISO currency code.")
        ],
        to_currency: Annotated[
            str, Field(description="Target 3-letter ISO currency code.")
        ],
    ) -> dict[str, Any]:
        try:
            return await fx.convert(amount, from_currency, to_currency)
        except ValueError as e:
            return {"error": str(e)}

    return mcp, settings


__all__ = ["create_server"]
