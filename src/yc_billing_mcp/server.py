from __future__ import annotations

import logging
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
    async def spend_summary(
        billing_account_id: str,
        from_date: Annotated[
            str, Field(description="Inclusive start date (YYYY-MM-DD or ISO 8601).")
        ],
        to_date: Annotated[
            str, Field(description="Inclusive end date (YYYY-MM-DD or ISO 8601).")
        ],
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.billing_account_report(
            billing_account_id=billing_account_id,
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
    async def spend_by_service(
        billing_account_id: str,
        from_date: str,
        to_date: str,
        service_ids: Annotated[
            list[str] | None,
            Field(description="Optional whitelist of service ids to include."),
        ] = None,
        cloud_ids: list[str] | None = None,
        folder_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.service_report(
            billing_account_id=billing_account_id,
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
    async def spend_by_cloud(
        billing_account_id: str,
        from_date: str,
        to_date: str,
        cloud_ids: list[str] | None = None,
        service_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.cloud_report(
            billing_account_id=billing_account_id,
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
    async def spend_by_folder(
        billing_account_id: str,
        from_date: str,
        to_date: str,
        folder_ids: list[str] | None = None,
        cloud_ids: list[str] | None = None,
        service_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.folder_report(
            billing_account_id=billing_account_id,
            from_date=from_date,
            to_date=to_date,
            folder_ids=folder_ids,
            cloud_ids=cloud_ids,
            service_ids=service_ids,
            aggregation_period=aggregation_period,
        ))

    @mcp.tool(
        description=(
            "Spend broken down by SKU. Use after spend_by_service when you need to "
            "know which exact line items (e.g. vCPU vs RAM vs egress) drive the cost."
        )
    )
    async def spend_by_sku(
        billing_account_id: str,
        from_date: str,
        to_date: str,
        sku_ids: list[str] | None = None,
        service_ids: list[str] | None = None,
        cloud_ids: list[str] | None = None,
        folder_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.sku_report(
            billing_account_id=billing_account_id,
            from_date=from_date,
            to_date=to_date,
            sku_ids=sku_ids,
            service_ids=service_ids,
            cloud_ids=cloud_ids,
            folder_ids=folder_ids,
            aggregation_period=aggregation_period,
        ))

    @mcp.tool(
        description=(
            "Spend broken down by individual cloud resources (VMs, buckets, "
            "clusters, …). Useful for hunting cost outliers."
        )
    )
    async def spend_by_resource(
        billing_account_id: str,
        from_date: str,
        to_date: str,
        resource_ids: list[str] | None = None,
        service_ids: list[str] | None = None,
        folder_ids: list[str] | None = None,
        cloud_ids: list[str] | None = None,
        aggregation_period: AggregationPeriod = "MONTH",
    ) -> dict[str, Any]:
        return await _attach_spend_fx(await usage.resource_report(
            billing_account_id=billing_account_id,
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
            "Spend grouped by a resource label key (cost-allocation style). Pass "
            "the labels filter as a dict of key→value; set labels_or_filter_logic=true "
            "to OR them instead of AND."
        )
    )
    async def spend_by_label(
        billing_account_id: str,
        from_date: str,
        to_date: str,
        labels: Annotated[
            dict[str, list[str]] | None,
            Field(
                description=(
                    "Optional label filter — map of key → list of allowed values. "
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
        return await _attach_spend_fx(await usage.label_key_report(
            billing_account_id=billing_account_id,
            from_date=from_date,
            to_date=to_date,
            labels=labels,
            labels_or_filter_logic=labels_or_filter_logic,
            service_ids=service_ids,
            cloud_ids=cloud_ids,
            folder_ids=folder_ids,
            aggregation_period=aggregation_period,
        ))

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
