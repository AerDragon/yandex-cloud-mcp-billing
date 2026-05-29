"""Foreign exchange rates for converting Yandex Cloud billing amounts between
currencies. Uses the Central Bank of Russia daily reference rates by default —
the API is free, requires no auth, and publishes 50+ currencies once a day."""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal, getcontext
from typing import Any, Protocol

import httpx

log = logging.getLogger(__name__)

CBR_DAILY_URL = "https://www.cbr-xml-daily.ru/daily_json.js"

# 28 digits is the default; bump just in case we chain multiplications.
getcontext().prec = 38


class FxProvider(Protocol):
    async def fetch(self) -> tuple[dict[str, Decimal], str]: ...


class CbrFxProvider:
    """Central Bank of Russia daily reference rates. RUB-based.

    Returns rates as `RUB per 1 unit of foreign currency`. RUB itself is
    represented as 1. Cross rates are derived by going through RUB."""

    def __init__(self, http: httpx.AsyncClient, url: str = CBR_DAILY_URL) -> None:
        self._http = http
        self._url = url

    async def fetch(self) -> tuple[dict[str, Decimal], str]:
        r = await self._http.get(self._url, follow_redirects=True)
        r.raise_for_status()
        data = r.json()
        rates: dict[str, Decimal] = {"RUB": Decimal(1)}
        for code, entry in (data.get("Valute") or {}).items():
            try:
                value = Decimal(str(entry["Value"]))
                nominal = Decimal(str(entry["Nominal"]))
            except (KeyError, TypeError):
                continue
            if nominal == 0:
                continue
            rates[code.upper()] = value / nominal
        rate_date = data.get("Date") or ""
        return rates, rate_date


class FxRates:
    """Async wrapper around an FxProvider with TTL caching."""

    def __init__(self, provider: FxProvider, ttl_seconds: float = 6 * 3600) -> None:
        self._provider = provider
        self._ttl = ttl_seconds
        self._cache: tuple[float, dict[str, Decimal], str] | None = None
        self._lock = asyncio.Lock()

    async def _get(self) -> tuple[dict[str, Decimal], str]:
        async with self._lock:
            now = time.time()
            if self._cache is not None and now - self._cache[0] < self._ttl:
                return self._cache[1], self._cache[2]
            rates, date_iso = await self._provider.fetch()
            self._cache = (now, rates, date_iso)
            log.info(
                "FX rates refreshed (%d currencies, date=%s)", len(rates), date_iso
            )
            return rates, date_iso

    async def snapshot(self) -> dict[str, Any]:
        rates, date_iso = await self._get()
        return {
            "base": "RUB",
            "rate_date": date_iso,
            "rates_per_unit_to_rub": {
                k: format(v.normalize(), "f") for k, v in sorted(rates.items())
            },
        }

    async def supported(self) -> list[str]:
        rates, _ = await self._get()
        return sorted(rates)

    async def convert(
        self,
        amount: str | int | float | Decimal,
        from_currency: str,
        to_currency: str,
    ) -> dict[str, Any]:
        rates, date_iso = await self._get()
        f = from_currency.upper()
        t = to_currency.upper()
        try:
            amt = Decimal(str(amount))
        except Exception as e:
            raise ValueError(f"Invalid amount {amount!r}: {e}") from None

        if f == t:
            return {
                "amount": format(amt.normalize(), "f"),
                "currency": t,
                "source_currency": f,
                "source_amount": format(amt.normalize(), "f"),
                "rate": "1",
                "rate_date": date_iso,
            }

        missing = [c for c in (f, t) if c not in rates]
        if missing:
            raise ValueError(
                f"Unknown currency code(s): {missing}. "
                f"Available: {sorted(rates)[:20]}…"
            )

        # via RUB: amt * (RUB/unit_f) / (RUB/unit_t)
        cross = rates[f] / rates[t]
        converted = amt * cross

        return {
            "amount": format(converted.normalize(), "f"),
            "currency": t,
            "source_currency": f,
            "source_amount": format(amt.normalize(), "f"),
            "rate": format(cross.normalize(), "f"),
            "rate_date": date_iso,
        }
