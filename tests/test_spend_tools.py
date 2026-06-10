"""Regression tests for the spend-tool fixes.

These cover the pure / schema-level contracts that need no Yandex Cloud
credentials. The actual gRPC behaviour (label-filtered SKU breakdown, deadline
mapping) is verified against the live Usage API at deploy time.
"""

from __future__ import annotations

import asyncio
import copy

import pytest

from yc_billing_mcp.config import Settings
from yc_billing_mcp.server import (
    _compact_entity_row,
    _too_many_values_response,
    create_server,
)
from yc_billing_mcp.usage import _collapse_redundant_periodic, _periodic_matches_row


def _settings() -> Settings:
    import os

    # A token is enough for create_server to wire up; no network call is made
    # until a tool actually runs.
    os.environ.setdefault("YC_IAM_TOKEN", "test-token")
    os.environ.setdefault("YC_BILLING_ACCOUNT_ID", "dn2test000000000000")
    return Settings.from_env()


def _tools_by_name():
    mcp, _ = create_server(_settings())
    return {t.name: t for t in asyncio.run(mcp.list_tools())}


def test_usage_rpc_timeout_has_default():
    assert _settings().usage_rpc_timeout == 30.0


def test_spend_by_resource_requires_resource_ids():
    """resource_ids is mandatory — the Usage API rejects an unscoped resource report."""
    tool = _tools_by_name()["spend_by_resource"]
    required = (tool.inputSchema or {}).get("required", [])
    assert "resource_ids" in required


def test_spend_by_sku_exposes_label_and_resource_filters():
    """SKU breakdown must be scopable to one cluster/project via labels/resource_ids."""
    tool = _tools_by_name()["spend_by_sku"]
    props = (tool.inputSchema or {}).get("properties", {})
    assert "labels" in props
    assert "resource_ids" in props


@pytest.mark.asyncio
async def test_spend_by_resource_empty_list_raises_actionable_error():
    """An explicitly empty resource_ids list yields a clear ValueError, not a raw
    gRPC INVALID_ARGUMENT."""
    mcp, _ = create_server(_settings())
    # Resolve the underlying callable registered with FastMCP.
    tool = await mcp.get_tool("spend_by_resource") if hasattr(mcp, "get_tool") else None
    fn = getattr(tool, "fn", None) if tool is not None else None
    if fn is None:  # fall back: pull from the tool manager registry
        fn = mcp._tool_manager._tools["spend_by_resource"].fn  # type: ignore[attr-defined]
    with pytest.raises(ValueError) as ei:
        await fn(from_date="2026-05-01", to_date="2026-05-31", resource_ids=[])
    assert "resource_ids" in str(ei.value)


def _fn(mcp, name):
    return mcp._tool_manager._tools[name].fn  # type: ignore[attr-defined]


# ----- new label tools -----

def test_new_label_tools_registered():
    tools = _tools_by_name()
    assert "spend_grouped_by_label" in tools
    assert "list_label_keys" in tools
    sg = tools["spend_grouped_by_label"]
    assert "label_key" in (sg.inputSchema or {}).get("required", [])


@pytest.mark.asyncio
async def test_spend_by_label_requires_filter_or_scope():
    """Unfiltered/unscoped label report (the 9k-row cartesian) is refused with an
    actionable error pointing at spend_grouped_by_label / list_label_keys."""
    mcp, _ = create_server(_settings())
    with pytest.raises(ValueError) as ei:
        await _fn(mcp, "spend_by_label")(from_date="2026-05-01", to_date="2026-05-31")
    msg = str(ei.value)
    assert "spend_grouped_by_label" in msg and "list_label_keys" in msg


# ----- lossless periodic collapse -----

def _row(cost, *, expense=None, credits=None, periodic=None):
    r = {"cost": {"value": cost}, "expense": {"value": expense if expense is not None else cost}}
    if credits is not None:
        r["credit_details"] = {k: {"value": v} for k, v in credits.items()}
    if periodic is not None:
        r["periodic"] = periodic
    return r


def test_periodic_collapse_drops_only_redundant_single_bucket():
    # single bucket that equals the row → dropped (lossless)
    bucket = {"cost": {"value": "10"}, "expense": {"value": "10"}, "timestamp": "2026-05-01T00:00:00Z"}
    row = _row("10", periodic=[copy.deepcopy(bucket)])
    obj = {"entities_data": [row]}
    _collapse_redundant_periodic(obj)
    assert "periodic" not in obj["entities_data"][0]
    # cost/expense untouched
    assert obj["entities_data"][0]["cost"]["value"] == "10"


def test_periodic_collapse_keeps_multibucket_series():
    row = _row("30", periodic=[{"cost": {"value": "10"}}, {"cost": {"value": "20"}}])
    obj = {"x": [row]}
    _collapse_redundant_periodic(obj)
    assert obj["x"][0]["periodic"] == [{"cost": {"value": "10"}}, {"cost": {"value": "20"}}]


def test_periodic_collapse_keeps_mismatched_single_bucket():
    # single bucket whose cost differs from the row → KEPT (would lose info)
    row = _row("10", periodic=[{"cost": {"value": "7"}}])
    obj = {"x": [row]}
    _collapse_redundant_periodic(obj)
    assert obj["x"][0]["periodic"] == [{"cost": {"value": "7"}}]


def test_periodic_matches_respects_credit_details():
    row = _row("10", credits={"credit": "5"})
    same = {"cost": {"value": "10"}, "expense": {"value": "10"}, "credit_details": {"credit": {"value": "5"}}}
    diff = {"cost": {"value": "10"}, "expense": {"value": "10"}, "credit_details": {"credit": {"value": "0"}}}
    assert _periodic_matches_row(row, same) is True
    assert _periodic_matches_row(row, diff) is False


# ----- compact projection (lossless for non-trivial signal) -----

def test_compact_row_minimal_when_only_cost():
    r = _compact_entity_row(_row("100", credits={"credit": "0", "free_credit": "0"}))
    assert r == {"value": None, "cost": "100"}  # expense==cost dropped, all-zero credits dropped


def test_compact_row_keeps_expense_and_nonzero_credits():
    e = {"label": {"key": "project", "value": "px"}, "cost": {"value": "100"},
         "expense": {"value": "80"}, "credit_details": {"credit": {"value": "20"}, "free_credit": {"value": "0"}}}
    r = _compact_entity_row(e)
    assert r["value"] == "px" and r["cost"] == "100"
    assert r["expense"] == "80"           # differs from cost → kept
    assert r["credits"] == {"credit": "20"}  # only the nonzero component


# ----- high-cardinality backstop (refuse summary the LLM can act on) -----

def test_grouped_max_bytes_setting_default():
    assert _settings().grouped_max_bytes == 40000


def test_too_many_values_response_is_actionable_summary():
    raw = {"currency": "RUB", "cost": {"value": "650592.85"}, "display": {"currency": "USD"}}
    out = _too_many_values_response("managed-kubernetes-node-group-id", raw, 7977)
    # NOT an error/exception — a normal result the LLM reasons over
    assert out["status"] == "too_many_values_to_list"
    # the complete, correct aggregate the LLM can still report
    assert out["total"] == {"value": "650592.85"}
    assert out["value_count"] == 7977
    assert "no breakdown" not in out  # no truncated/partial rows are present
    assert "breakdown" not in out
    # three concrete, parameter-named recovery paths
    paths = " ".join(out["how_to_narrow"])
    assert "spend_grouped_by_label" in paths and "label_key" in paths  # coarser key
    assert "service_ids" in paths and "folder_ids" in paths            # scope
    assert "spend_by_label" in paths                                   # named values
    assert "list_label_keys" in paths                                  # discovery
    # the named-values hint carries the actual key, not a placeholder
    assert "managed-kubernetes-node-group-id" in paths
    assert out["display"] == {"currency": "USD"}  # display preserved when present


@pytest.mark.asyncio
async def test_spend_grouped_by_label_refuses_when_over_budget(monkeypatch):
    """End-to-end: a high-cardinality group-by (synthetic 5k values) trips the size
    budget and returns the summary, not thousands of rows."""
    mcp, _ = create_server(_settings())
    usage = mcp._tool_manager._tools["spend_grouped_by_label"]  # type: ignore[attr-defined]
    # Patch the underlying report to return a large synthetic breakdown.
    rows = [
        {"label": {"key": "k", "value": f"v{i}"}, "cost": {"value": f"{i}.0"},
         "expense": {"value": f"{i}.0"}}
        for i in range(5000)
    ]
    big = {"currency": "RUB", "cost": {"value": "12345.0"}, "entities_data": rows}

    async def fake_report(self, **kw):  # bound-method signature (self injected)
        return big

    # The tool closes over a UsageClient instance; patch the method at class level
    # so that instance uses the fake.
    from yc_billing_mcp.usage import UsageClient
    monkeypatch.setattr(UsageClient, "label_key_report", fake_report, raising=True)

    out = await usage.fn(label_key="k", from_date="2026-05-01", to_date="2026-05-31")
    assert out["status"] == "too_many_values_to_list"
    assert out["value_count"] == 5000
    assert out["total"] == {"value": "12345.0"}
    assert "breakdown" not in out
