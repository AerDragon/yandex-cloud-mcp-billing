from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


Transport = Literal["streamable-http", "sse", "stdio"]


@dataclass(frozen=True)
class Settings:
    # Auth
    iam_token: str | None
    oauth_token: str | None
    sa_key_file: str | None
    sa_key_json: str | None
    workload_token_file: str | None
    workload_audience: str | None
    use_metadata: bool

    # API
    billing_endpoint: str
    usage_endpoint: str
    iam_endpoint: str
    workload_endpoint: str
    metadata_endpoint: str
    default_currency: str
    usage_cache_ttl: float
    fx_url: str
    fx_cache_ttl: float
    default_display_currency: str

    # HTTP server
    transport: Transport
    host: str
    port: int
    path: str

    @classmethod
    def from_env(cls) -> "Settings":
        transport = os.getenv("MCP_TRANSPORT", "streamable-http")
        if transport not in ("streamable-http", "sse", "stdio"):
            raise ValueError(
                f"MCP_TRANSPORT must be one of streamable-http, sse, stdio; got {transport!r}"
            )
        return cls(
            iam_token=os.getenv("YC_IAM_TOKEN"),
            oauth_token=os.getenv("YC_OAUTH_TOKEN"),
            sa_key_file=os.getenv("YC_SA_KEY_FILE"),
            sa_key_json=os.getenv("YC_SA_KEY_JSON"),
            workload_token_file=os.getenv("YC_WORKLOAD_TOKEN_FILE"),
            workload_audience=os.getenv("YC_WORKLOAD_AUDIENCE"),
            use_metadata=_env_bool("YC_USE_METADATA"),
            billing_endpoint=os.getenv(
                "YC_BILLING_ENDPOINT", "https://billing.api.cloud.yandex.net"
            ),
            usage_endpoint=os.getenv(
                "YC_USAGE_ENDPOINT", "billing.api.cloud.yandex.net:443"
            ),
            iam_endpoint=os.getenv(
                "YC_IAM_ENDPOINT", "https://iam.api.cloud.yandex.net/iam/v1/tokens"
            ),
            workload_endpoint=os.getenv(
                "YC_WORKLOAD_ENDPOINT", "https://auth.yandex.cloud/oauth/token"
            ),
            metadata_endpoint=os.getenv(
                "YC_METADATA_ENDPOINT",
                "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
            ),
            default_currency=os.getenv("YC_DEFAULT_CURRENCY", "RUB"),
            usage_cache_ttl=float(os.getenv("YC_USAGE_CACHE_TTL", "300")),
            fx_url=os.getenv(
                "FX_RATES_URL", "https://www.cbr-xml-daily.ru/daily_json.js"
            ),
            fx_cache_ttl=float(os.getenv("FX_CACHE_TTL", str(6 * 3600))),
            default_display_currency=os.getenv("YC_DISPLAY_CURRENCY", "USD").upper(),
            transport=transport,  # type: ignore[arg-type]
            host=os.getenv("MCP_HOST", "127.0.0.1"),
            port=int(os.getenv("MCP_PORT", "8000")),
            path=os.getenv("MCP_PATH", "/mcp"),
        )
