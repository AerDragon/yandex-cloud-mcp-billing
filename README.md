# yandex-cloud-mcp-billing

MCP server that exposes the [Yandex Cloud Billing Usage API](https://yandex.cloud/en/docs/billing/usage/api-ref/grpc/)
as cost-reporting tools, with automatic currency conversion. Usable from Claude
Desktop, Cursor, IDE extensions, or any MCP-compatible client.

The server is intentionally focused on **cost reports**: it answers "how much did we
spend on service X / cloud Y / folder Z in this period?" It does not expose
account balances, budgets or the price catalogue — only consumption data.

## Tools

### Service catalogue (REST)

Tiny helper layer for resolving service IDs that you pass to the spend tools:

| Tool | What it does |
| --- | --- |
| `list_services` | The Yandex Cloud service catalogue (Compute, Storage, MK8s, …). |
| `get_service` | One service by id. |

### Spend / consumption (ConsumptionCore gRPC)

Every tool below takes `billing_account_id`, `from_date`, `to_date` (YYYY-MM-DD or
ISO 8601) and `aggregation_period` (DAY | WEEK | MONTH | QUARTER | YEAR, default
MONTH). Each returns a three-level structure: totals (cost, credits, expense), the
per-entity breakdown for the requested dimension, and a time series.

| Tool | What it answers |
| --- | --- |
| `spend_summary` | "How much did we spend on this billing account in this period?" |
| `spend_by_service` | "How much did we spend on Compute / S3 / MK8s …?" |
| `spend_by_cloud` | "Which cloud (tenant) drove the bill?" |
| `spend_by_folder` | "Which folder / project drove the bill?" |
| `spend_by_sku` | "Which line items inside this service cost the most?" |
| `spend_by_resource` | "Which individual VM / bucket / cluster was the outlier?" |
| `spend_by_label` | "Cost-allocate by resource labels (team, env, …)." |

These hit the `ConsumptionCoreService` gRPC API, which has a **1-request-per-minute
per-IP rate limit**. Responses are cached in-process for `YC_USAGE_CACHE_TTL` seconds
(default 300). Requires the role `billing.accounts.getReport`.

### Display currency (auto-conversion)

The server keeps a session-level *display currency*. Every spend response carries a
top-level `display` block with cost / expense / credit totals converted at the
current CBR daily rate. Default: `USD` (override via `YC_DISPLAY_CURRENCY` env).

| Tool | What it does |
| --- | --- |
| `get_display_currency` | Read the active display currency. |
| `set_display_currency` | Switch to a new one — any 3-letter code from the CBR feed (USD, EUR, RUB, KZT, CNY, GBP, JPY, …). |
| `get_exchange_rates` | Dump the full CBR rate table (RUB per 1 unit, with publication date). |
| `convert_amount` | One-off conversion between two arbitrary currencies. |

The LLM only needs to call `set_display_currency` once — the user says "show me everything
in euros", LLM switches, all subsequent tools auto-include EUR values alongside native
YC ones.

## Authentication

The server picks the first auth method whose env var is set, in this priority:

1. `YC_IAM_TOKEN` — a ready IAM token (`yc iam create-token`). Not refreshed.
2. `YC_SA_KEY_JSON` — service account key JSON inline. Signs a PS256 JWT and exchanges it.
3. `YC_SA_KEY_FILE` — path to the same JSON on disk.
4. `YC_WORKLOAD_TOKEN_FILE` — path to a projected OIDC token (Kubernetes Workload
   Identity Federation). The token is exchanged at
   `https://auth.yandex.cloud/oauth/token`. Set `YC_WORKLOAD_AUDIENCE` if your federation
   requires an explicit audience.
5. `YC_OAUTH_TOKEN` — Yandex Passport OAuth token, exchanged for an IAM token.
6. `YC_USE_METADATA=true` — read the IAM token from the Compute instance metadata
   service (`169.254.169.254`). Use this when the pod or VM has a YC service account
   attached.

All refreshable providers cache the token until 60 s before expiry.

## Run locally

```bash
uv venv && source .venv/bin/activate
uv pip install -e .

export YC_IAM_TOKEN="$(yc iam create-token)"
yc-billing-mcp
# → Streamable HTTP MCP endpoint on http://127.0.0.1:8000/mcp
```

The default bind is `127.0.0.1` per the MCP spec (DNS rebinding protection). Set
`MCP_HOST=0.0.0.0` only when running behind a reverse proxy / inside a container.

### Transport

`MCP_TRANSPORT` selects the wire format:

- `streamable-http` (default) — single endpoint at `MCP_PATH`, POST for requests and an
  optional SSE upgrade for streaming. This is the modern MCP HTTP transport.
- `sse` — the legacy HTTP+SSE transport, still useful for older clients.
- `stdio` — for desktop clients that spawn the server as a subprocess.

## Client config

### Claude Desktop / Claude Code (stdio)

```jsonc
{
  "mcpServers": {
    "yc-billing": {
      "command": "yc-billing-mcp",
      "env": {
        "MCP_TRANSPORT": "stdio",
        "YC_SA_KEY_FILE": "/Users/me/.yc/sa-key.json"
      }
    }
  }
}
```

### Streamable HTTP

```jsonc
{
  "mcpServers": {
    "yc-billing": {
      "url": "http://127.0.0.1:8000/mcp"
    }
  }
}
```

## Kubernetes (Workload Identity Federation)

1. Create a federation in YC and bind it to a service account with the
   `billing.accounts.viewer` (or stronger) role on the billing account.
2. Project a JWT into the pod with the federation's expected audience:

```yaml
apiVersion: v1
kind: Pod
spec:
  serviceAccountName: yc-billing-mcp
  containers:
    - name: server
      image: ghcr.io/your-org/yc-billing-mcp:latest
      env:
        - { name: YC_WORKLOAD_TOKEN_FILE, value: /var/run/secrets/tokens/yc-token }
        - { name: YC_WORKLOAD_AUDIENCE,   value: "https://yc.example/federations/<fed-id>" }
        - { name: MCP_HOST,               value: "0.0.0.0" }
      volumeMounts:
        - name: yc-token
          mountPath: /var/run/secrets/tokens
          readOnly: true
  volumes:
    - name: yc-token
      projected:
        sources:
          - serviceAccountToken:
              path: yc-token
              audience: https://yc.example/federations/<fed-id>
              expirationSeconds: 3600
```

3. Front the pod with a Service / Ingress that adds your own auth (mTLS, OIDC at the
   proxy, etc.) — the server itself does not authenticate MCP clients.

## Local container

```bash
docker build -t yc-billing-mcp .
docker run --rm -p 8000:8000 \
  -e YC_IAM_TOKEN="$(yc iam create-token)" \
  yc-billing-mcp
```

## Required IAM roles

- `billing.accounts.getReport` on the billing account you query — required for all
  `spend_*` tools (ConsumptionCore API).
- No role is needed to read the public service catalogue.

## Configuration reference

See [.env.example](./.env.example).
