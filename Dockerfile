FROM python:3.12-slim AS build
WORKDIR /app
RUN pip install --no-cache-dir uv
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN uv pip install --system --no-cache .

FROM python:3.12-slim
WORKDIR /app
COPY --from=build /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=build /usr/local/bin /usr/local/bin
COPY src ./src

ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8000
ENV MCP_PATH=/mcp
ENV MCP_TRANSPORT=streamable-http
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

USER 1000:1000
CMD ["python", "-m", "yc_billing_mcp"]
