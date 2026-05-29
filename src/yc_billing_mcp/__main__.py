from __future__ import annotations

import logging
import os

from .server import create_server


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    mcp, settings = create_server()
    if settings.transport == "stdio":
        mcp.run(transport="stdio")
    elif settings.transport == "sse":
        mcp.run(transport="sse")
    else:
        mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
