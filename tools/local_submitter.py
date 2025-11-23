#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local submitter helper for DMS Scheduler")
    parser.add_argument("--api-url", default="http://localhost:8000", help="Scheduler base URL")
    return parser.parse_args()


ASYNC_POLL_DELAY = 1.0


async def main() -> None:
    args = parse_args()
    async with httpx.AsyncClient(base_url=args.api_url, timeout=10) as client:
        while True:
            response = await client.post("/submitter/next")
            if response.status_code == 204:
                print("No tasks available. Retrying in 1s...")
                await asyncio.sleep(ASYNC_POLL_DELAY)
                continue
            response.raise_for_status()
            data: Any = response.json()
            print("Dispatched task:\n", json.dumps(data, indent=2))
            break


if __name__ == "__main__":
    asyncio.run(main())
