#!/usr/bin/env python3
"""
get_long_lived_token.py

One-time helper for Meta Threads access tokens.

Modes:
  1) exchange (default)
     Exchange a short-lived Threads user access token for a long-lived token.
  2) refresh
     Refresh an unexpired long-lived Threads user access token.

Examples:
  python get_long_lived_token.py --short-token "EAAB..." --app-id "..." --app-secret "..."
  python get_long_lived_token.py --mode refresh --long-token "..."

The script prints the token JSON response to stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict

import requests


GRAPH_BASE_URL = "https://graph.facebook.com"
THREADS_BASE_URL = "https://graph.threads.net"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exchange or refresh a Threads access token.")
    parser.add_argument("--mode", choices=["exchange", "refresh"], default="exchange")
    parser.add_argument("--short-token", help="Short-lived Threads user access token (exchange mode).")
    parser.add_argument("--long-token", help="Long-lived Threads user access token (refresh mode).")
    parser.add_argument("--app-id", default=os.getenv("THREADS_APP_ID"))
    parser.add_argument("--app-secret", default=os.getenv("THREADS_APP_SECRET"))
    parser.add_argument("--timeout", type=int, default=30)
    return parser.parse_args()


def request_json(url: str, params: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=timeout)
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
    return response.json()


def exchange_short_lived_token(short_token: str, app_id: str, app_secret: str, timeout: int) -> Dict[str, Any]:
    url = f"{GRAPH_BASE_URL}/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": short_token,
    }
    return request_json(url, params, timeout)


def refresh_long_lived_token(long_token: str, timeout: int) -> Dict[str, Any]:
    # Meta's Threads docs describe GET /refresh_access_token for refreshing
    # unexpired long-lived Threads user access tokens.
    url = f"{THREADS_BASE_URL}/refresh_access_token"
    params = {
        "grant_type": "th_refresh_token",
        "access_token": long_token,
    }
    return request_json(url, params, timeout)


def main() -> int:
    args = parse_args()

    try:
        if args.mode == "exchange":
            app_id = (args.app_id or "").strip()
            app_secret = (args.app_secret or "").strip()
            if not app_id or not app_secret:
                print(
                    "ERROR: --app-id / --app-secret (or THREADS_APP_ID / THREADS_APP_SECRET) are required in exchange mode.",
                    file=sys.stderr,
                )
                return 1

            short_token = (args.short_token or "").strip()
            if not short_token:
                print("ERROR: --short-token is required in exchange mode.", file=sys.stderr)
                return 1
            result = exchange_short_lived_token(short_token, app_id, app_secret, args.timeout)
        else:
            long_token = (args.long_token or "").strip()
            if not long_token:
                print("ERROR: --long-token is required in refresh mode.", file=sys.stderr)
                return 1
            result = refresh_long_lived_token(long_token, args.timeout)

        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
